from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, ValidationError

from app.core.settings import get_settings
from app.core.utils import extract_first_json_object, read_text_file

LOGGER = logging.getLogger("final_composer")


@dataclass(frozen=True)
class ComposeResult:
    answer: str
    error: str | None


class ComposerOutputSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")
    answer: str


def _load_prompt(prompt_path: Path | None = None) -> str:
    settings = get_settings()
    target = prompt_path or settings.final_composer_prompt_path
    return read_text_file(
        target,
        missing_message="Final composer prompt file not found: {path}",
    )


class FinalResponseComposer:
    def __init__(
        self,
        *,
        model: str | None = None,
        prompt_path: Path | None = None,
        openai_client: OpenAI | None = None,
    ) -> None:
        self.settings = get_settings()
        self.model = model or self.settings.openai_copilot_model
        self.max_output_tokens = self.settings.openai_final_max_output_tokens
        self.max_answer_chars = self.settings.openai_final_max_answer_chars
        self.reasoning_effort = self.settings.openai_final_reasoning_effort
        self.prompt = _load_prompt(prompt_path=prompt_path)
        self._openai_client = openai_client

    def _client(self) -> OpenAI | None:
        if self._openai_client is not None:
            return self._openai_client
        api_key = self.settings.openai_api_key
        if not api_key:
            return None
        self._openai_client = OpenAI(api_key=api_key)
        return self._openai_client

    def compose(
        self,
        *,
        question: str,
        intent: str,
        entities: Sequence[dict[str, Any]],
        used_sql: bool,
        used_rag: bool,
        sql_rows_preview: Sequence[dict[str, Any]],
        rag_context_snippets: Sequence[dict[str, Any]],
        force_fallback: bool = False,
    ) -> ComposeResult:
        question_clean = (question or "").strip()
        if not question_clean:
            return ComposeResult(answer="I need a question to provide an answer.", error="Question is empty.")

        if force_fallback:
            return ComposeResult(
                answer=self._deterministic_fallback(
                    question=question_clean,
                    intent=intent,
                    entities=entities,
                    used_sql=used_sql,
                    used_rag=used_rag,
                    sql_rows_preview=sql_rows_preview,
                    rag_context_snippets=rag_context_snippets,
                ),
                error="Forced fallback mode.",
            )

        client = self._client()
        if client is None:
            return ComposeResult(
                answer=self._deterministic_fallback(
                    question=question_clean,
                    intent=intent,
                    entities=entities,
                    used_sql=used_sql,
                    used_rag=used_rag,
                    sql_rows_preview=sql_rows_preview,
                    rag_context_snippets=rag_context_snippets,
                ),
                error="OPENAI_API_KEY is missing.",
            )

        payload = {
            "question": question_clean,
            "intent": intent,
            "used_sql": used_sql,
            "used_rag": used_rag,
            "entities": list(entities),
            "sql_rows_preview": list(sql_rows_preview),
            "rag_context_snippets": list(rag_context_snippets),
            "max_answer_chars": self.max_answer_chars,
        }
        try:
            response = client.responses.parse(
                model=self.model,
                max_output_tokens=self.max_output_tokens,
                reasoning={"effort": self.reasoning_effort},
                input=[
                    {"role": "system", "content": self.prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                text_format=ComposerOutputSchema,
            )
        except Exception as exc:
            LOGGER.warning("Final composer failed: %s", exc)
            return ComposeResult(
                answer=self._deterministic_fallback(
                    question=question_clean,
                    intent=intent,
                    entities=entities,
                    used_sql=used_sql,
                    used_rag=used_rag,
                    sql_rows_preview=sql_rows_preview,
                    rag_context_snippets=rag_context_snippets,
                ),
                error=f"LLM final composer failed ({exc}).",
            )

        parsed = getattr(response, "output_parsed", None)
        if not isinstance(parsed, ComposerOutputSchema):
            parsed = self._parse_from_text(response)
        if not isinstance(parsed, ComposerOutputSchema):
            status = getattr(response, "status", "unknown")
            return ComposeResult(
                answer=self._deterministic_fallback(
                    question=question_clean,
                    intent=intent,
                    entities=entities,
                    used_sql=used_sql,
                    used_rag=used_rag,
                    sql_rows_preview=sql_rows_preview,
                    rag_context_snippets=rag_context_snippets,
                ),
                error=f"LLM returned non-schema final output (status={status}).",
            )

        answer = re.sub(r"\s+", " ", parsed.answer or "").strip()
        if not answer:
            return ComposeResult(
                answer=self._deterministic_fallback(
                    question=question_clean,
                    intent=intent,
                    entities=entities,
                    used_sql=used_sql,
                    used_rag=used_rag,
                    sql_rows_preview=sql_rows_preview,
                    rag_context_snippets=rag_context_snippets,
                ),
                error="LLM final composer returned empty answer.",
            )

        return ComposeResult(answer=answer, error=None)

    def _parse_from_text(self, response: object) -> ComposerOutputSchema | None:
        raw_output = getattr(response, "output_text", "") or ""
        payload = extract_first_json_object(raw_output)
        if not payload:
            return None
        try:
            return ComposerOutputSchema.model_validate(payload)
        except ValidationError:
            return None

    def _deterministic_fallback(
        self,
        *,
        question: str,
        intent: str,
        entities: Sequence[dict[str, Any]],
        used_sql: bool,
        used_rag: bool,
        sql_rows_preview: Sequence[dict[str, Any]],
        rag_context_snippets: Sequence[dict[str, Any]],
    ) -> str:
        parts: list[str] = []
        entity_names = [
            str(item.get("company_name")).strip()
            for item in entities
            if str(item.get("company_name", "")).strip()
        ]
        if entity_names:
            unique_names = list(dict.fromkeys(entity_names))
            parts.append(f"This answer focuses on: {', '.join(unique_names[:5])}.")

        if used_sql and sql_rows_preview:
            first_row = sql_rows_preview[0]
            preferred_keys = (
                "company_name",
                "ticker",
                "price",
                "target_price",
                "dividend_yield",
                "recommendation",
                "sector_level_1",
                "region",
            )
            metrics: list[str] = []
            for key in preferred_keys:
                if key in first_row and first_row[key] is not None and str(first_row[key]).strip():
                    metrics.append(f"{key}={first_row[key]}")
                if len(metrics) >= 4:
                    break
            if metrics:
                parts.append("Structured data highlights: " + ", ".join(metrics) + ".")

        if used_rag and rag_context_snippets:
            snippet_text = str(rag_context_snippets[0].get("text", "")).strip()
            snippet_text = re.sub(r"\s+", " ", snippet_text)
            if snippet_text:
                parts.append("Research context: " + snippet_text[:320].rstrip() + ".")

        if not parts:
            base = (
                "I don’t have enough reliable data to answer confidently. "
                "Please provide a more specific question or additional context."
            )
            return base[: self.max_answer_chars].rstrip()

        answer = " ".join(parts)
        return answer
