from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, ValidationError

from app.core.sqlite_schema import schema_lines_from_db
from app.core.settings import get_settings
from app.core.utils import extract_first_json_object, read_text_file
from app.domain.equities.schema import COLUMN_SPECS

LOGGER = logging.getLogger("router")

IntentType = Literal["equity_only", "macro_only", "hybrid", "unknown"]

DEFAULT_DB_PATH = Path("db/equities.db")


@dataclass(frozen=True)
class IntentDecision:
    intent: IntentType
    raw_intent: IntentType
    company_specific: bool
    confidence: float
    reason: str


class IntentSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")
    intent: IntentType
    company_specific: bool
    confidence: float
    reason: str


def load_intent_prompt(prompt_path: Path | None = None) -> str:
    settings = get_settings()
    target = prompt_path or settings.router_prompt_path
    return read_text_file(
        target,
        missing_message="Intent prompt file not found: {path}",
    )


def load_intent_user_prompt(prompt_path: Path | None = None) -> str:
    settings = get_settings()
    target = prompt_path or settings.router_user_prompt_path
    return read_text_file(
        target,
        missing_message="Intent user prompt template not found: {path}",
    )


def _fallback_decision(reason: str) -> IntentDecision:
    return IntentDecision(
        intent="unknown",
        raw_intent="unknown",
        company_specific=False,
        confidence=0.0,
        reason=reason,
    )


class IntentRouter:
    def __init__(
        self,
        *,
        model: str | None = None,
        prompt_path: Path | None = None,
        db_path: Path = DEFAULT_DB_PATH,
        openai_client: OpenAI | None = None,
    ) -> None:
        self.settings = get_settings()
        self.model = model or self.settings.openai_copilot_model
        self.prompt = load_intent_prompt(prompt_path=prompt_path)
        self.user_prompt_template = load_intent_user_prompt()
        self.db_path = db_path
        self.schema_context = self._build_schema_context()
        self._openai_client = openai_client

    def _client(self) -> OpenAI | None:
        if self._openai_client is not None:
            return self._openai_client
        api_key = self.settings.openai_api_key
        if not api_key:
            return None
        self._openai_client = OpenAI(api_key=api_key)
        return self._openai_client

    def classify(self, question: str) -> IntentDecision:
        question_clean = question.strip()
        if not question_clean:
            return _fallback_decision("Question is empty; defaulted to unknown.")

        client = self._client()
        if client is None:
            return _fallback_decision("OPENAI_API_KEY is missing; defaulted to unknown.")

        try:
            user_prompt = self._build_user_prompt(
                question=question_clean,
                schema_context=self.schema_context,
            )
            response = client.responses.parse(
                model=self.model,
                max_output_tokens=200,
                reasoning={"effort": "minimal"},
                text={"verbosity": "low"},
                input=[
                    {"role": "system", "content": self.prompt},
                    {"role": "user", "content": user_prompt},
                ],
                text_format=IntentSchema,
            )
        except Exception as exc:
            LOGGER.warning("Router classification failed: %s", exc)
            return _fallback_decision(f"LLM classification failed ({exc}); defaulted to unknown.")

        parsed = getattr(response, "output_parsed", None)
        if not isinstance(parsed, IntentSchema):
            parsed = self._parse_from_text(response)
        if not isinstance(parsed, IntentSchema):
            status = getattr(response, "status", "unknown")
            return _fallback_decision(f"LLM returned non-schema result (status={status}); defaulted to unknown.")

        raw_intent: IntentType = parsed.intent
        company_specific = bool(parsed.company_specific)
        effective_intent: IntentType = raw_intent
        confidence = min(1.0, max(0.0, float(parsed.confidence)))
        reason = parsed.reason.strip() if parsed.reason else ""
        return IntentDecision(
            intent=effective_intent,
            raw_intent=raw_intent,
            company_specific=company_specific,
            confidence=confidence,
            reason=reason or "No reason returned by classifier.",
        )

    def _build_schema_context(self) -> str:
        schema_lines = self._schema_lines_from_db()
        if not schema_lines:
            schema_lines = [f"{spec.name} ({spec.sqlite_type})" for spec in COLUMN_SPECS]
        return "\n".join(f"- {line}" for line in schema_lines)

    def _schema_lines_from_db(self) -> list[str]:
        return schema_lines_from_db(self.db_path, "equities")

    def _parse_from_text(self, response: object) -> IntentSchema | None:
        raw_output = getattr(response, "output_text", "") or ""
        payload = extract_first_json_object(raw_output)
        if not payload:
            return None
        try:
            return IntentSchema.model_validate(payload)
        except ValidationError:
            return None

    def _build_user_prompt(self, *, question: str, schema_context: str) -> str:
        return (
            self.user_prompt_template
            .replace("{{question}}", question)
            .replace("{{schema_context}}", schema_context)
        ).strip()
