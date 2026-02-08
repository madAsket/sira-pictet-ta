from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, ValidationError

from app.core.sqlite_schema import schema_lines_from_db
from app.core.settings import get_settings
from app.core.utils import extract_first_json_object, read_text_file
from app.domain.equities.schema import COLUMN_SPECS

LOGGER = logging.getLogger("text_to_sql")
DEFAULT_DB_PATH = Path("db/equities.db")


@dataclass(frozen=True)
class SQLGenerationResult:
    sql: str | None
    notes: str | None
    error: str | None


class SQLQuerySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sql: str
    notes: str | None = None


def _load_prompt(prompt_path: Path | None = None) -> str:
    settings = get_settings()
    target = prompt_path or settings.sql_text_to_sql_prompt_path
    return read_text_file(
        target,
        missing_message="Text-to-SQL prompt file not found: {path}",
    )


def _load_user_prompt(prompt_path: Path | None = None) -> str:
    settings = get_settings()
    target = prompt_path or settings.sql_text_to_sql_user_prompt_path
    return read_text_file(
        target,
        missing_message="Text-to-SQL user prompt template not found: {path}",
    )


def _strip_code_fences(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_+-]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


class TextToSQLGenerator:
    def __init__(
        self,
        *,
        model: str | None = None,
        db_path: Path = DEFAULT_DB_PATH,
        prompt_path: Path | None = None,
        openai_client: OpenAI | None = None,
    ) -> None:
        self.settings = get_settings()
        self.model = (
            model
            or self.settings.txt2sql_model
        )
        self.max_output_tokens = self.settings.openai_txt2sql_max_output_tokens
        self.reasoning_effort = self.settings.openai_txt2sql_reasoning_effort
        self.db_path = db_path
        self.prompt = _load_prompt(prompt_path=prompt_path)
        self.user_prompt_template = _load_user_prompt()
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

    def generate(
        self,
        *,
        question: str,
        entities: Sequence[dict[str, Any]],
        company_specific: bool,
        intent: str,
    ) -> SQLGenerationResult:
        question_clean = question.strip()
        if not question_clean:
            return SQLGenerationResult(sql=None, notes=None, error="Question is empty.")

        client = self._client()
        if client is None:
            return SQLGenerationResult(sql=None, notes=None, error="OPENAI_API_KEY is missing.")

        entities_payload = self._build_entities_context(entities=entities)
        user_prompt = self._build_user_prompt(
            question=question_clean,
            intent=intent,
            company_specific=company_specific,
            entities_payload=entities_payload,
            schema_context=self.schema_context,
        )
        try:
            response = client.responses.parse(
                model=self.model,
                max_output_tokens=self.max_output_tokens,
                reasoning={"effort": self.reasoning_effort},
                input=[
                    {"role": "system", "content": self.prompt},
                    {"role": "user", "content": user_prompt},
                ],
                text_format=SQLQuerySchema,
            )
        except Exception as exc:
            LOGGER.warning("Text-to-SQL generation failed: %s", exc)
            return SQLGenerationResult(
                sql=None,
                notes=None,
                error=f"LLM text-to-sql generation failed ({exc}).",
            )

        parsed = getattr(response, "output_parsed", None)
        if not isinstance(parsed, SQLQuerySchema):
            parsed = self._parse_from_text(response)
        if not isinstance(parsed, SQLQuerySchema):
            return SQLGenerationResult(
                sql=None,
                notes=None,
                error=self._build_non_schema_error(response),
            )

        sql_text = _strip_code_fences(parsed.sql)
        if not sql_text:
            return SQLGenerationResult(sql=None, notes=parsed.notes, error="LLM returned empty SQL.")

        return SQLGenerationResult(
            sql=sql_text,
            notes=(parsed.notes or "").strip() or None,
            error=None,
        )

    def _parse_from_text(self, response: object) -> SQLQuerySchema | None:
        raw_output = getattr(response, "output_text", "") or ""
        payload = extract_first_json_object(raw_output)
        if not payload:
            return None
        try:
            return SQLQuerySchema.model_validate(payload)
        except ValidationError:
            return None

    def _build_schema_context(self) -> str:
        schema_lines = self._schema_lines_from_db()
        if not schema_lines:
            schema_lines = [f"{spec.name} ({spec.sqlite_type})" for spec in COLUMN_SPECS]
        return "\n".join(f"- {line}" for line in schema_lines)

    def _schema_lines_from_db(self) -> list[str]:
        return schema_lines_from_db(self.db_path, "equities")

    def _build_entities_context(self, *, entities: Sequence[dict[str, Any]]) -> str:
        try:
            return json.dumps(list(entities), ensure_ascii=False, indent=2)
        except TypeError:
            return "[]"

    def _build_user_prompt(
        self,
        *,
        question: str,
        intent: str,
        company_specific: bool,
        entities_payload: str,
        schema_context: str,
    ) -> str:
        return (
            self.user_prompt_template
            .replace("{{question}}", question)
            .replace("{{intent}}", intent)
            .replace("{{company_specific}}", str(company_specific).lower())
            .replace("{{entities_json}}", entities_payload)
            .replace("{{schema_context}}", schema_context)
        ).strip()

    def _build_non_schema_error(self, response: object) -> str:
        status = getattr(response, "status", "unknown")
        incomplete_details = getattr(response, "incomplete_details", None)
        incomplete_reason = getattr(incomplete_details, "reason", None)
        if not incomplete_reason and isinstance(incomplete_details, dict):
            incomplete_reason = incomplete_details.get("reason")

        response_error = getattr(response, "error", None)
        error_message = None
        if response_error is not None:
            error_message = getattr(response_error, "message", None)
            if not error_message and isinstance(response_error, dict):
                error_message = response_error.get("message")

        preview = (getattr(response, "output_text", "") or "").strip()
        if len(preview) > 240:
            preview = preview[:240].rstrip() + "..."

        details: list[str] = [f"status={status}"]
        if incomplete_reason:
            details.append(f"incomplete_reason={incomplete_reason}")
        if error_message:
            details.append(f"model_error={error_message}")
        if preview:
            details.append(f"preview={preview}")

        return "LLM returned non-schema SQL output (" + "; ".join(details) + ")."
