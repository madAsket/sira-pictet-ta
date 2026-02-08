from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, ValidationError

from app.core.settings import get_settings
from app.core.utils import collapse_spaces, extract_first_json_object, read_text_file

@dataclass(frozen=True)
class TopicDecision:
    is_relevant: bool
    confidence: float
    reason: str


class TopicDecisionSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")
    is_relevant: bool
    confidence: float
    reason: str


def _load_prompt(prompt_path: Path | None = None) -> str:
    settings = get_settings()
    target = prompt_path or settings.upload_topic_prompt_path
    return read_text_file(
        target,
        missing_message="Topic classifier prompt not found: {path}",
    )


def _normalize_confidence(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


class PDFTopicClassifier:
    def __init__(
        self,
        *,
        model: str | None = None,
        prompt_path: Path | None = None,
        openai_client: OpenAI | None = None,
    ) -> None:
        self.settings = get_settings()
        self.model = model or self.settings.topic_classifier_model
        self.prompt = _load_prompt(prompt_path=prompt_path)
        self.max_output_tokens = self.settings.pdf_topic_max_output_tokens
        self.reasoning_effort = self.settings.pdf_topic_reasoning_effort
        self._openai_client = openai_client

    def _client(self) -> OpenAI | None:
        if self._openai_client is not None:
            return self._openai_client
        api_key = self.settings.openai_api_key
        if not api_key:
            return None
        self._openai_client = OpenAI(api_key=api_key)
        return self._openai_client

    def classify(self, *, file_name: str, preview_text: str) -> TopicDecision:
        normalized_preview = collapse_spaces(preview_text)
        if not normalized_preview:
            return TopicDecision(is_relevant=False, confidence=1.0, reason="No extractable text.")

        client = self._client()
        if client is None:
            return TopicDecision(is_relevant=True, confidence=0.5, reason="Topic check skipped: missing OPENAI_API_KEY.")

        payload = {
            "file_name": file_name,
            "preview_text": normalized_preview[:8000],
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
                text_format=TopicDecisionSchema,
            )
        except Exception as exc:
            return TopicDecision(is_relevant=True, confidence=0.5, reason=f"Topic check fallback: {exc}")

        parsed = getattr(response, "output_parsed", None)
        if not isinstance(parsed, TopicDecisionSchema):
            raw_output = getattr(response, "output_text", "") or ""
            parsed_json = extract_first_json_object(raw_output)
            if parsed_json:
                try:
                    parsed = TopicDecisionSchema.model_validate(parsed_json)
                except ValidationError:
                    parsed = None
        if not isinstance(parsed, TopicDecisionSchema):
            status = getattr(response, "status", "unknown")
            return TopicDecision(
                is_relevant=True,
                confidence=0.5,
                reason=f"Topic check fallback: non-schema output (status={status}).",
            )

        return TopicDecision(
            is_relevant=parsed.is_relevant,
            confidence=_normalize_confidence(parsed.confidence),
            reason=collapse_spaces(parsed.reason) or "No reason.",
        )
