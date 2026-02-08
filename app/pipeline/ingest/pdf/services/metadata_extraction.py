from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path

from openai import OpenAI
from pydantic import ValidationError

from app.core.settings import get_settings
from app.core.utils import collapse_spaces, extract_first_json_object, read_text_file
from app.pipeline.ingest.pdf.models import DocumentMetadata
from app.pipeline.ingest.pdf.schemas import MetadataExtractionSchema

LOGGER = logging.getLogger("ingest_pdfs")


def prettify_filename(file_name: str) -> str:
    stem = Path(file_name).stem
    spaced = re.sub(r"[_\-]+", " ", stem)
    spaced = collapse_spaces(spaced)
    if not spaced:
        return "Untitled Document"
    return " ".join(word.capitalize() for word in spaced.split(" "))


def sanitize_year(raw_year: object) -> int | None:
    if raw_year is None:
        return None
    if isinstance(raw_year, int):
        return raw_year if 1900 <= raw_year <= 2100 else None
    if isinstance(raw_year, float) and raw_year.is_integer():
        converted = int(raw_year)
        return converted if 1900 <= converted <= 2100 else None
    if isinstance(raw_year, str):
        match = re.search(r"(19|20)\d{2}", raw_year)
        if match:
            converted = int(match.group(0))
            return converted if 1900 <= converted <= 2100 else None
    return None


def sanitize_confidence(raw_confidence: object) -> float:
    try:
        numeric = float(raw_confidence)
    except (TypeError, ValueError):
        return 0.0
    return min(1.0, max(0.0, numeric))


def sanitize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = collapse_spaces(str(value))
    return text if text else None


@lru_cache(maxsize=1)
def _load_default_prompt_template() -> str:
    settings = get_settings()
    return read_text_file(
        settings.pdf_metadata_extraction_prompt_path,
        missing_message="PDF metadata extraction prompt file not found: {path}",
    )


def load_metadata_prompt_template(prompt_path: Path | None = None) -> str:
    if prompt_path is None:
        return _load_default_prompt_template()
    return read_text_file(
        prompt_path,
        missing_message="PDF metadata extraction prompt file not found: {path}",
    )


def build_metadata_prompt(
    *,
    file_name: str,
    preview_text: str,
    prompt_template: str,
) -> str:
    return (
        prompt_template
        .replace("{{file_name}}", file_name)
        .replace("{{preview_text}}", preview_text)
    ).strip()


def parse_metadata_from_output_text(raw_output: str) -> MetadataExtractionSchema | None:
    if not raw_output:
        return None
    parsed_json = extract_first_json_object(raw_output)
    if not parsed_json:
        return None
    try:
        return MetadataExtractionSchema.model_validate(parsed_json)
    except ValidationError:
        return None


def build_non_schema_reason(response: object) -> str:
    status = getattr(response, "status", None)
    error = getattr(response, "error", None)
    incomplete = getattr(response, "incomplete_details", None)

    refusal_preview: str | None = None
    output_items = getattr(response, "output", None) or []
    for item in output_items:
        content = getattr(item, "content", None) or []
        for entry in content:
            if getattr(entry, "type", None) == "refusal":
                refusal_text = collapse_spaces(str(getattr(entry, "refusal", "")))
                refusal_preview = refusal_text[:180] if refusal_text else "refusal_present"
                break
        if refusal_preview:
            break

    parts: list[str] = []
    if status is not None:
        parts.append(f"status={status}")
    if error:
        parts.append(f"error={error}")
    if incomplete:
        parts.append(f"incomplete={incomplete}")
    if refusal_preview:
        parts.append(f"refusal={refusal_preview}")
    if not parts:
        return "unknown_reason"
    return "; ".join(parts)


def extract_metadata_with_llm(
    openai_client: OpenAI,
    extractor_model: str,
    file_name: str,
    preview_text: str,
    confidence_threshold: float,
) -> DocumentMetadata:
    fallback_title = prettify_filename(file_name)
    fallback = DocumentMetadata(
        title=fallback_title,
        publisher="Unknown",
        year=None,
        confidence=0.0,
        evidence={"title_line": None, "publisher_line": None, "year_line": None},
        meta_source="filename_fallback",
        title_source="filename_fallback",
    )

    if not preview_text:
        return fallback

    prompt_template = load_metadata_prompt_template()
    prompt = build_metadata_prompt(
        file_name=file_name,
        preview_text=preview_text,
        prompt_template=prompt_template,
    )

    settings = get_settings()
    max_output_tokens = settings.pdf_metadata_max_output_tokens
    response = None
    for attempt in range(2):
        try:
            response = openai_client.responses.parse(
                model=extractor_model,
                max_output_tokens=max_output_tokens,
                reasoning={"effort": "minimal"},
                text={"verbosity": "low"},
                input=prompt,
                text_format=MetadataExtractionSchema,
            )
        except Exception as exc:
            LOGGER.warning("Metadata extractor failed for %s: %s", file_name, exc)
            return fallback

        incomplete = getattr(response, "incomplete_details", None)
        incomplete_reason = getattr(incomplete, "reason", None)
        if incomplete_reason == "max_output_tokens" and attempt == 0:
            max_output_tokens = min(max_output_tokens * 2, 4000)
            LOGGER.warning(
                "Metadata extractor incomplete for %s due max_output_tokens. Retrying with %s tokens.",
                file_name,
                max_output_tokens,
            )
            continue
        break

    if response is None:
        return fallback

    parsed = getattr(response, "output_parsed", None)
    if not isinstance(parsed, MetadataExtractionSchema):
        raw_output = getattr(response, "output_text", "") or ""
        parsed_from_text = parse_metadata_from_output_text(raw_output)
        if parsed_from_text is not None:
            parsed = parsed_from_text
        else:
            reason = build_non_schema_reason(response)
            preview = collapse_spaces(raw_output)[:220] if raw_output else ""
            LOGGER.warning(
                "Metadata extractor returned non-schema output for %s. reason=%s preview=%s",
                file_name,
                reason,
                preview,
            )
            return fallback

    title = sanitize_optional_string(parsed.title)
    publisher = sanitize_optional_string(parsed.publisher)
    year = sanitize_year(parsed.year)
    confidence = sanitize_confidence(parsed.confidence)

    evidence = {
        "title_line": sanitize_optional_string(parsed.evidence.title_line),
        "publisher_line": sanitize_optional_string(parsed.evidence.publisher_line),
        "year_line": sanitize_optional_string(parsed.evidence.year_line),
    }

    missing_all = title is None and publisher is None and year is None
    if confidence < confidence_threshold or missing_all:
        return fallback

    meta_source = "llm"
    title_source = "llm"

    if title is None:
        title = fallback_title
        title_source = "filename_fallback"
        meta_source = "llm_partial"

    return DocumentMetadata(
        title=title,
        publisher=publisher,
        year=year,
        confidence=confidence,
        evidence=evidence,
        meta_source=meta_source,
        title_source=title_source,
    )
