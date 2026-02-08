from __future__ import annotations

import re
from typing import Any

from app.web_api.schemas import (
    AskResponse,
    UploadEquitiesResponse,
    UploadPDFResponse,
    UploadSkippedDocumentResponse,
    UploadSkippedEquityResponse,
)


def compact_sql(sql: str | None, *, max_chars: int = 260) -> str:
    if not sql:
        return "-"
    compact = re.sub(r"\s+", " ", sql).strip()
    if len(compact) > max_chars:
        return compact[: max_chars - 3].rstrip() + "..."
    return compact


def entity_log_value(entities: list[dict[str, Any]]) -> str:
    values: list[str] = []
    for item in entities:
        isin = str(item.get("isin", "")).strip() or "-"
        confidence = item.get("confidence")
        confidence_text = "-" if confidence is None else str(confidence)
        values.append(f"{isin}:{confidence_text}")
    return ",".join(values) if values else "-"


def source_log_value(sources: list[dict[str, Any]]) -> str:
    values: list[str] = []
    for item in sources:
        title = str(item.get("title", "")).strip() or "unknown"
        page = item.get("page")
        if page is None:
            values.append(title)
        else:
            values.append(f"{title}|p{page}")
    return ",".join(values) if values else "-"


def read_field(payload: Any, name: str, default: Any = None) -> Any:
    if isinstance(payload, dict):
        return payload.get(name, default)
    return getattr(payload, name, default)


def build_response(result: Any, *, include_debug: bool) -> AskResponse:
    if not include_debug:
        return AskResponse(
            question=read_field(result, "question", ""),
            answer=read_field(result, "answer", ""),
            sources=read_field(result, "sources", []),
        )
    return AskResponse(
        question=read_field(result, "question", ""),
        answer=read_field(result, "answer", ""),
        sources=read_field(result, "sources", []),
        entities=read_field(result, "entities", []),
        used_sql=read_field(result, "used_sql", False),
        used_rag=read_field(result, "used_rag", False),
        sql=read_field(result, "sql"),
        sql_rows_preview=read_field(result, "sql_rows_preview", []),
        errors=read_field(result, "errors", []),
    )


def to_upload_pdf_response(summary: Any) -> UploadPDFResponse:
    accepted = list(getattr(summary, "accepted", []))
    skipped_documents = list(getattr(summary, "skipped_documents", []))
    return UploadPDFResponse(
        accepted=accepted,
        skipped_documents=[
            UploadSkippedDocumentResponse(
                file_name=getattr(item, "file_name", ""),
                reason=getattr(item, "reason", "unknown"),
                details=getattr(item, "details", None),
            )
            for item in skipped_documents
        ],
    )


def to_upload_equities_response(summary: Any) -> UploadEquitiesResponse:
    skipped_items = list(getattr(summary, "skipped", []))
    return UploadEquitiesResponse(
        file_name=getattr(summary, "file_name", ""),
        added_count=int(getattr(summary, "added_count", 0)),
        updated_count=int(getattr(summary, "updated_count", 0)),
        skipped_count=int(getattr(summary, "skipped_count", 0)),
        skipped=[
            UploadSkippedEquityResponse(
                isin=getattr(item, "isin", None),
                reason=getattr(item, "reason", "unknown"),
                row_number=getattr(item, "row_number", None),
            )
            for item in skipped_items
        ],
    )
