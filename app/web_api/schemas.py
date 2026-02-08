from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1)

    @field_validator("question")
    @classmethod
    def validate_question(cls, value: str) -> str:
        normalized = " ".join(value.split()).strip()
        if not normalized:
            raise ValueError("question must not be empty")
        return normalized


class SourceItem(BaseModel):
    title: str | None = None
    publisher: str | None = None
    year: int | None = None
    page: int | None = None
    quote_snippet: str | None = None


class AskResponse(BaseModel):
    question: str
    answer: str
    sources: list[SourceItem] = Field(default_factory=list)
    entities: list[dict[str, Any]] | None = None
    used_sql: bool | None = None
    used_rag: bool | None = None
    sql: str | None = None
    sql_rows_preview: list[dict[str, Any]] | None = None
    errors: list[dict[str, Any]] | None = None


class UploadSkippedDocumentResponse(BaseModel):
    file_name: str
    reason: str
    details: str | None = None


class UploadPDFResponse(BaseModel):
    accepted: list[str] = Field(default_factory=list)
    skipped_documents: list[UploadSkippedDocumentResponse] = Field(default_factory=list)


class UploadSkippedEquityResponse(BaseModel):
    isin: str | None = None
    reason: str
    row_number: int | None = None


class UploadEquitiesResponse(BaseModel):
    file_name: str
    added_count: int
    updated_count: int
    skipped_count: int
    skipped: list[UploadSkippedEquityResponse] = Field(default_factory=list)

