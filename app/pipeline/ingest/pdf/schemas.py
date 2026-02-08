from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MetadataEvidenceSchema(StrictSchema):
    title_line: str | None = None
    publisher_line: str | None = None
    year_line: str | None = None


class MetadataExtractionSchema(StrictSchema):
    title: str | None = None
    publisher: str | None = None
    year: int | None = None
    confidence: float = 0.0
    evidence: MetadataEvidenceSchema = Field(default_factory=MetadataEvidenceSchema)
