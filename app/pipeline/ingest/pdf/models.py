from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class DocumentMetadata:
    title: str | None
    publisher: str | None
    year: int | None
    confidence: float
    evidence: dict[str, str | None]
    meta_source: str
    title_source: str


@dataclass(frozen=True)
class ChunkRecord:
    point_id: str
    doc_id: str
    page: int
    chunk_index: int
    text: str
    token_count: int
    quote_snippet: str
    mentions_company_names: list[str]
    mentions_company_names_norm: list[str]
    mentions_tickers: list[str]


@dataclass(frozen=True)
class MentionCatalog:
    aliases: tuple[tuple[str, str], ...]
    ticker_patterns: tuple[tuple[str, re.Pattern[str]], ...]


@dataclass(frozen=True)
class IngestSkippedDocument:
    file_name: str
    reason: str
    details: str | None = None


@dataclass(frozen=True)
class IngestPDFReport:
    accepted: list[str]
    skipped_documents: list[IngestSkippedDocument]
    failed_docs: int
    uploaded_points: int
