from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from pathlib import Path
from typing import Sequence

from pypdf import PdfReader

from app.core.settings import get_settings
from app.core.utils import collapse_spaces
from app.pipeline.ingest.pdf.models import DocumentMetadata

LOGGER = logging.getLogger("ingest_pdfs")

DOCUMENTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        file_sha256 TEXT NOT NULL,
        doc_version TEXT NOT NULL,
        title TEXT,
        publisher TEXT,
        year INTEGER,
        meta_confidence REAL NOT NULL,
        meta_source TEXT NOT NULL,
        title_source TEXT NOT NULL,
        extractor_evidence TEXT,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
""".strip()


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def build_doc_id(file_hash: str, doc_version: str) -> str:
    return f"pdf_{doc_version}_{file_hash[:16]}"


def ensure_documents_table(connection: sqlite3.Connection) -> None:
    connection.execute(DOCUMENTS_TABLE_SQL)


def upsert_document_metadata(
    connection: sqlite3.Connection,
    doc_id: str,
    pdf_path: Path,
    doc_version: str,
    file_hash: str,
    metadata: DocumentMetadata,
) -> None:
    connection.execute(
        """
        INSERT INTO documents (
            doc_id,
            file_name,
            file_path,
            file_sha256,
            doc_version,
            title,
            publisher,
            year,
            meta_confidence,
            meta_source,
            title_source,
            extractor_evidence,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(doc_id) DO UPDATE SET
            file_name = excluded.file_name,
            file_path = excluded.file_path,
            file_sha256 = excluded.file_sha256,
            doc_version = excluded.doc_version,
            title = excluded.title,
            publisher = excluded.publisher,
            year = excluded.year,
            meta_confidence = excluded.meta_confidence,
            meta_source = excluded.meta_source,
            title_source = excluded.title_source,
            extractor_evidence = excluded.extractor_evidence,
            updated_at = CURRENT_TIMESTAMP;
        """,
        (
            doc_id,
            pdf_path.name,
            str(pdf_path),
            file_hash,
            doc_version,
            metadata.title,
            metadata.publisher,
            metadata.year,
            metadata.confidence,
            metadata.meta_source,
            metadata.title_source,
            json.dumps(metadata.evidence, ensure_ascii=False),
        ),
    )


def resolve_input_paths(input_paths: Sequence[Path] | None, default_input_dir: Path) -> list[Path]:
    if input_paths:
        candidates = list(input_paths)
    else:
        candidates = [default_input_dir]

    files: list[Path] = []
    for candidate in candidates:
        if candidate.is_file() and candidate.suffix.casefold() == ".pdf":
            files.append(candidate)
            continue
        if candidate.is_dir():
            files.extend(sorted(path for path in candidate.glob("*.pdf") if path.is_file()))
            continue
        raise FileNotFoundError(f"Input path is not a PDF file or directory: {candidate}")

    unique_files = sorted({path.resolve() for path in files})
    if not unique_files:
        raise FileNotFoundError("No PDF files found for ingestion.")
    return unique_files


def extract_pdf_pages(pdf_path: Path) -> list[tuple[int, str]]:
    reader = PdfReader(str(pdf_path))
    pages: list[tuple[int, str]] = []
    for page_number, page in enumerate(reader.pages, start=1):
        extracted = page.extract_text() or ""
        cleaned = collapse_spaces(extracted)
        if not cleaned:
            LOGGER.warning("Skipped empty page %s in %s", page_number, pdf_path.name)
            continue
        pages.append((page_number, cleaned))
    return pages


def build_metadata_preview(pages: Sequence[tuple[int, str]], max_pages: int = 3) -> str:
    settings = get_settings()
    max_chars_per_page = settings.pdf_metadata_page_char_limit
    max_total_chars = settings.pdf_metadata_total_char_limit

    parts: list[str] = []
    total_chars = 0
    for page_number, text in pages[:max_pages]:
        if total_chars >= max_total_chars:
            break
        clipped = text[:max_chars_per_page].strip()
        if not clipped:
            continue
        budget = max_total_chars - total_chars
        clipped = clipped[:budget].strip()
        if not clipped:
            continue
        block = f"[Page {page_number}]\n{clipped}"
        parts.append(block)
        total_chars += len(block)
    return "\n\n".join(parts)
