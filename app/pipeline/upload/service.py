from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from app.core.settings import get_settings
from app.pipeline.ingest.equities.pipeline import EquitiesIngestPipeline
from app.pipeline.ingest.equities.services.upsert_policy import SkippedEquity as IngestSkippedEquity
from app.pipeline.ingest.pdf.models import IngestPDFReport
from app.pipeline.ingest.pdf.pipeline import PDFIngestPipeline, PDFIngestRequest
from app.pipeline.ingest.pdf.services.topic_classifier import PDFTopicClassifier


@dataclass(frozen=True)
class UploadedBinary:
    file_name: str
    content: bytes


@dataclass(frozen=True)
class SkippedDocument:
    file_name: str
    reason: str
    details: str | None = None


@dataclass(frozen=True)
class PDFUploadSummary:
    accepted: list[str]
    skipped_documents: list[SkippedDocument]


@dataclass(frozen=True)
class EquitiesUploadSummary:
    file_name: str
    added_count: int
    updated_count: int
    skipped_count: int
    skipped: list[IngestSkippedEquity]


def _sanitize_file_name(file_name: str, *, fallback: str) -> str:
    candidate = Path(file_name or "").name
    candidate = re.sub(r"[^A-Za-z0-9._-]", "_", candidate).strip("._")
    if not candidate:
        return fallback
    return candidate


def _unique_file_path(base_dir: Path, file_name: str) -> Path:
    target = base_dir / file_name
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    for index in range(1, 10_000):
        candidate = base_dir / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Failed to allocate unique file path for {file_name}.")


class UploadService:
    def __init__(
        self,
        *,
        db_path: Path = Path("db/equities.db"),
        upload_pdf_dir: Path = Path("data/uploads/pdf"),
        upload_equities_dir: Path = Path("data/uploads/equities"),
        topic_classifier: PDFTopicClassifier | None = None,
        pdf_ingest_pipeline: PDFIngestPipeline | None = None,
        equities_ingest_pipeline: EquitiesIngestPipeline | None = None,
    ) -> None:
        self.settings = get_settings()
        self.db_path = db_path
        self.upload_pdf_dir = upload_pdf_dir
        self.upload_equities_dir = upload_equities_dir
        self.topic_classifier = topic_classifier or PDFTopicClassifier()
        self.pdf_ingest_pipeline = pdf_ingest_pipeline or PDFIngestPipeline()
        self.equities_ingest_pipeline = equities_ingest_pipeline or EquitiesIngestPipeline()

        self.max_pdf_files = self.settings.api_upload_pdf_max_files
        self.max_pdf_file_size_bytes = self.settings.api_upload_pdf_max_file_bytes
        self.max_equities_file_size_bytes = self.settings.api_upload_equities_max_file_bytes
        self.topic_min_confidence = self.settings.pdf_topic_min_confidence

    def upload_pdfs(self, files: Sequence[UploadedBinary]) -> PDFUploadSummary:
        payloads = list(files)
        if not payloads:
            raise ValueError("At least one PDF file is required.")
        if len(payloads) > self.max_pdf_files:
            raise ValueError(f"Too many PDF files. Maximum allowed is {self.max_pdf_files}.")

        self.upload_pdf_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        skipped_documents: list[SkippedDocument] = []
        ingest_queue: list[Path] = []
        for item in payloads:
            safe_name = _sanitize_file_name(item.file_name, fallback="upload.pdf")
            if not safe_name.casefold().endswith(".pdf"):
                skipped_documents.append(
                    SkippedDocument(file_name=safe_name, reason="invalid_format", details="Only .pdf is supported.")
                )
                continue

            file_size = len(item.content)
            if file_size > self.max_pdf_file_size_bytes:
                skipped_documents.append(
                    SkippedDocument(
                        file_name=safe_name,
                        reason="file_too_large",
                        details=f"File exceeds {self.max_pdf_file_size_bytes} bytes.",
                    )
                )
                continue

            target_path = _unique_file_path(self.upload_pdf_dir, safe_name)
            target_path.write_bytes(item.content)
            ingest_queue.append(target_path)

        if not ingest_queue:
            return PDFUploadSummary(accepted=[], skipped_documents=skipped_documents)

        try:
            request = PDFIngestRequest(
                input_paths=ingest_queue,
                metadata_db_path=self.db_path,
                qdrant_url=self.settings.qdrant_url,
                qdrant_collection=self.settings.qdrant_collection,
                embedding_model=self.settings.openai_embedding_model,
                extractor_model=self.settings.openai_extractor_model,
                chunk_size_tokens=self.settings.pdf_chunk_size_tokens,
                chunk_overlap_ratio=self.settings.pdf_chunk_overlap_ratio,
                dedup_similarity=self.settings.pdf_dedup_similarity,
                metadata_confidence_threshold=self.settings.pdf_metadata_confidence_threshold,
                doc_version=self.settings.pdf_doc_version,
                batch_size=self.settings.pdf_upload_batch_size,
                default_input_dir=self.upload_pdf_dir,
                topic_classifier=self.topic_classifier,
                topic_min_confidence=self.topic_min_confidence,
                skip_duplicates_by_sha256=True,
                delete_skipped_files=True,
                fail_on_no_upload=False,
            )
            ingest_report: IngestPDFReport = self.pdf_ingest_pipeline.process(request)
        except Exception as exc:
            for pdf_path in ingest_queue:
                skipped_documents.append(
                    SkippedDocument(
                        file_name=pdf_path.name,
                        reason="failed_ingest",
                        details=str(exc),
                    )
                )
            return PDFUploadSummary(accepted=[], skipped_documents=skipped_documents)

        skipped_documents.extend(
            [
                SkippedDocument(file_name=item.file_name, reason=item.reason, details=item.details)
                for item in ingest_report.skipped_documents
            ]
        )
        return PDFUploadSummary(
            accepted=list(ingest_report.accepted),
            skipped_documents=skipped_documents,
        )

    def upload_equities(self, file: UploadedBinary) -> EquitiesUploadSummary:
        safe_name = _sanitize_file_name(file.file_name, fallback="upload.xlsx")
        if not safe_name.casefold().endswith(".xlsx"):
            raise ValueError("Invalid equities file format. Only .xlsx is supported.")
        if len(file.content) > self.max_equities_file_size_bytes:
            raise ValueError(f"Equities file exceeds {self.max_equities_file_size_bytes} bytes.")

        self.upload_equities_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        target_path = _unique_file_path(self.upload_equities_dir, safe_name)
        target_path.write_bytes(file.content)

        try:
            completed = self.equities_ingest_pipeline.process(
                input_path=target_path,
                db_path=self.db_path,
                mode="append",
            )
        finally:
            target_path.unlink(missing_ok=True)

        skipped_items = [
            item
            for item in completed.skipped
            if isinstance(item, IngestSkippedEquity)
        ]

        return EquitiesUploadSummary(
            file_name=safe_name,
            added_count=completed.added_count,
            updated_count=completed.updated_count,
            skipped_count=completed.skipped_count,
            skipped=skipped_items,
        )
