from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from app.pipeline.ingest.pdf.context import PDFIngestContext
from app.pipeline.ingest.pdf.models import IngestPDFReport, IngestSkippedDocument
from app.pipeline.ingest.pdf.orchestrator import PDFIngestOrchestrator
from app.pipeline.ingest.pdf.services.ingest_service import PDFIngestService
from app.pipeline.ingest.pdf.services.topic_classifier import PDFTopicClassifier
from app.pipeline.ingest.pdf.stages import (
    ChunkStage,
    DiscoverStage,
    EmbedStage,
    MetadataExtractStage,
    TopicFilterStage,
    UpsertStage,
)


@dataclass(frozen=True)
class PDFIngestRequest:
    input_paths: Sequence[Path] | None
    metadata_db_path: Path
    qdrant_url: str
    qdrant_collection: str
    embedding_model: str
    extractor_model: str
    chunk_size_tokens: int
    chunk_overlap_ratio: float
    dedup_similarity: float
    metadata_confidence_threshold: float
    doc_version: str
    batch_size: int
    default_input_dir: Path
    topic_classifier: PDFTopicClassifier | None = None
    topic_min_confidence: float = 0.60
    skip_duplicates_by_sha256: bool = False
    delete_skipped_files: bool = False
    fail_on_no_upload: bool = True


class PDFIngestPipeline:
    def __init__(
        self,
        *,
        service: PDFIngestService | None = None,
    ) -> None:
        ingest_service = service or PDFIngestService()
        self.service = ingest_service
        self.orchestrator = PDFIngestOrchestrator(
            stages=[
                DiscoverStage(ingest_service),
                TopicFilterStage(ingest_service),
                MetadataExtractStage(ingest_service),
                ChunkStage(ingest_service),
                EmbedStage(ingest_service),
                UpsertStage(ingest_service),
            ]
        )

    def process(self, request: PDFIngestRequest) -> IngestPDFReport:
        context = PDFIngestContext(
            input_paths=list(request.input_paths or []),
            metadata_db_path=request.metadata_db_path,
            qdrant_url=request.qdrant_url,
            qdrant_collection=request.qdrant_collection,
            embedding_model=request.embedding_model,
            extractor_model=request.extractor_model,
            chunk_size_tokens=request.chunk_size_tokens,
            chunk_overlap_ratio=request.chunk_overlap_ratio,
            dedup_similarity=request.dedup_similarity,
            metadata_confidence_threshold=request.metadata_confidence_threshold,
            doc_version=request.doc_version,
            batch_size=request.batch_size,
            default_input_dir=request.default_input_dir,
            topic_min_confidence=request.topic_min_confidence,
            skip_duplicates_by_sha256=request.skip_duplicates_by_sha256,
            delete_skipped_files=request.delete_skipped_files,
            fail_on_no_upload=request.fail_on_no_upload,
            topic_classifier=request.topic_classifier,
        )
        completed = context
        try:
            completed = self.orchestrator.run(context)
            completed = self.service.finalize(completed)
        finally:
            self.service.close(completed)

        return IngestPDFReport(
            accepted=list(completed.accepted),
            skipped_documents=_normalize_skipped_documents(completed.skipped_documents),
            failed_docs=completed.failed_docs,
            uploaded_points=completed.uploaded_points,
        )


def ingest_pdfs(
    input_paths: Sequence[Path] | None,
    metadata_db_path: Path,
    qdrant_url: str,
    qdrant_collection: str,
    embedding_model: str,
    extractor_model: str,
    chunk_size_tokens: int,
    chunk_overlap_ratio: float,
    dedup_similarity: float,
    metadata_confidence_threshold: float,
    doc_version: str,
    batch_size: int,
    default_input_dir: Path,
    topic_classifier: PDFTopicClassifier | None = None,
    topic_min_confidence: float = 0.60,
    skip_duplicates_by_sha256: bool = False,
    delete_skipped_files: bool = False,
    fail_on_no_upload: bool = True,
) -> IngestPDFReport:
    pipeline = PDFIngestPipeline()
    request = PDFIngestRequest(
        input_paths=input_paths,
        metadata_db_path=metadata_db_path,
        qdrant_url=qdrant_url,
        qdrant_collection=qdrant_collection,
        embedding_model=embedding_model,
        extractor_model=extractor_model,
        chunk_size_tokens=chunk_size_tokens,
        chunk_overlap_ratio=chunk_overlap_ratio,
        dedup_similarity=dedup_similarity,
        metadata_confidence_threshold=metadata_confidence_threshold,
        doc_version=doc_version,
        batch_size=batch_size,
        default_input_dir=default_input_dir,
        topic_classifier=topic_classifier,
        topic_min_confidence=topic_min_confidence,
        skip_duplicates_by_sha256=skip_duplicates_by_sha256,
        delete_skipped_files=delete_skipped_files,
        fail_on_no_upload=fail_on_no_upload,
    )
    return pipeline.process(request)


def _normalize_skipped_documents(items: Sequence[object]) -> list[IngestSkippedDocument]:
    skipped_documents: list[IngestSkippedDocument] = []
    for item in items:
        if isinstance(item, IngestSkippedDocument):
            skipped_documents.append(item)
            continue
        if not isinstance(item, dict):
            continue
        file_name = str(item.get("file_name", "")).strip()
        reason = str(item.get("reason", "")).strip()
        details_raw = item.get("details")
        details = str(details_raw).strip() if details_raw is not None and str(details_raw).strip() else None
        if file_name and reason:
            skipped_documents.append(
                IngestSkippedDocument(
                    file_name=file_name,
                    reason=reason,
                    details=details,
                )
            )
    return skipped_documents
