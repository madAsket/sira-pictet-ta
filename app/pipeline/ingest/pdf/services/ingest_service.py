from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from openai import OpenAI
from qdrant_client import QdrantClient

from app.core.settings import get_settings
from app.pipeline.ingest.pdf.context import PDFIngestContext
from app.pipeline.ingest.pdf.models import ChunkRecord, DocumentMetadata
from app.pipeline.ingest.pdf.services.chunking import (
    build_quote_snippet,
    deduplicate_chunks,
    get_tokenizer,
    point_id_from_chunk,
    split_into_token_chunks,
)
from app.pipeline.ingest.pdf.services.document_store import (
    build_doc_id,
    build_metadata_preview,
    ensure_documents_table,
    extract_pdf_pages,
    file_sha256,
    resolve_input_paths,
    upsert_document_metadata,
)
from app.pipeline.ingest.pdf.services.mentions import detect_mentions, load_mention_catalog
from app.pipeline.ingest.pdf.services.metadata_extraction import extract_metadata_with_llm
from app.pipeline.ingest.pdf.services.vector_store import (
    chunk_records_to_points,
    embed_texts,
    ensure_qdrant_collection,
    enrich_chunk_payload,
    normalize_embedding_model_name,
    upload_points_in_batches,
)

LOGGER = logging.getLogger("ingest_pdfs")


class PDFIngestService:
    def discover(self, context: PDFIngestContext) -> PDFIngestContext:
        if context.chunk_size_tokens < 100:
            raise ValueError("chunk_size_tokens must be >= 100")
        if not (0 <= context.chunk_overlap_ratio < 1):
            raise ValueError("chunk_overlap_ratio must be in [0, 1)")
        if not (0 <= context.dedup_similarity <= 1):
            raise ValueError("dedup_similarity must be in [0, 1]")
        if not (0 <= context.metadata_confidence_threshold <= 1):
            raise ValueError("metadata_confidence_threshold must be in [0, 1]")
        if not (0 <= context.topic_min_confidence <= 1):
            raise ValueError("topic_min_confidence must be in [0, 1]")
        if context.batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if context.metadata_db_path is None:
            raise ValueError("metadata_db_path is required.")

        settings = get_settings()
        api_key = settings.openai_api_key
        if not api_key:
            raise ValueError("OPENAI_API_KEY is not configured.")

        context.resolved_inputs = resolve_input_paths(
            input_paths=context.input_paths or None,
            default_input_dir=context.default_input_dir,
        )
        context.normalized_embedding_model = normalize_embedding_model_name(context.embedding_model)
        context.tokenizer = get_tokenizer(context.normalized_embedding_model)

        context.openai_client = OpenAI(api_key=api_key)
        context.qdrant_client = QdrantClient(url=context.qdrant_url)
        try:
            context.qdrant_client.get_collections()
        except Exception as exc:
            raise RuntimeError(
                f"Cannot connect to Qdrant at {context.qdrant_url}. "
                "Check docker status and QDRANT_URL."
            ) from exc

        context.metadata_db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(context.metadata_db_path)
        ensure_documents_table(connection)
        context.mention_catalog = load_mention_catalog(connection)
        context.db_connection = connection

        if not context.mention_catalog.aliases and not context.mention_catalog.ticker_patterns:
            LOGGER.warning("Mention catalog is empty. Mention tagging will produce empty arrays.")

        documents: list[dict[str, Any]] = []
        for pdf_path in context.resolved_inputs:
            LOGGER.info("Processing PDF: %s", pdf_path)
            pages = extract_pdf_pages(pdf_path)
            if not pages:
                LOGGER.warning("No extractable text found in %s. Skipped.", pdf_path.name)
                self._append_skipped(context, pdf_path.name, reason="unreadable")
                self._delete_if_needed(context, pdf_path)
                continue

            file_hash = file_sha256(pdf_path)
            documents.append(
                {
                    "pdf_path": pdf_path,
                    "pages": pages,
                    "file_hash": file_hash,
                    "doc_id": build_doc_id(file_hash=file_hash, doc_version=context.doc_version),
                    "preview_text": build_metadata_preview(pages=pages, max_pages=3),
                }
            )
        context.documents = documents
        return context

    def topic_filter(self, context: PDFIngestContext) -> PDFIngestContext:
        connection = self._require_connection(context)
        filtered: list[dict[str, Any]] = []
        for item in context.documents:
            pdf_path = self._pdf_path(item)
            file_hash = str(item["file_hash"])

            if context.skip_duplicates_by_sha256:
                existing_doc = connection.execute(
                    "SELECT 1 FROM documents WHERE file_sha256 = ? LIMIT 1;",
                    (file_hash,),
                ).fetchone()
                if existing_doc is not None:
                    self._append_skipped(context, pdf_path.name, reason="duplicate")
                    self._delete_if_needed(context, pdf_path)
                    continue

            classifier = context.topic_classifier
            if classifier is not None:
                decision = classifier.classify(
                    file_name=pdf_path.name,
                    preview_text=str(item["preview_text"]),
                )
                if (not decision.is_relevant) and decision.confidence >= context.topic_min_confidence:
                    self._append_skipped(
                        context,
                        pdf_path.name,
                        reason="irrelevant",
                        details=decision.reason,
                    )
                    self._delete_if_needed(context, pdf_path)
                    continue

            filtered.append(item)
        context.documents = filtered
        return context

    def metadata_extract(self, context: PDFIngestContext) -> PDFIngestContext:
        openai_client = self._require_openai(context)
        for item in context.documents:
            pdf_path = self._pdf_path(item)
            item["metadata"] = extract_metadata_with_llm(
                openai_client=openai_client,
                extractor_model=context.extractor_model,
                file_name=pdf_path.name,
                preview_text=str(item["preview_text"]),
                confidence_threshold=context.metadata_confidence_threshold,
            )
        return context

    def chunk(self, context: PDFIngestContext) -> PDFIngestContext:
        tokenizer = context.tokenizer
        mention_catalog = context.mention_catalog
        if tokenizer is None:
            raise ValueError("Tokenizer is not initialized.")
        if mention_catalog is None:
            raise ValueError("Mention catalog is not initialized.")

        prepared: list[dict[str, Any]] = []
        for item in context.documents:
            pdf_path = self._pdf_path(item)
            doc_id = str(item["doc_id"])
            pages = list(item["pages"])

            chunk_records: list[ChunkRecord] = []
            for page_number, page_text in pages:
                raw_chunks = split_into_token_chunks(
                    text=page_text,
                    encoding=tokenizer,
                    chunk_size=context.chunk_size_tokens,
                    overlap_ratio=context.chunk_overlap_ratio,
                )
                unique_chunks = deduplicate_chunks(
                    chunks=raw_chunks,
                    similarity_threshold=context.dedup_similarity,
                )
                for chunk_index, (chunk_text, token_count) in enumerate(unique_chunks):
                    mentions_company_names, mentions_company_names_norm, mentions_tickers = detect_mentions(
                        chunk_text=chunk_text,
                        catalog=mention_catalog,
                    )
                    chunk_records.append(
                        ChunkRecord(
                            point_id=point_id_from_chunk(
                                doc_id=doc_id,
                                page=page_number,
                                chunk_index=chunk_index,
                                text=chunk_text,
                            ),
                            doc_id=doc_id,
                            page=page_number,
                            chunk_index=chunk_index,
                            text=chunk_text,
                            token_count=token_count,
                            quote_snippet=build_quote_snippet(chunk_text),
                            mentions_company_names=mentions_company_names,
                            mentions_company_names_norm=mentions_company_names_norm,
                            mentions_tickers=mentions_tickers,
                        )
                    )

            if not chunk_records:
                LOGGER.warning("No chunks generated for %s. Skipped upload.", pdf_path.name)
                self._append_skipped(context, pdf_path.name, reason="unreadable")
                self._delete_if_needed(context, pdf_path)
                continue

            item["chunk_records"] = chunk_records
            prepared.append(item)
        context.documents = prepared
        return context

    def embed(self, context: PDFIngestContext) -> PDFIngestContext:
        openai_client = self._require_openai(context)
        qdrant_client = self._require_qdrant(context)

        prepared: list[dict[str, Any]] = []
        for item in context.documents:
            pdf_path = self._pdf_path(item)
            chunk_records = list(item["chunk_records"])
            metadata = item.get("metadata")
            if not isinstance(metadata, DocumentMetadata):
                metadata = DocumentMetadata(
                    title=pdf_path.stem,
                    publisher="Unknown",
                    year=None,
                    confidence=0.0,
                    evidence={"title_line": None, "publisher_line": None, "year_line": None},
                    meta_source="filename_fallback",
                    title_source="filename_fallback",
                )
                item["metadata"] = metadata

            try:
                vectors = embed_texts(
                    openai_client=openai_client,
                    embedding_model=context.normalized_embedding_model,
                    texts=[record.text for record in chunk_records],
                )
                ensure_qdrant_collection(
                    qdrant_client=qdrant_client,
                    collection_name=context.qdrant_collection,
                    vector_size=len(vectors[0]),
                )
                points = chunk_records_to_points(chunk_records=chunk_records, vectors=vectors)
                enrich_chunk_payload(points=points, metadata=metadata)
            except Exception as exc:
                context.failed_docs += 1
                LOGGER.error("Failed ingest for %s: %s", pdf_path.name, exc)
                self._append_skipped(
                    context,
                    pdf_path.name,
                    reason="failed_ingest",
                    details=str(exc),
                )
                self._delete_if_needed(context, pdf_path)
                continue

            item["points"] = points
            prepared.append(item)
        context.documents = prepared
        return context

    def upsert(self, context: PDFIngestContext) -> PDFIngestContext:
        connection = self._require_connection(context)
        qdrant_client = self._require_qdrant(context)

        for item in context.documents:
            pdf_path = self._pdf_path(item)
            metadata = item.get("metadata")
            if not isinstance(metadata, DocumentMetadata):
                continue
            points = item.get("points")
            if not isinstance(points, list) or not points:
                continue

            try:
                upsert_document_metadata(
                    connection=connection,
                    doc_id=str(item["doc_id"]),
                    pdf_path=pdf_path,
                    doc_version=context.doc_version,
                    file_hash=str(item["file_hash"]),
                    metadata=metadata,
                )
                upload_points_in_batches(
                    qdrant_client=qdrant_client,
                    collection_name=context.qdrant_collection,
                    points=points,
                    batch_size=context.batch_size,
                )
                connection.commit()
            except Exception as exc:
                context.failed_docs += 1
                LOGGER.error("Failed ingest for %s: %s", pdf_path.name, exc)
                self._append_skipped(
                    context,
                    pdf_path.name,
                    reason="failed_ingest",
                    details=str(exc),
                )
                connection.commit()
                self._delete_if_needed(context, pdf_path)
                continue

            context.total_docs += 1
            context.total_chunks += len(item.get("chunk_records", []))
            context.uploaded_points += len(points)
            context.accepted.append(pdf_path.name)
            LOGGER.info(
                "Uploaded %s chunks for %s (doc_id=%s, meta_source=%s)",
                len(points),
                pdf_path.name,
                item["doc_id"],
                metadata.meta_source,
            )
        return context

    def finalize(self, context: PDFIngestContext) -> PDFIngestContext:
        LOGGER.info(
            "Ingest complete. docs=%s chunks=%s uploaded=%s failed_docs=%s skipped=%s",
            context.total_docs,
            context.total_chunks,
            context.uploaded_points,
            context.failed_docs,
            len(context.skipped_documents),
        )
        if context.fail_on_no_upload and context.uploaded_points == 0:
            raise RuntimeError(
                "No chunks were uploaded to Qdrant. "
                "Check OpenAI connectivity/model settings and Qdrant collection config."
            )
        return context

    def close(self, context: PDFIngestContext) -> None:
        connection = context.db_connection
        if isinstance(connection, sqlite3.Connection):
            connection.close()
        context.db_connection = None

    def _append_skipped(
        self,
        context: PDFIngestContext,
        file_name: str,
        *,
        reason: str,
        details: str | None = None,
    ) -> None:
        context.skipped_documents.append(
            {
                "file_name": file_name,
                "reason": reason,
                "details": details,
            }
        )

    def _delete_if_needed(self, context: PDFIngestContext, pdf_path: Path) -> None:
        if context.delete_skipped_files:
            pdf_path.unlink(missing_ok=True)

    def _require_openai(self, context: PDFIngestContext) -> OpenAI:
        if isinstance(context.openai_client, OpenAI):
            return context.openai_client
        raise ValueError("OpenAI client is not initialized.")

    def _require_qdrant(self, context: PDFIngestContext) -> QdrantClient:
        if isinstance(context.qdrant_client, QdrantClient):
            return context.qdrant_client
        raise ValueError("Qdrant client is not initialized.")

    def _require_connection(self, context: PDFIngestContext) -> sqlite3.Connection:
        connection = context.db_connection
        if isinstance(connection, sqlite3.Connection):
            return connection
        raise ValueError("Metadata DB connection is not initialized.")

    def _pdf_path(self, item: dict[str, Any]) -> Path:
        pdf_path = item.get("pdf_path")
        if isinstance(pdf_path, Path):
            return pdf_path
        raise ValueError("Invalid document payload: pdf_path is required.")
