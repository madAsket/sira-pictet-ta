from __future__ import annotations

import argparse
import logging
from pathlib import Path

from app.core.settings import get_settings
from app.pipeline.ingest.pdf.pipeline import ingest_pdfs
from app.pipeline.ingest.pdf.services.topic_classifier import PDFTopicClassifier

LOGGER = logging.getLogger("ingest_pdfs")


def parse_args() -> argparse.Namespace:
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Ingest PDF files into Qdrant with metadata extraction.")
    parser.add_argument(
        "--input",
        nargs="*",
        type=Path,
        default=None,
        help="PDF file(s) or directory(ies). If omitted, uses PDF_INPUT_DIR.",
    )
    parser.add_argument(
        "--metadata-db",
        type=Path,
        default=Path("db/equities.db"),
        help="SQLite DB path used for documents metadata and mention catalog.",
    )
    parser.add_argument(
        "--qdrant-url",
        type=str,
        default=settings.qdrant_url,
        help="Qdrant URL.",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=settings.qdrant_collection,
        help="Qdrant collection name.",
    )
    parser.add_argument(
        "--embedding-model",
        type=str,
        default=settings.openai_embedding_model,
        help="OpenAI embedding model.",
    )
    parser.add_argument(
        "--extractor-model",
        type=str,
        default=settings.openai_extractor_model,
        help="OpenAI model for metadata extraction.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=settings.pdf_chunk_size_tokens,
        help="Chunk size in tokens.",
    )
    parser.add_argument(
        "--chunk-overlap-ratio",
        type=float,
        default=settings.pdf_chunk_overlap_ratio,
        help="Chunk overlap ratio in [0,1).",
    )
    parser.add_argument(
        "--dedup-similarity",
        type=float,
        default=settings.pdf_dedup_similarity,
        help="Near-duplicate threshold in [0,1].",
    )
    parser.add_argument(
        "--metadata-confidence-threshold",
        type=float,
        default=settings.pdf_metadata_confidence_threshold,
        help="Fallback threshold for metadata extraction confidence.",
    )
    parser.add_argument(
        "--disable-topic-validation",
        action="store_true",
        help="Disable topic relevance validation before ingestion.",
    )
    parser.add_argument(
        "--topic-min-confidence",
        type=float,
        default=settings.pdf_topic_min_confidence,
        help="Minimum confidence required to skip as irrelevant when topic validation is enabled.",
    )
    parser.add_argument(
        "--skip-duplicates",
        action="store_true",
        help="Skip files already present in documents table by sha256.",
    )
    parser.add_argument(
        "--doc-version",
        type=str,
        default="v1",
        help="Version marker included in doc_id.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Upsert batch size for Qdrant.",
    )
    parser.add_argument(
        "--default-input-dir",
        type=Path,
        default=settings.pdf_input_dir,
        help="Default PDF directory used when --input is omitted.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    try:
        topic_classifier = None if args.disable_topic_validation else PDFTopicClassifier()
        report = ingest_pdfs(
            input_paths=args.input,
            metadata_db_path=args.metadata_db,
            qdrant_url=args.qdrant_url,
            qdrant_collection=args.collection,
            embedding_model=args.embedding_model,
            extractor_model=args.extractor_model,
            chunk_size_tokens=args.chunk_size,
            chunk_overlap_ratio=args.chunk_overlap_ratio,
            dedup_similarity=args.dedup_similarity,
            metadata_confidence_threshold=args.metadata_confidence_threshold,
            doc_version=args.doc_version,
            batch_size=args.batch_size,
            default_input_dir=args.default_input_dir,
            topic_classifier=topic_classifier,
            topic_min_confidence=args.topic_min_confidence,
            skip_duplicates_by_sha256=args.skip_duplicates,
            delete_skipped_files=False,
            fail_on_no_upload=True,
        )
        LOGGER.info(
            "Ingest report accepted=%s skipped=%s failed_docs=%s uploaded_points=%s",
            len(report.accepted),
            len(report.skipped_documents),
            report.failed_docs,
            report.uploaded_points,
        )
    except Exception as exc:
        LOGGER.error("%s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
