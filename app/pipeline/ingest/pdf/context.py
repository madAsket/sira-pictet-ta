from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class PDFIngestContext:
    input_paths: list[Path] = field(default_factory=list)
    metadata_db_path: Path | None = None

    qdrant_url: str = ""
    qdrant_collection: str = ""
    embedding_model: str = ""
    extractor_model: str = ""
    chunk_size_tokens: int = 900
    chunk_overlap_ratio: float = 0.15
    dedup_similarity: float = 0.95
    metadata_confidence_threshold: float = 0.70
    doc_version: str = "v1"
    batch_size: int = 64
    default_input_dir: Path = Path("data/PDF")
    topic_min_confidence: float = 0.60
    skip_duplicates_by_sha256: bool = False
    delete_skipped_files: bool = False
    fail_on_no_upload: bool = True

    topic_classifier: Any | None = None
    normalized_embedding_model: str = ""
    resolved_inputs: list[Path] = field(default_factory=list)
    documents: list[dict[str, Any]] = field(default_factory=list)

    openai_client: Any | None = None
    qdrant_client: Any | None = None
    tokenizer: Any | None = None
    db_connection: Any | None = None
    mention_catalog: Any | None = None

    accepted: list[str] = field(default_factory=list)
    skipped_documents: list[dict[str, Any]] = field(default_factory=list)
    failed_docs: int = 0
    uploaded_points: int = 0
    total_docs: int = 0
    total_chunks: int = 0
    debug: dict[str, Any] = field(default_factory=dict)
