from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_ENTITY_NOT_FOUND_TEMPLATE = (
    "I couldn't find any matching companies in the provided equities dataset for: {question}"
)

_ENV_LOADED = False


def ensure_env_loaded() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    load_dotenv()
    _ENV_LOADED = True


def _get_text(name: str, *, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip()
    return normalized or default


def _get_optional_text(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    normalized = raw.strip()
    return normalized or None


def _get_int(name: str, *, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def _get_float(
    name: str,
    *,
    default: float,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return default
    if maximum is not None and value > maximum:
        return maximum
    return value


def _get_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_reasoning_effort(name: str, *, default: str) -> str:
    value = _get_text(name, default=default).lower()
    allowed = {"minimal", "low", "medium", "high"}
    return value if value in allowed else default


def _get_log_level(name: str, *, default: str) -> str:
    value = _get_text(name, default=default).upper()
    allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
    return value if value in allowed else default


def _get_prompt_relative_path(name: str, *, default: str) -> Path:
    candidate = Path(_get_text(name, default=default))
    if candidate.is_absolute():
        return Path(default)
    if any(part == ".." for part in candidate.parts):
        return Path(default)
    return candidate


@dataclass(frozen=True)
class Settings:
    openai_api_key: str | None
    openai_copilot_model: str
    openai_extractor_model: str
    openai_txt2sql_model: str | None
    openai_embedding_model: str
    openai_txt2sql_max_output_tokens: int
    openai_txt2sql_reasoning_effort: str
    openai_final_max_output_tokens: int
    openai_final_max_answer_chars: int
    openai_final_reasoning_effort: str
    qdrant_url: str
    qdrant_collection: str
    pdf_chunk_size_tokens: int
    pdf_chunk_overlap_ratio: float
    pdf_dedup_similarity: float
    pdf_metadata_confidence_threshold: float
    pdf_metadata_max_output_tokens: int
    pdf_metadata_page_char_limit: int
    pdf_metadata_total_char_limit: int
    pdf_doc_version: str
    pdf_upload_batch_size: int
    pdf_input_dir: Path
    pdf_topic_max_output_tokens: int
    pdf_topic_reasoning_effort: str
    pdf_topic_min_confidence: float
    api_upload_pdf_max_files: int
    api_upload_pdf_max_file_bytes: int
    api_upload_equities_max_file_bytes: int
    rag_top_k: int
    rag_max_sources: int
    rag_dedup_similarity_threshold: float
    rag_min_score: float
    rag_context_max_snippets: int
    rag_context_max_chars: int
    sql_rows_preview_limit: int
    sql_max_limit: int
    entity_not_found_template: str
    non_company_intent_override_threshold: float
    composer_debug_flags_enabled: bool
    api_debug_response: bool
    api_log_level: str
    api_host: str
    api_port: int
    prompt_root_dir: Path
    prompt_router_intent_path: Path
    prompt_router_intent_user_path: Path
    prompt_sql_text_to_sql_path: Path
    prompt_sql_text_to_sql_user_path: Path
    prompt_final_composer_path: Path
    prompt_upload_topic_path: Path
    prompt_pdf_metadata_extraction_path: Path

    @property
    def txt2sql_model(self) -> str:
        return self.openai_txt2sql_model or self.openai_copilot_model

    @property
    def topic_classifier_model(self) -> str:
        return self.openai_extractor_model or self.openai_copilot_model

    def _resolve_prompt_path(self, relative_path: Path) -> Path:
        if relative_path.is_absolute():
            return relative_path
        return self.prompt_root_dir / relative_path

    @property
    def router_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_router_intent_path)

    @property
    def router_user_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_router_intent_user_path)

    @property
    def sql_text_to_sql_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_sql_text_to_sql_path)

    @property
    def sql_text_to_sql_user_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_sql_text_to_sql_user_path)

    @property
    def final_composer_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_final_composer_path)

    @property
    def upload_topic_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_upload_topic_path)

    @property
    def pdf_metadata_extraction_prompt_path(self) -> Path:
        return self._resolve_prompt_path(self.prompt_pdf_metadata_extraction_path)

    @classmethod
    def from_env(cls) -> Settings:
        ensure_env_loaded()
        return cls(
            openai_api_key=_get_optional_text("OPENAI_API_KEY"),
            openai_copilot_model=_get_text("OPENAI_COPILOT_MODEL", default="gpt-5-mini"),
            openai_extractor_model=_get_text("OPENAI_EXTRACTOR_MODEL", default="gpt-5-mini"),
            openai_txt2sql_model=_get_optional_text("OPENAI_TXT2SQL_MODEL"),
            openai_embedding_model=_get_text("OPENAI_EMBEDDING_MODEL", default="text-embedding-3-large"),
            openai_txt2sql_max_output_tokens=_get_int(
                "OPENAI_TXT2SQL_MAX_OUTPUT_TOKENS",
                default=900,
                minimum=1,
            ),
            openai_txt2sql_reasoning_effort=_get_reasoning_effort(
                "OPENAI_TXT2SQL_REASONING_EFFORT",
                default="minimal",
            ),
            openai_final_max_output_tokens=_get_int("OPENAI_FINAL_MAX_OUTPUT_TOKENS", default=4000, minimum=1),
            openai_final_max_answer_chars=_get_int("OPENAI_FINAL_MAX_ANSWER_CHARS", default=3000, minimum=1),
            openai_final_reasoning_effort=_get_reasoning_effort(
                "OPENAI_FINAL_REASONING_EFFORT",
                default="minimal",
            ),
            qdrant_url=_get_text("QDRANT_URL", default="http://localhost:6333"),
            qdrant_collection=_get_text("QDRANT_COLLECTION", default="pdf_chunks"),
            pdf_chunk_size_tokens=_get_int("PDF_CHUNK_SIZE_TOKENS", default=900, minimum=100),
            pdf_chunk_overlap_ratio=_get_float(
                "PDF_CHUNK_OVERLAP_RATIO",
                default=0.15,
                minimum=0.0,
                maximum=0.9999,
            ),
            pdf_dedup_similarity=_get_float(
                "PDF_DEDUP_SIMILARITY",
                default=0.95,
                minimum=0.0,
                maximum=1.0,
            ),
            pdf_metadata_confidence_threshold=_get_float(
                "PDF_METADATA_CONFIDENCE_THRESHOLD",
                default=0.70,
                minimum=0.0,
                maximum=1.0,
            ),
            pdf_metadata_max_output_tokens=_get_int("PDF_METADATA_MAX_OUTPUT_TOKENS", default=1200, minimum=1),
            pdf_metadata_page_char_limit=_get_int("PDF_METADATA_PAGE_CHAR_LIMIT", default=3000, minimum=200),
            pdf_metadata_total_char_limit=_get_int("PDF_METADATA_TOTAL_CHAR_LIMIT", default=8000, minimum=200),
            pdf_doc_version=_get_text("PDF_DOC_VERSION", default="v1"),
            pdf_upload_batch_size=_get_int("PDF_UPLOAD_BATCH_SIZE", default=64, minimum=1),
            pdf_input_dir=Path(_get_text("PDF_INPUT_DIR", default="data/PDF")),
            pdf_topic_max_output_tokens=_get_int("PDF_TOPIC_MAX_OUTPUT_TOKENS", default=300, minimum=1),
            pdf_topic_reasoning_effort=_get_reasoning_effort("PDF_TOPIC_REASONING_EFFORT", default="minimal"),
            pdf_topic_min_confidence=_get_float(
                "PDF_TOPIC_MIN_CONFIDENCE",
                default=0.60,
                minimum=0.0,
                maximum=1.0,
            ),
            api_upload_pdf_max_files=_get_int("API_UPLOAD_PDF_MAX_FILES", default=20, minimum=1),
            api_upload_pdf_max_file_bytes=_get_int(
                "API_UPLOAD_PDF_MAX_FILE_BYTES",
                default=10 * 1024 * 1024,
                minimum=1,
            ),
            api_upload_equities_max_file_bytes=_get_int(
                "API_UPLOAD_EQUITIES_MAX_FILE_BYTES",
                default=20 * 1024 * 1024,
                minimum=1,
            ),
            rag_top_k=_get_int("RAG_TOP_K", default=8, minimum=1),
            rag_max_sources=_get_int("RAG_MAX_SOURCES", default=3, minimum=1),
            rag_dedup_similarity_threshold=_get_float(
                "RAG_DEDUP_SIMILARITY_THRESHOLD",
                default=0.95,
                minimum=0.0,
            ),
            rag_min_score=_get_float("RAG_MIN_SCORE", default=0.25, minimum=0.0),
            rag_context_max_snippets=_get_int("RAG_CONTEXT_MAX_SNIPPETS", default=5, minimum=1),
            rag_context_max_chars=_get_int("RAG_CONTEXT_MAX_CHARS", default=4000, minimum=100),
            sql_rows_preview_limit=_get_int("SQL_ROWS_PREVIEW_LIMIT", default=5, minimum=1),
            sql_max_limit=_get_int("SQL_MAX_LIMIT", default=50, minimum=1),
            entity_not_found_template=_get_text(
                "ENTITY_NOT_FOUND_TEMPLATE",
                default=DEFAULT_ENTITY_NOT_FOUND_TEMPLATE,
            ),
            non_company_intent_override_threshold=_get_float(
                "NON_COMPANY_INTENT_OVERRIDE_THRESHOLD",
                default=0.70,
                minimum=0.0,
            ),
            composer_debug_flags_enabled=_get_bool("COMPOSER_DEBUG_FLAGS_ENABLED", default=True),
            api_debug_response=_get_bool("API_DEBUG_RESPONSE", default=False),
            api_log_level=_get_log_level("API_LOG_LEVEL", default="INFO"),
            api_host=_get_text("API_HOST", default="localhost"),
            api_port=_get_int("API_PORT", default=8020, minimum=1),
            prompt_root_dir=Path(_get_text("PROMPT_ROOT_DIR", default="app/resources/prompts")),
            prompt_router_intent_path=_get_prompt_relative_path(
                "PROMPT_ROUTER_INTENT_PATH",
                default="router/intent_classification.md",
            ),
            prompt_router_intent_user_path=_get_prompt_relative_path(
                "PROMPT_ROUTER_INTENT_USER_PATH",
                default="router/intent_classification_user.md",
            ),
            prompt_sql_text_to_sql_path=_get_prompt_relative_path(
                "PROMPT_SQL_TEXT_TO_SQL_PATH",
                default="sql/text_to_sql.md",
            ),
            prompt_sql_text_to_sql_user_path=_get_prompt_relative_path(
                "PROMPT_SQL_TEXT_TO_SQL_USER_PATH",
                default="sql/text_to_sql_user.md",
            ),
            prompt_final_composer_path=_get_prompt_relative_path(
                "PROMPT_FINAL_COMPOSER_PATH",
                default="final/response_composer.md",
            ),
            prompt_upload_topic_path=_get_prompt_relative_path(
                "PROMPT_UPLOAD_TOPIC_PATH",
                default="upload/pdf_topic_classification.md",
            ),
            prompt_pdf_metadata_extraction_path=_get_prompt_relative_path(
                "PROMPT_PDF_METADATA_EXTRACTION_PATH",
                default="pdf/metadata_extraction.md",
            ),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
