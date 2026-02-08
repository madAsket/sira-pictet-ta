from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.core.errors import AppError
from app.pipeline.ask.models import IntentType, RAGBranchResult, SQLBranchResult


@dataclass
class AskPipelineContext:
    question: str
    intent: IntentType = "unknown"
    raw_intent: IntentType = "unknown"
    company_specific: bool = False
    intent_confidence: float = 0.0
    entities: list[dict[str, Any]] = field(default_factory=list)
    used_sql: bool = False
    used_rag: bool = False
    sql: str | None = None
    sql_rows_preview: list[dict[str, Any]] = field(default_factory=list)
    rag_context_snippets: list[dict[str, Any]] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    answer: str = ""
    errors: list[AppError] = field(default_factory=list)
    debug: dict[str, Any] = field(default_factory=dict)
    sql_result: SQLBranchResult = field(default_factory=lambda: SQLBranchResult(sql=None, rows_preview=[], success=False))
    rag_result: RAGBranchResult = field(
        default_factory=lambda: RAGBranchResult(sources=[], context_snippets=[], success=False)
    )
