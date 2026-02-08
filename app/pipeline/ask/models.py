from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

IntentType = Literal["equity_only", "macro_only", "hybrid", "unknown"]


@dataclass(frozen=True)
class PipelineResult:
    question: str
    intent: IntentType
    raw_intent: IntentType
    company_specific: bool
    intent_confidence: float
    entities: list[dict[str, Any]]
    used_sql: bool
    used_rag: bool
    sql: str | None
    sql_rows_preview: list[dict[str, Any]]
    answer: str
    sources: list[dict[str, Any]]
    errors: list[dict[str, Any]]


@dataclass(frozen=True)
class SQLBranchResult:
    sql: str | None
    rows_preview: list[dict[str, Any]]
    success: bool


@dataclass(frozen=True)
class RAGBranchResult:
    sources: list[dict[str, Any]]
    context_snippets: list[dict[str, Any]]
    success: bool


def intent_usage(intent: IntentType) -> tuple[bool, bool]:
    if intent == "equity_only":
        return True, False
    if intent == "macro_only":
        return False, True
    if intent == "hybrid":
        return True, True
    return True, True
