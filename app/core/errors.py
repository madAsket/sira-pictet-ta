from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    ENTITY_NOT_FOUND = "ENTITY_NOT_FOUND"
    REJECTED_CANDIDATES_DEBUG = "REJECTED_CANDIDATES_DEBUG"
    NON_COMPANY_UNKNOWN_DEFAULTED_TO_MACRO = "NON_COMPANY_UNKNOWN_DEFAULTED_TO_MACRO"
    NON_COMPANY_UNKNOWN_DEFAULTED_BY_HEURISTIC = "NON_COMPANY_UNKNOWN_DEFAULTED_BY_HEURISTIC"
    NON_COMPANY_LOW_CONFIDENCE_OVERRIDDEN_BY_HEURISTIC = "NON_COMPANY_LOW_CONFIDENCE_OVERRIDDEN_BY_HEURISTIC"
    ROUTER_FALLBACK_COMPANY_SPECIFIC_HEURISTIC = "ROUTER_FALLBACK_COMPANY_SPECIFIC_HEURISTIC"
    SQL_GENERATION_FAILED = "SQL_GENERATION_FAILED"
    SQL_GUARDRAIL_BLOCKED = "SQL_GUARDRAIL_BLOCKED"
    SQL_EXECUTION_FAILED = "SQL_EXECUTION_FAILED"
    RAG_RETRIEVAL_FAILED = "RAG_RETRIEVAL_FAILED"
    RAG_NO_RELEVANT_CHUNKS = "RAG_NO_RELEVANT_CHUNKS"
    FINAL_COMPOSER_FALLBACK = "FINAL_COMPOSER_FALLBACK"
    COMPOSER_DEBUG_FLAGS = "COMPOSER_DEBUG_FLAGS"
    API_RUNTIME_ERROR = "API_RUNTIME_ERROR"


@dataclass(frozen=True)
class AppError:
    code: ErrorCode
    message: str
    details: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": self.code.value,
            "message": self.message,
        }
        if self.details is not None:
            payload["details"] = self.details
        return payload


WarningItem = AppError


def to_error_dict(error: AppError) -> dict[str, Any]:
    return error.to_dict()

