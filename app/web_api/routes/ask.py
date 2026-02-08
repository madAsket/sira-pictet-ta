from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Query, Request

from app.core.errors import AppError, ErrorCode, to_error_dict
from app.core.logging import log_event
from app.dependencies import build_question_pipeline
from app.web_api.mappers import (
    build_response,
    compact_sql,
    entity_log_value,
    source_log_value,
)
from app.web_api.schemas import AskRequest, AskResponse


def build_ask_router() -> APIRouter:
    router = APIRouter()

    def _ensure_pipeline(request: Request) -> Any:
        pipeline = getattr(request.app.state, "pipeline", None)
        if pipeline is not None:
            return pipeline
        db_path = getattr(request.app.state, "db_path", None)
        pipeline = build_question_pipeline(db_path=db_path)
        request.app.state.pipeline = pipeline
        return pipeline

    @router.post("/ask", response_model=AskResponse, response_model_exclude_none=True)
    def ask(
        payload: AskRequest,
        request: Request,
        debug: bool | None = Query(default=None, description="Return technical pipeline fields."),
    ) -> AskResponse:
        started = time.perf_counter()
        include_debug = request.app.state.debug_response_default if debug is None else debug
        try:
            result = _ensure_pipeline(request).process(question=payload.question)
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            request.app.state.logger.exception("ask failed question=%s error=%s", payload.question, exc)
            log_event(
                request.app.state.logger,
                request_id=getattr(request.state, "request_id", None),
                component="api",
                operation="ask",
                status="error",
                duration_ms=duration_ms,
                error_code=ErrorCode.API_RUNTIME_ERROR.value,
            )
            fallback = {
                "question": payload.question,
                "answer": "I'm unable to process this request right now. Please try again.",
                "sources": [],
                "entities": [],
                "used_sql": False,
                "used_rag": False,
                "sql": None,
                "sql_rows_preview": [],
                "errors": [
                    to_error_dict(
                        AppError(
                            code=ErrorCode.API_RUNTIME_ERROR,
                            message="Unexpected API failure.",
                        )
                    )
                ],
            }
            return build_response(fallback, include_debug=include_debug)

        duration_ms = int((time.perf_counter() - started) * 1000)
        log_event(
            request.app.state.logger,
            request_id=getattr(request.state, "request_id", None),
            component="api",
            operation="ask",
            status="ok",
            duration_ms=duration_ms,
            intent=result.intent,
            entities=entity_log_value(result.entities),
            used_sql=result.used_sql,
            used_rag=result.used_rag,
            sql=compact_sql(result.sql),
            source_ids=source_log_value(result.sources),
        )
        return build_response(result, include_debug=include_debug)

    return router
