from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from app.core.errors import ErrorCode
from app.core.logging import log_event
from app.dependencies import build_upload_service
from app.web_api.mappers import to_upload_equities_response, to_upload_pdf_response
from app.web_api.schemas import UploadEquitiesResponse, UploadPDFResponse


@dataclass(frozen=True)
class UploadedBinaryPayload:
    file_name: str
    content: bytes


def build_upload_router(*, multipart_supported: bool) -> APIRouter:
    router = APIRouter()

    def _ensure_upload_service(request: Request) -> Any:
        upload_service = getattr(request.app.state, "upload_service", None)
        if upload_service is not None:
            return upload_service
        db_path = getattr(request.app.state, "db_path", None)
        upload_service = build_upload_service(db_path=db_path)
        request.app.state.upload_service = upload_service
        return upload_service

    if multipart_supported:
        @router.post("/upload/pdfs", response_model=UploadPDFResponse, response_model_exclude_none=True)
        def upload_pdfs(
            request: Request,
            files: list[UploadFile] = File(..., description="Up to 20 PDF files."),
        ) -> UploadPDFResponse:
            started = time.perf_counter()
            if not files:
                raise HTTPException(status_code=400, detail="At least one PDF file is required.")
            payloads: list[UploadedBinaryPayload] = []
            for item in files:
                payloads.append(
                    UploadedBinaryPayload(
                        file_name=item.filename or "upload.pdf",
                        content=item.file.read(),
                    )
                )

            try:
                summary = _ensure_upload_service(request).upload_pdfs(payloads)
            except ValueError as exc:
                log_event(
                    request.app.state.logger,
                    request_id=getattr(request.state, "request_id", None),
                    component="api",
                    operation="upload_pdfs",
                    status="warning",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    error_code="UPLOAD_VALIDATION_ERROR",
                )
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                request.app.state.logger.exception("upload_pdfs failed error=%s", exc)
                log_event(
                    request.app.state.logger,
                    request_id=getattr(request.state, "request_id", None),
                    component="api",
                    operation="upload_pdfs",
                    status="error",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    error_code=ErrorCode.API_RUNTIME_ERROR.value,
                )
                raise HTTPException(status_code=500, detail="Failed to upload PDF documents.") from exc

            log_event(
                request.app.state.logger,
                request_id=getattr(request.state, "request_id", None),
                component="api",
                operation="upload_pdfs",
                status="ok",
                duration_ms=int((time.perf_counter() - started) * 1000),
                accepted=len(summary.accepted),
                skipped=len(summary.skipped_documents),
            )
            return to_upload_pdf_response(summary)

        @router.post("/upload/equities", response_model=UploadEquitiesResponse, response_model_exclude_none=True)
        def upload_equities(
            request: Request,
            file: UploadFile = File(..., description="Single XLSX file."),
        ) -> UploadEquitiesResponse:
            started = time.perf_counter()
            payload = UploadedBinaryPayload(
                file_name=file.filename or "upload.xlsx",
                content=file.file.read(),
            )

            try:
                summary = _ensure_upload_service(request).upload_equities(payload)
            except ValueError as exc:
                log_event(
                    request.app.state.logger,
                    request_id=getattr(request.state, "request_id", None),
                    component="api",
                    operation="upload_equities",
                    status="warning",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    error_code="UPLOAD_VALIDATION_ERROR",
                )
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                request.app.state.logger.exception(
                    "upload_equities failed file=%s error=%s",
                    payload.file_name,
                    exc,
                )
                log_event(
                    request.app.state.logger,
                    request_id=getattr(request.state, "request_id", None),
                    component="api",
                    operation="upload_equities",
                    status="error",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    error_code=ErrorCode.API_RUNTIME_ERROR.value,
                )
                raise HTTPException(status_code=500, detail="Failed to upload equities file.") from exc

            log_event(
                request.app.state.logger,
                request_id=getattr(request.state, "request_id", None),
                component="api",
                operation="upload_equities",
                status="ok",
                duration_ms=int((time.perf_counter() - started) * 1000),
                file=summary.file_name,
                added=summary.added_count,
                updated=summary.updated_count,
                skipped=summary.skipped_count,
            )
            return to_upload_equities_response(summary)
    else:
        @router.post("/upload/pdfs", response_model=UploadPDFResponse, response_model_exclude_none=True)
        def upload_pdfs_unavailable() -> UploadPDFResponse:
            raise HTTPException(
                status_code=503,
                detail="Upload endpoint requires python-multipart. Install dependency and restart API.",
            )

        @router.post("/upload/equities", response_model=UploadEquitiesResponse, response_model_exclude_none=True)
        def upload_equities_unavailable() -> UploadEquitiesResponse:
            raise HTTPException(
                status_code=503,
                detail="Upload endpoint requires python-multipart. Install dependency and restart API.",
            )

    return router
