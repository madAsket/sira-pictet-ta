from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, Response

from app.core.errors import ErrorCode
from app.core.logging import configure_logging, log_event
from app.core.settings import get_settings
from app.web_api.routes.ask import build_ask_router
from app.web_api.routes.upload import build_upload_router


def _has_multipart_support() -> bool:
    try:
        import python_multipart  # type: ignore # noqa: F401
        return True
    except Exception:
        try:
            from multipart.multipart import parse_options_header  # type: ignore # noqa: F401
            return True
        except Exception:
            return False


def create_app(
    *,
    db_path: Path = Path("db/equities.db"),
    pipeline: Any | None = None,
    upload_service: Any | None = None,
    debug_response_default: bool | None = None,
) -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Stock Investment Research Assistant API", version="0.1.0")
    app.state.settings = settings
    app.state.db_path = db_path
    app.state.pipeline = pipeline
    app.state.upload_service = upload_service
    app.state.debug_response_default = (
        settings.api_debug_response
        if debug_response_default is None
        else debug_response_default
    )
    app.state.logger = configure_logging(settings.api_log_level)
    app.state.multipart_supported = _has_multipart_support()

    @app.middleware("http")
    async def request_context_middleware(request: Request, call_next) -> Response:
        request_id = (request.headers.get("X-Request-ID") or "").strip() or uuid.uuid4().hex
        request.state.request_id = request_id
        started = time.perf_counter()
        status = "ok"
        error_code: str | None = None
        try:
            response = await call_next(request)
            if response.status_code >= 500:
                status = "error"
            elif response.status_code >= 400:
                status = "warning"
            response.headers["X-Request-ID"] = request_id
            return response
        except Exception:
            status = "error"
            error_code = ErrorCode.API_RUNTIME_ERROR.value
            raise
        finally:
            duration_ms = int((time.perf_counter() - started) * 1000)
            log_event(
                app.state.logger,
                request_id=request_id,
                component="http",
                operation=f"{request.method} {request.url.path}",
                status=status,
                duration_ms=duration_ms,
                error_code=error_code,
            )

    app.include_router(build_ask_router())
    if not app.state.multipart_supported:
        app.state.logger.warning(
            "python-multipart is not installed. Upload endpoints are disabled until dependency is installed."
        )
    app.include_router(build_upload_router(multipart_supported=app.state.multipart_supported))
    return app


app = create_app()
