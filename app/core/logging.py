from __future__ import annotations

import logging
from typing import Any


def configure_logging(level: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return logging.getLogger("api")


def log_event(
    logger: logging.Logger,
    *,
    request_id: str | None,
    component: str,
    operation: str,
    status: str,
    duration_ms: int | None = None,
    error_code: str | None = None,
    **fields: Any,
) -> None:
    payload: dict[str, Any] = {
        "request_id": request_id or "-",
        "component": component,
        "operation": operation,
        "status": status,
        "duration_ms": duration_ms if duration_ms is not None else 0,
    }
    if error_code:
        payload["error_code"] = error_code
    for key, value in fields.items():
        if value is not None:
            payload[key] = value

    log_line = " ".join(f"{key}={value}" for key, value in payload.items())
    if status == "error":
        logger.error(log_line)
    elif status == "warning":
        logger.warning(log_line)
    else:
        logger.info(log_line)

