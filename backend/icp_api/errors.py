"""Consistent error envelope: ``{"error": {"code", "message", "request_id", "details"}}``.

Internal exception text is never returned to clients; it is logged with the request ID.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from icp_api.logging_setup import request_id_var

logger = logging.getLogger("icp_api.errors")


class ApiError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}
        self.headers = headers


class BodyTooLarge(Exception):
    """Raised while streaming a request body that exceeds MAX_REQUEST_BYTES."""


def error_body(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id_var.get(),
            "details": details or {},
        }
    }


def error_response(
    status: int,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(error_body(code, message, details), status_code=status, headers=headers)


_HTTP_CODES = {
    400: "BAD_REQUEST",
    401: "AUTH_REQUIRED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    409: "CONFLICT",
    413: "PAYLOAD_TOO_LARGE",
    415: "UNSUPPORTED_MEDIA_TYPE",
    429: "RATE_LIMITED",
    503: "SERVICE_UNAVAILABLE",
}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return error_response(exc.status, exc.code, exc.message, exc.details, exc.headers)

    @app.exception_handler(BodyTooLarge)
    async def _too_large(_: Request, exc: BodyTooLarge) -> JSONResponse:
        return error_response(413, "PAYLOAD_TOO_LARGE", "Request body too large.")

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        issues = [
            {
                "loc": [str(p) for p in e.get("loc", ())],
                "msg": str(e.get("msg", "")),
                "type": str(e.get("type", "")),
            }
            for e in exc.errors()
        ]
        return error_response(422, "VALIDATION_ERROR", "Request validation failed.", {"issues": issues})

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _HTTP_CODES.get(exc.status_code, "HTTP_ERROR")
        message = exc.detail if isinstance(exc.detail, str) else code.replace("_", " ").title()
        return error_response(exc.status_code, code, message)

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error: %s", type(exc).__name__)
        return error_response(
            500,
            "INTERNAL_ERROR",
            "An unexpected error occurred. Quote the request ID when reporting this problem.",
        )
