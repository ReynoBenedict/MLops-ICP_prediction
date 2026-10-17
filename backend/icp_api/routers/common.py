"""Shared router helpers."""

from __future__ import annotations

from typing import Any

from fastapi import Query, Request

from icp_api.container import Container
from icp_api.logging_setup import request_id_var
from icp_api.schemas import ErrorResponse
from icp_api.security import Principal

ERRORS: dict[int | str, dict[str, Any]] = {
    code: {"model": ErrorResponse} for code in (400, 401, 403, 404, 409, 413, 422, 429, 500, 503, 504)
}

LimitQuery = Query(20, ge=1, le=100, description="Page size")
CursorQuery = Query(None, max_length=512, description="Opaque cursor from a previous page")


def container(request: Request) -> Container:
    return request.app.state.container


def audit(
    request: Request,
    principal: Principal,
    action: str,
    outcome: str,
    target: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    container(request).repo.audit(
        principal.name, action, outcome, target=target, request_id=request_id_var.get(), details=details
    )
