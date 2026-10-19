"""Model registry views, experiment history, comparison, promotion, rollback and drift."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request

from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, audit, container
from icp_api.schemas import (
    DriftResponse,
    ExperimentRunList,
    ModelList,
    ModelVersionDetail,
    PromoteRequest,
    PromotionList,
    PromotionResult,
    RollbackRequest,
    ValidationReport,
)
from icp_api.security import Principal, rate_limit, require_admin, require_user
from icp_api.storage.repository import InvalidCursor

router = APIRouter(
    prefix="/api/v1/models",
    tags=["models"],
    responses=ERRORS,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
SIGNED_IN = Depends(require_user("viewer"))  # customer-facing model card
OPERATOR_ONLY = Depends(require_admin)  # experiment, promotion and drift internals
VERSION_PATTERN = r"^[0-9]{1,6}$"


@router.get("", response_model=ModelList, dependencies=[SIGNED_IN])
def list_models(request: Request) -> ModelList:
    m = container(request).models
    versions = m.list_versions()
    production = next((v["version"] for v in versions if m.alias in v["aliases"]), None)
    return ModelList.model_validate(
        {"model_name": m.model_name, "alias": m.alias, "production_version": production, "versions": versions}
    )


@router.get("/versions/{version}", response_model=ModelVersionDetail, dependencies=[SIGNED_IN])
def version_detail(request: Request, version: str) -> ModelVersionDetail:
    if not version.isdigit():
        raise ApiError(422, "VALIDATION_ERROR", "version must be numeric")
    return ModelVersionDetail.model_validate(container(request).models.version_detail(version))


@router.get("/compare", response_model=list[ModelVersionDetail], dependencies=[OPERATOR_ONLY])
def compare(
    request: Request,
    versions: str = Query(
        ..., pattern=r"^[0-9]{1,6}(,[0-9]{1,6}){1,4}$", description="2-5 comma-separated versions"
    ),
) -> list[ModelVersionDetail]:
    m = container(request).models
    unique = list(dict.fromkeys(versions.split(",")))
    return [ModelVersionDetail.model_validate(m.version_detail(v)) for v in unique]


@router.get("/experiments/runs", response_model=ExperimentRunList, dependencies=[OPERATOR_ONLY])
def experiment_runs(
    request: Request, limit: int = LimitQuery, cursor: str | None = CursorQuery
) -> ExperimentRunList:
    runs, token = container(request).models.experiment_runs(limit, cursor)
    return ExperimentRunList.model_validate({"items": runs, "next_cursor": token})


@router.get("/promotions", response_model=PromotionList, dependencies=[OPERATOR_ONLY])
def promotions(request: Request, limit: int = LimitQuery, cursor: str | None = CursorQuery) -> PromotionList:
    try:
        page = container(request).models.promotions(limit, cursor)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return PromotionList.model_validate({"items": page.items, "next_cursor": page.next_cursor})


@router.get("/drift", response_model=DriftResponse, dependencies=[OPERATOR_ONLY])
def drift(request: Request, recent_months: int = Query(12, ge=3, le=36)) -> DriftResponse:
    return DriftResponse.model_validate(container(request).models.drift(recent_months))


@router.post("/versions/{version}/validate", response_model=ValidationReport)
def validate(
    request: Request, version: str, principal: Principal = Depends(require_admin)
) -> ValidationReport:
    if not version.isdigit():
        raise ApiError(422, "VALIDATION_ERROR", "version must be numeric")
    return ValidationReport.model_validate(container(request).models.validate(version))


@router.post("/versions/{version}/promote", response_model=PromotionResult)
def promote(
    request: Request, version: str, body: PromoteRequest, principal: Principal = Depends(require_admin)
) -> PromotionResult:
    if not version.isdigit():
        raise ApiError(422, "VALIDATION_ERROR", "version must be numeric")
    try:
        result = container(request).models.promote(
            version, body.reason, body.force, principal.name, request_id_var.get()
        )
    except ApiError as exc:
        audit(request, principal, "model.promote", "failure", target=version, details={"code": exc.code})
        raise
    audit(
        request,
        principal,
        "model.promote",
        "success",
        target=version,
        details={"from_version": result["from_version"], "forced": result["forced"], "reason": body.reason},
    )
    return PromotionResult.model_validate(result)


@router.post("/rollback", response_model=PromotionResult)
def rollback(
    request: Request, body: RollbackRequest, principal: Principal = Depends(require_admin)
) -> PromotionResult:
    try:
        result = container(request).models.rollback(body.reason, principal.name, request_id_var.get())
    except ApiError as exc:
        audit(request, principal, "model.rollback", "failure", details={"code": exc.code})
        raise
    audit(
        request,
        principal,
        "model.rollback",
        "success",
        target=result["to_version"],
        details={"from_version": result["from_version"], "reason": body.reason},
    )
    return PromotionResult.model_validate(result)
