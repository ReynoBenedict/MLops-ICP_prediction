"""Background job control (admin only) and the audit log."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ValidationError

from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, audit, container
from icp_api.schemas import (
    AuditList,
    EvaluateAlertsParams,
    IngestBenchmarksParams,
    IngestFxParams,
    IngestIcpParams,
    Job,
    JobCreateRequest,
    JobList,
    RefreshForecastParams,
    SelftestParams,
    TrainParams,
)
from icp_api.security import Principal, require_admin
from icp_api.services.jobs import JobManager
from icp_api.storage.repository import InvalidCursor

router = APIRouter(prefix="/api/v1", responses=ERRORS)
PARAMS: dict[str, type[BaseModel]] = {
    "train": TrainParams,
    "ingest_benchmarks": IngestBenchmarksParams,
    "ingest_fx": IngestFxParams,
    "ingest_icp": IngestIcpParams,
    "refresh_forecast": RefreshForecastParams,
    "evaluate_alerts": EvaluateAlertsParams,
    "selftest": SelftestParams,
}


def manager(request: Request) -> JobManager:
    jobs: JobManager | None = getattr(request.app.state, "jobs", None)
    if jobs is None:
        raise ApiError(503, "JOBS_DISABLED", "Background jobs are disabled in this process.")
    return jobs


@router.get("/jobs", response_model=JobList, tags=["jobs"])
def list_jobs(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"] | None = Query(None),
    _: Principal = Depends(require_admin),
) -> JobList:
    try:
        page = container(request).repo.list_jobs(limit, cursor, status)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return JobList.model_validate({"items": page.items, "next_cursor": page.next_cursor})


@router.get("/jobs/{job_id}", response_model=Job, tags=["jobs"])
def get_job(request: Request, job_id: str, _: Principal = Depends(require_admin)) -> Job:
    job = container(request).repo.get_job(job_id)
    if job is None:
        raise ApiError(404, "NOT_FOUND", "Job not found.")
    return Job.model_validate(job)


@router.post("/jobs", response_model=Job, status_code=202, tags=["jobs"])
def create_job(
    request: Request, body: JobCreateRequest, principal: Principal = Depends(require_admin)
) -> Job:
    try:
        params = PARAMS[body.job_type].model_validate(body.params).model_dump()
    except ValidationError as exc:
        raise ApiError(
            422,
            "VALIDATION_ERROR",
            "Invalid job parameters.",
            {"issues": [{"loc": list(map(str, e["loc"])), "msg": e["msg"]} for e in exc.errors()]},
        ) from exc
    try:
        job = manager(request).submit(
            body.job_type, params, principal.name, request_id_var.get(), idempotency_key=body.idempotency_key
        )
    except ApiError as exc:
        audit(request, principal, f"job.{body.job_type}.submit", "failure", details={"code": exc.code})
        raise
    audit(
        request,
        principal,
        f"job.{body.job_type}.submit",
        "success",
        target=job["id"],
        details={"params": params},
    )
    return Job.model_validate(job)


@router.post("/jobs/{job_id}/cancel", response_model=Job, tags=["jobs"])
def cancel_job(request: Request, job_id: str, principal: Principal = Depends(require_admin)) -> Job:
    job = manager(request).cancel(job_id)
    audit(request, principal, "job.cancel", "success", target=job_id)
    return Job.model_validate(job)


@router.post("/jobs/{job_id}/retry", response_model=Job, status_code=202, tags=["jobs"])
def retry_job(request: Request, job_id: str, principal: Principal = Depends(require_admin)) -> Job:
    job = manager(request).retry(job_id, principal.name, request_id_var.get())
    audit(request, principal, "job.retry", "success", target=job["id"], details={"retry_of": job_id})
    return Job.model_validate(job)


@router.get("/audit", response_model=AuditList, tags=["audit"])
def audit_log(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    action: str | None = Query(None, max_length=100),
    _: Principal = Depends(require_admin),
) -> AuditList:
    try:
        page = container(request).repo.list_audit(limit, cursor, action)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return AuditList.model_validate({"items": page.items, "next_cursor": page.next_cursor})
