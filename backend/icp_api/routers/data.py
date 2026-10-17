"""Data exploration, quality, provenance, source evidence, versioning and controlled imports."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from icp_api.errors import ApiError
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, audit, container
from icp_api.schemas import (
    PERIOD_PATTERN,
    ActiveDatasets,
    DataQualityResponse,
    DatasetProvenance,
    DatasetVersion,
    DatasetVersionList,
    DataSummary,
    EvidenceResponse,
    HistoryResponse,
    ImportResponse,
    IngestionRunList,
)
from icp_api.security import Principal, rate_limit, require_admin
from icp_api.storage.repository import InvalidCursor
from icp_ml.schema import DATASETS

router = APIRouter(
    prefix="/api/v1",
    responses=ERRORS,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)
MAX_IMPORT_BYTES = 1_000_000
DatasetQuery = Query("icp", description=f"One of: {', '.join(DATASETS)}")


def _dataset(name: str) -> str:
    if name not in DATASETS:
        raise ApiError(422, "VALIDATION_ERROR", f"Unknown dataset '{name}'. Known: {', '.join(DATASETS)}")
    return name


@router.get("/data/summary", response_model=DataSummary, tags=["data"])
def summary(request: Request) -> DataSummary:
    return DataSummary.model_validate(container(request).datasets.summary())


@router.get("/data/history", response_model=HistoryResponse, tags=["data"])
def history(
    request: Request,
    start: str | None = Query(None, pattern=PERIOD_PATTERN),
    end: str | None = Query(None, pattern=PERIOD_PATTERN),
) -> HistoryResponse:
    return HistoryResponse(points=container(request).datasets.history(start, end))  # type: ignore[arg-type]


@router.get("/data/quality", response_model=DataQualityResponse, tags=["data"])
def quality(request: Request, dataset: str = DatasetQuery) -> DataQualityResponse:
    return DataQualityResponse.model_validate(container(request).datasets.quality(_dataset(dataset)))


@router.get("/data/evidence", response_model=EvidenceResponse, tags=["data"])
def evidence(
    request: Request, dataset: str = DatasetQuery, period: str | None = Query(None, pattern=PERIOD_PATTERN)
) -> EvidenceResponse:
    items = container(request).datasets.evidence(_dataset(dataset), period)
    return EvidenceResponse.model_validate({"dataset": dataset, "period": period, "items": items})


@router.get("/datasets/active", response_model=ActiveDatasets, tags=["datasets"])
def active(request: Request) -> ActiveDatasets:
    prov = container(request).datasets.all_provenance()
    return ActiveDatasets(datasets={k: DatasetProvenance.model_validate(v) for k, v in prov.items()})


@router.get("/datasets/versions", response_model=DatasetVersionList, tags=["datasets"])
def list_versions(
    request: Request,
    dataset: str | None = Query(None),
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
) -> DatasetVersionList:
    try:
        page = container(request).datasets.list_versions(
            _dataset(dataset) if dataset else None, limit, cursor
        )
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return DatasetVersionList.model_validate({"items": page.items, "next_cursor": page.next_cursor})


@router.get("/datasets/ingestions", response_model=IngestionRunList, tags=["datasets"])
def list_ingestions(
    request: Request,
    dataset: str | None = Query(None),
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
) -> IngestionRunList:
    try:
        page = container(request).repo.list_ingestion_runs(
            _dataset(dataset) if dataset else None, limit, cursor
        )
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc
    return IngestionRunList.model_validate({"items": page.items, "next_cursor": page.next_cursor})


@router.get("/datasets/versions/{version_id}", response_model=DatasetVersion, tags=["datasets"])
def get_version(request: Request, version_id: str) -> DatasetVersion:
    return DatasetVersion.model_validate(container(request).datasets.get_version(version_id))


@router.get(
    "/datasets/versions/{version_id}/download",
    tags=["datasets"],
    responses={200: {"content": {"text/csv": {}}}},
)
def download(request: Request, version_id: str) -> Response:
    row, data = container(request).datasets.download(version_id)
    filename = f"{row['dataset']}_v{row['version']}_{row['sha256'][:12]}.csv"
    return Response(
        data,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "private, max-age=86400, immutable",
        },
    )


@router.post(
    "/datasets/{dataset}/imports",
    response_model=ImportResponse,
    tags=["datasets"],
    openapi_extra={
        "requestBody": {"required": True, "content": {"text/csv": {"schema": {"type": "string"}}}}
    },
)
async def import_dataset(
    request: Request,
    dataset: str,
    mode: Literal["merge", "replace"] = Query(
        "merge",
        description="merge: add/update months; replace: full replacement (removals need confirmation)",
    ),
    commit: bool = Query(False, description="false = dry-run preview with validation report and diff"),
    confirm_overwrite: bool = Query(
        False, description="required when existing observations change or disappear"
    ),
    principal: Principal = Depends(require_admin),
) -> ImportResponse:
    _dataset(dataset)
    ctype = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if ctype not in ("text/csv", "text/plain", "application/csv"):
        raise ApiError(
            415, "UNSUPPORTED_MEDIA_TYPE", "Send the CSV as the request body with Content-Type text/csv."
        )
    body = await request.body()
    if not body:
        raise ApiError(422, "VALIDATION_ERROR", "Empty CSV body.")
    if len(body) > MAX_IMPORT_BYTES:
        raise ApiError(413, "PAYLOAD_TOO_LARGE", f"CSV imports are limited to {MAX_IMPORT_BYTES} bytes.")
    c = container(request)
    try:
        result = c.datasets.import_csv(dataset, body, mode, commit, confirm_overwrite, actor=principal.name)
    except ApiError as exc:
        if commit:
            audit(request, principal, f"dataset.{dataset}.import", "failure", details={"code": exc.code})
        raise
    if commit:
        audit(
            request,
            principal,
            f"dataset.{dataset}.import",
            "success" if result["committed"] else "failure",
            target=(result.get("version") or {}).get("id"),
            details={
                "mode": mode,
                "confirm_overwrite": confirm_overwrite,
                "added": len((result.get("diff") or {}).get("added", [])),
                "changed": len((result.get("diff") or {}).get("changed", [])),
                "removed": len((result.get("diff") or {}).get("removed", [])),
            },
        )
    return ImportResponse.model_validate(result)


@router.post("/datasets/versions/{version_id}/activate", response_model=DatasetVersion, tags=["datasets"])
def activate(
    request: Request, version_id: str, principal: Principal = Depends(require_admin)
) -> DatasetVersion:
    c = container(request)
    row = c.datasets.activate(version_id)
    audit(request, principal, f"dataset.{row['dataset']}.activate", "success", target=version_id)
    return DatasetVersion.model_validate(row)
