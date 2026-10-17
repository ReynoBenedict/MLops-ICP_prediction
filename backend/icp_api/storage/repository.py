"""Application metadata repository (datasets, jobs, forecasts, promotions, audit).

Business logic never issues SQL directly; it calls this repository, which runs on either SQL
executor. Every write is one atomic statement so behaviour is identical on SQLite and D1.
"""

from __future__ import annotations

import base64
import json
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from icp_api.storage.sql import IntegrityError, SqlExecutor, StorageError

ACTIVE_JOB_STATUSES = ("queued", "running")
TERMINAL_JOB_STATUSES = ("succeeded", "failed", "cancelled")


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_before(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%f")[
        :-3
    ] + "Z"


def new_id(prefix: str) -> str:
    """Time-ordered, collision-resistant identifier, e.g. ``fc_0192f3a1b2c3d4e5f6a7b8c9``."""
    return f"{prefix}_{int(time.time() * 1000):012x}{secrets.token_hex(6)}"


class DuplicateActiveJob(StorageError):
    pass


class InvalidCursor(ValueError):
    pass


@dataclass
class Page:
    items: list[dict[str, Any]]
    next_cursor: str | None


def _encode_cursor(row: dict[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps([row["created_at"], row["id"]]).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[str, str]:
    try:
        created_at, row_id = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        if not isinstance(created_at, str) or not isinstance(row_id, str):
            raise ValueError
        return created_at, row_id
    except (ValueError, TypeError) as exc:
        raise InvalidCursor("Invalid pagination cursor") from exc


def _json(value: Any) -> str | None:
    return None if value is None else json.dumps(value, default=str, separators=(",", ":"))


def _decode(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out = dict(row)
    for key in list(out):
        if key.endswith("_json"):
            raw = out.pop(key)
            out[key[:-5]] = json.loads(raw) if raw else None
    return out


class MetadataRepository:
    def __init__(self, db: SqlExecutor):
        self.db = db

    # -- generic helpers ---------------------------------------------------------------------
    def _one(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        rows = self.db.query(sql, params)
        return _decode(rows[0]) if rows else None

    def _page(self, table: str, where: list[str], params: list[Any], limit: int, cursor: str | None) -> Page:
        if cursor:
            created_at, row_id = _decode_cursor(cursor)
            where = [*where, "(created_at < ? OR (created_at = ? AND id < ?))"]
            params = [*params, created_at, created_at, row_id]
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        select = (
            "SELECT *, started_at AS created_at FROM ingestion_runs"
            if table == "ingestion_runs"
            else f"SELECT * FROM {table}"  # noqa: S608 - table is an internal constant
        )
        # Table names and WHERE fragments are internal constants; every user value is a bound parameter.
        sql = f"{select} {clause} ORDER BY created_at DESC, id DESC LIMIT ?"
        rows = self.db.query(sql, (*params, limit + 1))
        items = [_decode(r) for r in rows[:limit]]
        next_cursor = _encode_cursor(rows[limit - 1]) if len(rows) > limit else None
        return Page([i for i in items if i is not None], next_cursor)

    # -- dataset versions --------------------------------------------------------------------
    def insert_dataset_version(self, row: dict[str, Any]) -> dict[str, Any]:
        self.db.execute(
            "INSERT INTO dataset_versions (id, dataset, version, object_key, sha256, size_bytes, row_count, "
            "period_start, period_end, source, source_detail, quality_json, diff_json, parent_version_id, "
            "is_active, created_at, created_by, provenance_json) "
            "VALUES (?, ?, (SELECT COALESCE(MAX(version), 0) + 1 FROM dataset_versions WHERE dataset = ?), "
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)",
            (
                row["id"],
                row["dataset"],
                row["dataset"],
                row["object_key"],
                row["sha256"],
                row["size_bytes"],
                row["row_count"],
                row.get("period_start"),
                row.get("period_end"),
                row["source"],
                row.get("source_detail"),
                _json(row["quality"]),
                _json(row.get("diff")),
                row.get("parent_version_id"),
                row["created_at"],
                row["created_by"],
                _json(row.get("provenance")),
            ),
        )
        created = self.get_dataset_version(row["id"])
        if created is None:
            raise StorageError("dataset version insert not visible")
        return created

    def get_dataset_version(self, version_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM dataset_versions WHERE id = ?", (version_id,))

    def find_dataset_by_sha(self, dataset: str, sha256: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM dataset_versions WHERE dataset = ? AND sha256 = ?", (dataset, sha256))

    def get_active_dataset(self, dataset: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM dataset_versions WHERE dataset = ? AND is_active = 1", (dataset,))

    def activate_dataset_version(self, dataset: str, version_id: str) -> None:
        changed = self.db.execute(
            "UPDATE dataset_versions SET is_active = CASE WHEN id = ? THEN 1 ELSE 0 END "
            "WHERE dataset = ? AND EXISTS (SELECT 1 FROM dataset_versions WHERE id = ? AND dataset = ?)",
            (version_id, dataset, version_id, dataset),
        )
        if changed == 0:
            raise StorageError(f"dataset version {version_id} not found for {dataset}")

    def list_dataset_versions(self, dataset: str | None, limit: int, cursor: str | None) -> Page:
        where, params = ([], []) if dataset is None else (["dataset = ?"], [dataset])
        return self._page("dataset_versions", where, params, limit, cursor)

    def list_active_datasets(self) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM dataset_versions WHERE is_active = 1 ORDER BY dataset")
        return [d for d in (_decode(r) for r in rows) if d is not None]

    # -- provenance ---------------------------------------------------------------------------
    def upsert_source_document(self, doc: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO source_documents (sha256, url, provider, content_type, size_bytes, retrieved_at, object_key, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(sha256) DO UPDATE SET retrieved_at = excluded.retrieved_at, "
            "object_key = COALESCE(excluded.object_key, source_documents.object_key)",
            (
                doc["sha256"],
                doc["url"],
                doc.get("provider", ""),
                doc.get("content_type"),
                doc.get("bytes", 0),
                doc["retrieved_at"],
                doc.get("object_key"),
                doc.get("note"),
            ),
        )

    def get_source_document(self, sha256: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM source_documents WHERE sha256 = ?", (sha256,))

    def insert_evidence(self, rows: list[dict[str, Any]]) -> int:
        n = 0
        for e in rows:
            self.db.execute(
                "INSERT INTO source_evidence (id, dataset, period, value, kind, verification_status, source_url, "
                "document_sha256, reference, retrieved_at, note, dataset_version_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    new_id("ev"),
                    e["dataset"],
                    e["period"],
                    e.get("value"),
                    e["kind"],
                    e["verification_status"],
                    e["source_url"],
                    e.get("document_sha256"),
                    e.get("reference"),
                    e.get("retrieved_at") or utcnow(),
                    e.get("note"),
                    e.get("dataset_version_id"),
                    utcnow(),
                ),
            )
            n += 1
        return n

    def list_evidence(
        self, dataset: str, period: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        if period:
            rows = self.db.query(
                "SELECT * FROM source_evidence WHERE dataset = ? AND period = ? ORDER BY created_at DESC LIMIT ?",
                (dataset, period, limit),
            )
        else:
            rows = self.db.query(
                "SELECT * FROM source_evidence WHERE dataset = ? ORDER BY period DESC, created_at DESC LIMIT ?",
                (dataset, limit),
            )
        return [d for d in (_decode(r) for r in rows) if d is not None]

    def evidence_status_by_period(self, dataset: str) -> dict[str, dict[str, Any]]:
        """Latest evidence row per period (status + kind), for provenance views."""
        rows = self.db.query(
            "SELECT e.period, e.verification_status, e.kind, e.value, e.source_url, e.reference FROM source_evidence e "
            "JOIN (SELECT period, MAX(created_at) AS mx FROM source_evidence WHERE dataset = ? GROUP BY period) m "
            "ON m.period = e.period AND m.mx = e.created_at WHERE e.dataset = ? ORDER BY e.period",
            (dataset, dataset),
        )
        return {r["period"]: dict(r) for r in rows}

    def insert_ingestion_run(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO ingestion_runs (id, dataset, connector, status, started_at, finished_at, job_id, requested_by, summary_json, version_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["dataset"],
                row["connector"],
                row["status"],
                row["started_at"],
                row["finished_at"],
                row.get("job_id"),
                row["requested_by"],
                _json(row.get("summary") or {}),
                row.get("version_id"),
            ),
        )

    def list_ingestion_runs(self, dataset: str | None, limit: int, cursor: str | None) -> Page:
        where, params = ([], []) if dataset is None else (["dataset = ?"], [dataset])
        return self._page("ingestion_runs", where, params, limit, cursor)

    def latest_ingestion_runs(self) -> dict[str, dict[str, Any]]:
        rows = self.db.query(
            "SELECT r.* FROM ingestion_runs r JOIN (SELECT dataset, MAX(started_at) AS mx FROM ingestion_runs GROUP BY dataset) m "
            "ON m.dataset = r.dataset AND m.mx = r.started_at"
        )
        return {r["dataset"]: d for r in rows if (d := _decode(r)) is not None}

    # -- jobs --------------------------------------------------------------------------------
    def create_job(self, row: dict[str, Any]) -> dict[str, Any]:
        try:
            self.db.execute(
                "INSERT INTO jobs (id, job_type, status, params_json, max_attempts, retry_of, idempotency_key, "
                "timeout_seconds, requested_by, request_id, created_at) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["id"],
                    row["job_type"],
                    _json(row.get("params") or {}),
                    row.get("max_attempts", 1),
                    row.get("retry_of"),
                    row.get("idempotency_key"),
                    row["timeout_seconds"],
                    row["requested_by"],
                    row.get("request_id"),
                    row["created_at"],
                ),
            )
        except IntegrityError as exc:
            if "idempotency_key" in str(exc):
                raise
            raise DuplicateActiveJob(f"A {row['job_type']} job is already queued or running") from exc
        job = self.get_job(row["id"])
        if job is None:
            raise StorageError("job insert not visible")
        return job

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM jobs WHERE id = ?", (job_id,))

    def find_job_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM jobs WHERE idempotency_key = ?", (key,))

    def find_active_job(self, job_type: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM jobs WHERE job_type = ? AND status IN ('queued', 'running')", (job_type,)
        )

    def list_jobs(self, limit: int, cursor: str | None, status: str | None = None) -> Page:
        where, params = ([], []) if status is None else (["status = ?"], [status])
        return self._page("jobs", where, params, limit, cursor)

    def mark_job_running(self, job_id: str, runner_id: str) -> bool:
        now = utcnow()
        return (
            self.db.execute(
                "UPDATE jobs SET status = 'running', runner_id = ?, started_at = ?, heartbeat_at = ? "
                "WHERE id = ? AND status = 'queued' AND cancel_requested = 0",
                (runner_id, now, now, job_id),
            )
            == 1
        )

    def job_attempt(self, job_id: str, attempt: int) -> None:
        self.db.execute(
            "UPDATE jobs SET attempt = ?, heartbeat_at = ? WHERE id = ?", (attempt, utcnow(), job_id)
        )

    def heartbeat_job(self, job_id: str) -> bool:
        """Refresh the heartbeat; returns True when cancellation has been requested."""
        self.db.execute(
            "UPDATE jobs SET heartbeat_at = ? WHERE id = ? AND status = 'running'", (utcnow(), job_id)
        )
        job = self.get_job(job_id)
        return bool(job and job["cancel_requested"])

    def finish_job(
        self,
        job_id: str,
        status: str,
        result: Any = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> bool:
        if status not in TERMINAL_JOB_STATUSES:
            raise ValueError(status)
        return (
            self.db.execute(
                "UPDATE jobs SET status = ?, result_json = ?, error_code = ?, error_message = ?, finished_at = ? "
                "WHERE id = ? AND status IN ('queued', 'running')",
                (status, _json(result), error_code, error_message, utcnow(), job_id),
            )
            == 1
        )

    def request_job_cancel(self, job_id: str) -> None:
        self.db.execute(
            "UPDATE jobs SET cancel_requested = 1 WHERE id = ? AND status IN ('queued', 'running')", (job_id,)
        )

    def fail_stale_jobs(self, stale_before: str, message: str) -> int:
        """Fail jobs whose runner disappeared (no heartbeat since ``stale_before``)."""
        return self.db.execute(
            "UPDATE jobs SET status = 'failed', error_code = 'INTERRUPTED', error_message = ?, finished_at = ? "
            "WHERE status IN ('queued', 'running') AND COALESCE(heartbeat_at, created_at) < ?",
            (message, utcnow(), stale_before),
        )

    # -- forecasts ---------------------------------------------------------------------------
    def insert_forecast(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO forecast_runs (id, created_at, request_id, requested_by, status, horizon, model_name, "
            "model_version, model_run_id, icp_dataset_version_id, wti_dataset_version_id, last_observed_period, "
            "params_json, result_json, error_code, error_message, export_object_key, duration_ms, "
            "issue_date, forecast_origin, method, scenario_json, dataset_versions_json, workflow, org_id, scenario_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["created_at"],
                row.get("request_id"),
                row["requested_by"],
                row["status"],
                row["horizon"],
                row.get("model_name"),
                row.get("model_version"),
                row.get("model_run_id"),
                row.get("icp_dataset_version_id"),
                row.get("wti_dataset_version_id"),
                row.get("last_observed_period"),
                _json(row["params"]),
                _json(row.get("result")),
                row.get("error_code"),
                row.get("error_message"),
                row.get("export_object_key"),
                row.get("duration_ms"),
                row.get("issue_date"),
                row.get("forecast_origin"),
                row.get("method"),
                _json(row.get("scenario")),
                _json(row.get("dataset_versions")),
                row.get("workflow"),
                row.get("org_id"),
                row.get("scenario_id"),
            ),
        )

    def get_forecast(self, forecast_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM forecast_runs WHERE id = ?", (forecast_id,))

    def list_forecasts(self, limit: int, cursor: str | None, status: str | None = None) -> Page:
        where, params = ([], []) if status is None else (["status = ?"], [status])
        return self._page("forecast_runs", where, params, limit, cursor)

    # -- promotions --------------------------------------------------------------------------
    def insert_promotion(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO model_promotions (id, created_at, model_name, action, from_version, to_version, reason, "
            "forced, validation_json, actor, request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["created_at"],
                row["model_name"],
                row["action"],
                row.get("from_version"),
                row["to_version"],
                row["reason"],
                int(bool(row.get("forced"))),
                _json(row.get("validation")),
                row["actor"],
                row.get("request_id"),
            ),
        )

    def latest_promotion(self, model_name: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM model_promotions WHERE model_name = ? ORDER BY created_at DESC, id DESC LIMIT 1",
            (model_name,),
        )

    def list_promotions(self, model_name: str, limit: int, cursor: str | None) -> Page:
        return self._page("model_promotions", ["model_name = ?"], [model_name], limit, cursor)

    # -- audit -------------------------------------------------------------------------------
    def audit(
        self,
        actor: str,
        action: str,
        outcome: str,
        target: str | None = None,
        request_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.db.execute(
            "INSERT INTO audit_events (id, created_at, actor, action, target, outcome, request_id, details_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (new_id("aud"), utcnow(), actor, action, target, outcome, request_id, _json(details)),
        )

    def list_audit(self, limit: int, cursor: str | None, action: str | None = None) -> Page:
        where, params = ([], []) if action is None else (["action = ?"], [action])
        return self._page("audit_events", where, params, limit, cursor)
