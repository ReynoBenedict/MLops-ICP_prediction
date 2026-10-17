"""Background job execution.

Design
* A job row is created first (status ``queued``). The partial unique index in the schema
  guarantees at most one queued/running job per type, across every backend instance.
* Each attempt runs in a **separate Python process** (``python -m icp_api.worker``), so the
  runner can enforce timeouts and cancellation by terminating it, and a crashing training run
  cannot take the API down.
* The runner heartbeats while the process runs; a reaper fails jobs whose heartbeat is stale
  (e.g. the instance restarted), so nothing stays "running" forever.
* Transient failures (worker exit code 75, e.g. network errors while fetching WTI) are retried
  with exponential backoff up to ``max_attempts``; other failures are final. Manual retries
  create a new job linked through ``retry_of``.

Constraint: jobs run on the instance that accepted them. Deploy the backend as a single
instance; on a free tier that sleeps when idle, submit jobs while the instance is awake and keep
them short (ingestion); training is done in the operator environment in bundle mode. See
docs/DEPLOYMENT.md.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from icp_api.errors import ApiError
from icp_api.metrics import JOBS
from icp_api.settings import Settings
from icp_api.storage.repository import (
    TERMINAL_JOB_STATUSES,
    DuplicateActiveJob,
    MetadataRepository,
    Page,
    new_id,
    utc_before,
    utcnow,
)
from icp_api.storage.sql import IntegrityError

logger = logging.getLogger(__name__)

EXIT_TRANSIENT = 75  # EX_TEMPFAIL


def job_definitions(settings: Settings) -> dict[str, dict[str, int]]:
    defs = {
        "train": {"timeout_seconds": settings.job_timeout_train_seconds, "max_attempts": 1},
        "ingest_benchmarks": {"timeout_seconds": settings.job_timeout_ingest_seconds, "max_attempts": 3},
        "ingest_fx": {"timeout_seconds": settings.job_timeout_ingest_seconds, "max_attempts": 3},
        # ESDM is slow and the first run OCRs ~90 decrees; allow the long timeout.
        "ingest_icp": {"timeout_seconds": max(settings.job_timeout_train_seconds, 3600), "max_attempts": 2},
        "refresh_forecast": {"timeout_seconds": 300, "max_attempts": 2},
        "evaluate_alerts": {"timeout_seconds": 300, "max_attempts": 2},
    }
    if settings.app_env in ("local", "test"):
        defs["selftest"] = {"timeout_seconds": 30, "max_attempts": 2}
    return defs


class JobManager:
    def __init__(self, repo: MetadataRepository, settings: Settings):
        self.repo = repo
        self.settings = settings
        self.definitions = job_definitions(settings)
        self.runner_id = f"runner_{os.getpid()}_{new_id('r')[-8:]}"
        self._slots = threading.BoundedSemaphore(max(1, settings.job_max_concurrent))
        self._threads: dict[str, threading.Thread] = {}
        self._stop = threading.Event()
        self._reaper: threading.Thread | None = None

    # -- lifecycle ----------------------------------------------------------------------------
    def start(self) -> None:
        self.reap()
        self._reaper = threading.Thread(target=self._reap_loop, name="job-reaper", daemon=True)
        self._reaper.start()

    def stop(self) -> None:
        self._stop.set()

    def _reap_loop(self) -> None:
        while not self._stop.wait(max(5, self.settings.job_stale_after_seconds // 2)):
            try:
                self.reap()
            except Exception:
                logger.exception("job reaper failed")

    def reap(self) -> int:
        n = self.repo.fail_stale_jobs(
            utc_before(self.settings.job_stale_after_seconds),
            "The job runner stopped (backend restart or crash) before the job finished. Retry the job.",
        )
        if n:
            logger.warning("marked %d stale job(s) as failed", n)
        return n

    # -- API operations -----------------------------------------------------------------------
    def submit(
        self,
        job_type: str,
        params: dict[str, Any],
        actor: str,
        request_id: str | None,
        idempotency_key: str | None = None,
        retry_of: str | None = None,
    ) -> dict[str, Any]:
        definition = self.definitions.get(job_type)
        if definition is None:
            raise ApiError(422, "VALIDATION_ERROR", f"Unknown job type '{job_type}'.")
        if job_type == "train" and self.settings.model_source == "bundle":
            raise ApiError(
                409,
                "OPERATION_UNAVAILABLE",
                "Training is unavailable in bundle mode: train and promote in the operator environment "
                "(MLflow), then export the promoted version with `icp-api export-model`.",
            )
        if idempotency_key:
            existing = self.repo.find_job_by_idempotency_key(idempotency_key)
            if existing is not None:
                if existing["job_type"] != job_type:
                    raise ApiError(
                        409, "IDEMPOTENCY_KEY_REUSED", "Idempotency key was used for another job type."
                    )
                return existing
        row = {
            "id": new_id("job"),
            "job_type": job_type,
            "params": params,
            "retry_of": retry_of,
            "idempotency_key": idempotency_key,
            "requested_by": actor,
            "request_id": request_id,
            "created_at": utcnow(),
            **definition,
        }
        try:
            job = self.repo.create_job(row)
        except DuplicateActiveJob as exc:
            active = self.repo.find_active_job(job_type)
            raise ApiError(
                409, "JOB_ALREADY_ACTIVE", str(exc), {"active_job_id": active["id"] if active else None}
            ) from exc
        except IntegrityError as exc:
            raise ApiError(409, "IDEMPOTENCY_KEY_REUSED", "Idempotency key already used.") from exc
        thread = threading.Thread(target=self._run, args=(job["id"],), name=f"job-{job['id']}", daemon=True)
        self._threads[job["id"]] = thread
        thread.start()
        return job

    def get(self, job_id: str) -> dict[str, Any]:
        job = self.repo.get_job(job_id)
        if job is None:
            raise ApiError(404, "NOT_FOUND", "Job not found.")
        return job

    def list(self, limit: int, cursor: str | None, status: str | None) -> Page:
        return self.repo.list_jobs(limit, cursor, status)

    def cancel(self, job_id: str) -> dict[str, Any]:
        job = self.get(job_id)
        if job["status"] in TERMINAL_JOB_STATUSES:
            raise ApiError(409, "JOB_FINISHED", f"Job is already {job['status']}.")
        self.repo.request_job_cancel(job_id)
        if job["status"] == "queued":
            self.repo.finish_job(
                job_id, "cancelled", error_code="CANCELLED", error_message="Cancelled before start."
            )
        return self.get(job_id)

    def retry(self, job_id: str, actor: str, request_id: str | None) -> dict[str, Any]:
        job = self.get(job_id)
        if job["status"] not in ("failed", "cancelled"):
            raise ApiError(409, "JOB_NOT_RETRYABLE", "Only failed or cancelled jobs can be retried.")
        return self.submit(job["job_type"], job["params"] or {}, actor, request_id, retry_of=job_id)

    def wait(self, job_id: str, timeout: float) -> dict[str, Any]:
        """Test/CLI helper: block until the job's runner thread finishes."""
        thread = self._threads.get(job_id)
        if thread:
            thread.join(timeout)
        return self.get(job_id)

    # -- execution ----------------------------------------------------------------------------
    def _run(self, job_id: str) -> None:
        with self._slots:
            if not self.repo.mark_job_running(job_id, self.runner_id):
                return  # cancelled while queued
            job = self.get(job_id)
            status, payload = "failed", {"error_code": "JOB_FAILED", "error_message": "Job did not run."}
            for attempt in range(1, int(job["max_attempts"]) + 1):
                self.repo.job_attempt(job_id, attempt)
                status, payload = self._attempt(job)
                logger.info(
                    "job attempt finished", extra={"job_id": job_id, "attempt": attempt, "outcome": status}
                )
                if status != "transient" or attempt == job["max_attempts"]:
                    break
                if self._sleep_cancellable(job_id, min(60, 2**attempt)):
                    status, payload = "cancelled", {"error_code": "CANCELLED", "error_message": "Cancelled."}
                    break
            if status == "succeeded":
                self.repo.finish_job(job_id, "succeeded", result=payload.get("result"))
            elif status == "cancelled":
                self.repo.finish_job(
                    job_id,
                    "cancelled",
                    error_code="CANCELLED",
                    error_message=payload.get("error_message", "Cancelled."),
                )
            else:
                self.repo.finish_job(
                    job_id,
                    "failed",
                    error_code=payload.get("error_code", "JOB_FAILED"),
                    error_message=str(payload.get("error_message", "Job failed."))[:1000],
                )
            final = self.get(job_id)["status"]
            JOBS.labels(job_type=job["job_type"], status=final).inc()
            self.repo.audit(
                f"job:{job_id}",
                f"job.{job['job_type']}.finished",
                "success" if final == "succeeded" else "failure",
                target=job_id,
                request_id=job.get("request_id"),
                details={"status": final},
            )

    def _sleep_cancellable(self, job_id: str, seconds: float) -> bool:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self.repo.heartbeat_job(job_id):
                return True
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        return False

    def _attempt(self, job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        fd, result_path = tempfile.mkstemp(prefix=f"{job['id']}-", suffix=".json")
        os.close(fd)
        Path(result_path).write_text("", encoding="utf-8")
        python = self.settings.job_python or sys.executable
        cmd = [python, "-m", "icp_api.worker", job["id"], result_path]
        deadline = time.monotonic() + int(job["timeout_seconds"])
        next_beat = 0.0
        outcome: str | None = None
        try:
            proc = subprocess.Popen(cmd, env=os.environ.copy())  # noqa: S603
            while proc.poll() is None:
                now = time.monotonic()
                if now >= deadline:
                    outcome = "timeout"
                    break
                if now >= next_beat:
                    next_beat = now + self.settings.job_heartbeat_seconds
                    if self.repo.heartbeat_job(job["id"]):
                        outcome = "cancelled"
                        break
                time.sleep(0.25)
            if outcome is not None:
                proc.terminate()
                try:
                    proc.wait(10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(10)
                if outcome == "timeout":
                    return "failed", {
                        "error_code": "TIMEOUT",
                        "error_message": f"Job exceeded its {job['timeout_seconds']}s timeout.",
                    }
                return "cancelled", {"error_message": "Cancelled while running."}
            text = Path(result_path).read_text(encoding="utf-8").strip()
            payload = json.loads(text) if text else {}
            if proc.returncode == 0 and payload.get("status") == "succeeded":
                return "succeeded", payload
            if proc.returncode == EXIT_TRANSIENT:
                return "transient", payload
            return "failed", {
                "error_code": payload.get("error_code", "WORKER_CRASHED"),
                "error_message": payload.get("error_message", f"Worker exited with code {proc.returncode}."),
            }
        finally:
            Path(result_path).unlink(missing_ok=True)
