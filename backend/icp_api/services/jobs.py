"""Background job execution.

Design
* A job row is created first (status ``queued``). The partial unique index in the schema
  guarantees at most one queued/running job per type, across every backend instance.
* Each attempt runs in a **separate Python process** (``python -m icp_api.worker``), so the
  runner can enforce timeouts and cancellation by terminating it, and a crashing training run
  cannot take the API down.
* The runner heartbeats while the process runs, and a job that is queued behind the runner's
  concurrency slot heartbeats while it waits; a reaper fails jobs whose heartbeat is stale (e.g.
  the instance restarted), so nothing stays "queued"/"running" forever (audit v7 JOB-02).
* If the metadata store stops accepting heartbeats for half the stale window, the worker process
  is terminated *before* the reaper could declare the job stale, so a reaped job never has a live
  worker and two jobs of one type never run at once (audit v7 JOB-01). Any unexpected runner
  error still records a terminal state when the store allows it.
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
    snapshot_data_changed,
    utc_before,
    utcnow,
)
from icp_api.storage.sql import IntegrityError, StorageError

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
        self._touch_waiting()
        n = self.repo.fail_stale_jobs(
            utc_before(self.settings.job_stale_after_seconds),
            "The job runner stopped (backend restart or crash) before the job finished. Retry the job.",
        )
        if n:
            logger.warning("marked %d stale job(s) as failed", n)
        return n

    def _waiting_ids(self) -> list[str]:
        return [jid for jid, t in list(self._threads.items()) if t.is_alive()]

    def _touch_waiting(self, job_ids: list[str] | None = None) -> None:
        """Refresh the liveness of queued jobs whose runner thread is alive in this process.

        A queued job only waits for a free slot; without this its NULL heartbeat would make the
        reaper fail it after ``job_stale_after_seconds`` (JOB-02). Jobs queued by a previous process
        have no live thread here and are still reaped. Status-guarded, so it never revives a job.
        """
        ids = self._waiting_ids() if job_ids is None else job_ids
        for job_id in ids:
            try:
                self.repo.db.execute(
                    "UPDATE jobs SET heartbeat_at = ? WHERE id = ? AND status = 'queued'", (utcnow(), job_id)
                )
            except StorageError:
                logger.warning("could not refresh queued job liveness", exc_info=True)
                return

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
        job_id = new_id("job")
        row = {
            "id": job_id,
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
        except (DuplicateActiveJob, IntegrityError) as exc:
            # An ambiguous write (e.g. a transport retry after the first INSERT was applied) can
            # report a conflict with the row this very call created; that job is ours (DATA-01).
            own = self.repo.get_job(job_id)
            if own is None:
                if isinstance(exc, DuplicateActiveJob):
                    active = self.repo.find_active_job(job_type)
                    raise ApiError(
                        409,
                        "JOB_ALREADY_ACTIVE",
                        str(exc),
                        {"active_job_id": active["id"] if active else None},
                    ) from exc
                raise ApiError(409, "IDEMPOTENCY_KEY_REUSED", "Idempotency key already used.") from exc
            job = own
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
    def _acquire_slot(self, job_id: str) -> None:
        """Wait for a runner slot, keeping the queued job visibly alive meanwhile (JOB-02)."""
        interval = max(
            0.2, min(float(self.settings.job_heartbeat_seconds), self.settings.job_stale_after_seconds / 3)
        )
        while not self._slots.acquire(timeout=interval):
            self._touch_waiting([job_id])

    def _run(self, job_id: str) -> None:
        self._acquire_slot(job_id)
        try:
            self._run_in_slot(job_id)
        except Exception:
            logger.exception("job runner failed", extra={"job_id": job_id})
            try:
                self.repo.finish_job(
                    job_id,
                    "failed",
                    error_code="RUNNER_ERROR",
                    error_message="The job runner hit an unexpected error; the worker was stopped. Retry the job.",
                )
            except Exception:
                # The store is unavailable; the reaper fails the job once its heartbeat is stale,
                # and no worker process is left running (``_attempt`` stops it before re-raising).
                logger.exception("could not record the job failure", extra={"job_id": job_id})
        finally:
            self._slots.release()

    def _run_in_slot(self, job_id: str) -> None:
        if not self.repo.mark_job_running(job_id, self.runner_id):
            return  # cancelled while queued (or reaped)
        job = self.get(job_id)
        status, payload = "failed", {"error_code": "JOB_FAILED", "error_message": "Job did not run."}
        for attempt in range(1, int(job["max_attempts"]) + 1):
            self.repo.job_attempt(job_id, attempt)
            try:
                status, payload = self._attempt(job)
            finally:
                # The worker process may have written what the public snapshot shows (a new dataset
                # version, evidence, a system forecast); its in-process memo learns it here, before the
                # job is reported as finished (services/public_snapshot.py, SEC-C-01).
                snapshot_data_changed()
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
        # Stop the attempt well before the reaper may call the job stale (JOB-01).
        storage_grace = max(1.0, self.settings.job_stale_after_seconds / 2)
        next_beat = 0.0
        last_beat_ok = time.monotonic()
        outcome: str | None = None
        proc: subprocess.Popen[bytes] | None = None
        try:
            proc = subprocess.Popen(cmd, env=os.environ.copy())  # noqa: S603
            while proc.poll() is None:
                now = time.monotonic()
                if now >= deadline:
                    outcome = "timeout"
                    break
                if now >= next_beat:
                    next_beat = now + self.settings.job_heartbeat_seconds
                    cancel = False
                    try:
                        cancel = self.repo.heartbeat_job(job["id"])
                    except Exception:
                        logger.warning("job heartbeat failed", exc_info=True, extra={"job_id": job["id"]})
                        if now - last_beat_ok >= storage_grace:
                            outcome = "storage"
                            break
                        next_beat = now + min(1.0, float(self.settings.job_heartbeat_seconds))
                    else:
                        last_beat_ok = now
                    if cancel:
                        outcome = "cancelled"
                        break
                time.sleep(0.25)
            if outcome is not None:
                _stop(proc)
                if outcome == "timeout":
                    return "failed", {
                        "error_code": "TIMEOUT",
                        "error_message": f"Job exceeded its {job['timeout_seconds']}s timeout.",
                    }
                if outcome == "storage":
                    return "failed", {
                        "error_code": "RUNNER_STORAGE_ERROR",
                        "error_message": "The metadata store stopped responding; the worker was stopped. "
                        "Retry the job.",
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
        except BaseException:
            if proc is not None:
                _stop(proc)
            raise
        finally:
            Path(result_path).unlink(missing_ok=True)


def _stop(proc: subprocess.Popen[bytes]) -> None:
    """Terminate the worker process and wait for it (kill it if it ignores the request)."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(10)
