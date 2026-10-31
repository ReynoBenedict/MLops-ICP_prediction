"""Job runner reliability (audit v7: JOB-01, JOB-02, DATA-01 create_job ambiguity)."""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import replace
from typing import Any

import pytest

from icp_api.services import jobs as jobs_module
from icp_api.services.jobs import JobManager
from icp_api.settings import load_settings
from icp_api.storage.migrations import apply_local
from icp_api.storage.repository import DuplicateActiveJob, MetadataRepository
from icp_api.storage.sql import SqliteExecutor, StorageError


@pytest.fixture()
def repo(tmp_path) -> MetadataRepository:
    db = SqliteExecutor(tmp_path / "jobs.db")
    apply_local(db)
    return MetadataRepository(db)


def _manager(repo: MetadataRepository, env_dir: Any, **overrides: Any) -> JobManager:
    settings = replace(
        load_settings(),
        job_max_concurrent=1,
        job_heartbeat_seconds=1,
        job_stale_after_seconds=3,
        **overrides,
    )
    mgr = JobManager(repo, settings)
    mgr.definitions["slow_a"] = {"timeout_seconds": 60, "max_attempts": 1}
    mgr.definitions["slow_b"] = {"timeout_seconds": 60, "max_attempts": 1}
    return mgr


def _fake_attempt(mgr: JobManager, seconds: float):
    def attempt(job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            mgr.repo.heartbeat_job(job["id"])
            time.sleep(0.2)
        return "succeeded", {"status": "succeeded", "result": {"ok": True}}

    return attempt


def test_reaper_keeps_a_queued_job_that_is_only_waiting_for_the_runner_slot(repo, env_dir, monkeypatch):
    """JOB-02: a job queued behind another job's slot is alive, not 'interrupted'."""
    mgr = _manager(repo, env_dir)
    monkeypatch.setattr(mgr, "_attempt", _fake_attempt(mgr, 6.0))
    first = mgr.submit("slow_a", {}, "t", None)
    time.sleep(0.5)
    second = mgr.submit("slow_b", {}, "t", None)
    time.sleep(4.5)  # longer than job_stale_after_seconds while the second job waits
    assert mgr.reap() == 0
    assert mgr.get(second["id"])["status"] == "queued"
    assert mgr.wait(first["id"], 15)["status"] == "succeeded"
    assert mgr.wait(second["id"], 15)["status"] == "succeeded"


def test_reaper_still_fails_orphaned_queued_jobs(repo, env_dir):
    """A queued job without a live runner thread (previous process) is still reaped."""
    mgr = _manager(repo, env_dir)
    repo.create_job(
        {
            "id": "job_orphan",
            "job_type": "slow_a",
            "params": {},
            "requested_by": "t",
            "created_at": "2020-01-01T00:00:00.000Z",
            "timeout_seconds": 60,
            "max_attempts": 1,
        }
    )
    assert mgr.reap() == 1
    assert repo.get_job("job_orphan")["error_code"] == "INTERRUPTED"


def test_worker_is_terminated_when_the_metadata_store_fails_during_heartbeats(repo, env_dir, monkeypatch):
    """JOB-01: no orphaned worker process; the job ends in a safe terminal state."""
    mgr = _manager(repo, env_dir)
    started: list[subprocess.Popen[bytes]] = []
    real_popen = subprocess.Popen

    def fake_popen(cmd: list[str], **kw: Any) -> subprocess.Popen[bytes]:
        proc = real_popen([sys.executable, "-c", "import time; time.sleep(60)"])
        started.append(proc)
        return proc

    monkeypatch.setattr(jobs_module.subprocess, "Popen", fake_popen)

    def broken_heartbeat(job_id: str) -> bool:
        raise StorageError("D1 request timed out")

    monkeypatch.setattr(repo, "heartbeat_job", broken_heartbeat)
    job = mgr.submit("slow_a", {}, "t", None)
    done = mgr.wait(job["id"], 20)
    assert started, "worker was not started"
    assert started[0].poll() is not None, "worker subprocess must be terminated"
    assert done["status"] == "failed"
    assert done["error_code"] == "RUNNER_STORAGE_ERROR"


def test_runner_records_a_terminal_state_when_an_unexpected_error_escapes(repo, env_dir, monkeypatch):
    mgr = _manager(repo, env_dir)

    def boom(job: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        raise RuntimeError("unexpected")

    monkeypatch.setattr(mgr, "_attempt", boom)
    job = mgr.submit("slow_a", {}, "t", None)
    done = mgr.wait(job["id"], 10)
    assert done["status"] == "failed"
    assert done["error_code"] == "RUNNER_ERROR"


def test_submit_returns_its_own_job_when_an_ambiguous_insert_was_applied(repo, env_dir, monkeypatch):
    """DATA-01: a retried INSERT that hits its own row is not 'JOB_ALREADY_ACTIVE'."""
    mgr = _manager(repo, env_dir)
    monkeypatch.setattr(mgr, "_attempt", _fake_attempt(mgr, 0.1))
    real_create = repo.create_job

    def applied_then_conflict(row: dict[str, Any]) -> dict[str, Any]:
        real_create(row)
        raise DuplicateActiveJob("A slow_a job is already queued or running")

    monkeypatch.setattr(repo, "create_job", applied_then_conflict)
    job = mgr.submit("slow_a", {}, "t", None)
    assert mgr.wait(job["id"], 10)["status"] == "succeeded"


def test_worker_refuses_a_job_that_is_no_longer_running():
    from icp_api.worker import refusal

    assert refusal({"status": "running"}) is None
    assert refusal({"status": "failed"}) == "JOB_NOT_RUNNING"
    assert refusal({"status": "queued"}) == "JOB_NOT_RUNNING"
