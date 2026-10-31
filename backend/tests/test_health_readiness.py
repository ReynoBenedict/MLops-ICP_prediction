"""Readiness probe: concurrent, bounded checks; truthful and quick while the start-up model warm-up runs;
a slow check is shared by later probes instead of piling up threads (2026-10-25)."""

from __future__ import annotations

import contextlib
import threading
import time
from typing import Any

from icp_api.routers import health

API = "/api/v1"


def _checks(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {c["name"]: c for c in body["checks"]}


def _settle(c: Any) -> None:
    """Wait until the start-up warm-up and every check thread of this container have finished."""
    deadline = time.time() + 30
    while time.time() < deadline and (
        c.models.warming or any(not f.done() for (owner, _), f in health._inflight.items() if owner == id(c))
    ):
        time.sleep(0.05)


def _wait_done(c: Any, name: str) -> None:
    """Wait until the in-flight check ``name`` of this container has finished (no wall-clock assertion)."""
    fut = health._inflight.get((id(c), name))
    if fut is not None:
        with contextlib.suppress(Exception):  # only completion matters here
            fut.result(timeout=10)


def test_checks_run_concurrently(client, admin, monkeypatch):
    """Two checks that can only finish together (a barrier both must reach) both succeed: they ran at
    the same time. Run one after the other, the first would give up at the barrier and report down."""
    c = client.app.state.container
    _settle(c)
    both = threading.Barrier(2, timeout=health.CHECK_TIMEOUT - 1)
    real_ping = c.objects.ping

    def ping_with_barrier() -> None:
        both.wait()
        real_ping()

    def status_with_barrier() -> dict[str, Any]:
        both.wait()
        return {"available": False, "message": "No model version holds the alias."}

    monkeypatch.setattr(c.objects, "ping", ping_with_barrier)
    monkeypatch.setattr(c.models, "status", status_with_barrier)
    r = client.get("/health/ready", headers=admin)
    assert r.status_code == 200
    checks = _checks(r.json())
    assert checks["object_store"]["status"] == "ok", checks["object_store"]
    assert checks["production_model"]["status"] == "degraded", checks["production_model"]
    assert not both.broken
    assert list(checks) == [
        "metadata_store",
        "schema_version",
        "object_store",
        "model_registry",
        "production_model",
        "datasets",
        "job_runner",
    ]


def test_model_still_loading_is_degraded_at_once_and_public_body_is_status_only(client, admin, monkeypatch):
    """During the start-up warm-up the model checks do not wait CHECK_TIMEOUT: "still loading" is known."""
    c = client.app.state.container
    _settle(c)
    release = threading.Event()

    def loading() -> dict[str, Any]:
        release.wait(10)
        return {"available": False, "message": "No model version holds the alias."}

    monkeypatch.setattr(c.models, "status", loading)
    c.models._warming.set()
    try:
        body = client.get("/health/ready", headers=admin).json()
        model = _checks(body)["production_model"]
        assert model["status"] == "degraded" and "loading" in model["detail"]
        # bounded by WARM_UP_WAIT (0.5 s), not CHECK_TIMEOUT (5 s)
        assert model["latency_ms"] < health.CHECK_TIMEOUT * 1000 / 2, model
        assert body["status"] == "degraded"
        public = client.get("/health/ready")
        assert public.status_code == 200 and public.json() == {
            "status": "degraded",
            "environment": None,
            "version": None,
            "checks": None,
        }
    finally:
        c.models._warming.clear()
    # the same timeout after the warm-up is a dependency that does not answer
    monkeypatch.setattr(health, "CHECK_TIMEOUT", 0.3)
    body = client.get("/health/ready", headers=admin).json()
    assert _checks(body)["production_model"]["status"] == "down"
    release.set()
    _settle(c)


def test_a_running_check_is_shared_by_later_probes(client, admin, monkeypatch):
    c = client.app.state.container
    _settle(c)
    calls = []
    release = threading.Event()

    def slow_status() -> dict[str, Any]:
        calls.append(time.perf_counter())
        release.wait(10)
        return {"available": False, "message": "No model version holds the alias."}

    monkeypatch.setattr(health, "CHECK_TIMEOUT", 0.2)
    monkeypatch.setattr(c.models, "status", slow_status)
    for _ in range(3):
        assert (
            _checks(client.get("/health/ready", headers=admin).json())["production_model"]["status"] == "down"
        )
    assert len(calls) == 1  # one thread for three probes while the first check is still running
    release.set()
    _wait_done(c, "production_model")
    body = client.get("/health/ready", headers=admin).json()
    assert _checks(body)["production_model"]["status"] == "degraded"
    assert len(calls) == 2  # a finished check is run again by the next probe
    _settle(c)


def test_every_check_is_shared_while_it_runs(client, admin, monkeypatch):
    """Not only the model checks: a stuck object store or metadata store is not pinged again by every
    probe while the first ping is still running."""
    c = client.app.state.container
    _settle(c)
    pings = {"objects": 0, "db": 0}
    release = threading.Event()
    real_obj, real_db = c.objects.ping, c.db.ping

    def stuck_objects() -> None:
        pings["objects"] += 1
        release.wait(10)
        real_obj()

    def stuck_db() -> None:
        pings["db"] += 1
        release.wait(10)
        real_db()

    monkeypatch.setattr(health, "CHECK_TIMEOUT", 0.2)
    monkeypatch.setattr(c.objects, "ping", stuck_objects)
    monkeypatch.setattr(c.db, "ping", stuck_db)
    for _ in range(3):
        r = client.get("/health/ready", headers=admin)
        assert r.status_code == 503
        checks = _checks(r.json())
        assert checks["object_store"]["status"] == "down" and checks["metadata_store"]["status"] == "down"
        assert checks["schema_version"]["status"] == "degraded"  # unknown while the store does not answer
    assert pings == {"objects": 1, "db": 1}
    release.set()
    _wait_done(c, "object_store")
    _wait_done(c, "metadata_store")
    monkeypatch.undo()
    body = client.get("/health/ready", headers=admin).json()
    checks = _checks(body)
    assert checks["object_store"]["status"] == "ok" and checks["metadata_store"]["status"] == "ok"
    assert checks["schema_version"]["status"] == "ok"
    _settle(c)


def test_a_database_behind_the_code_is_down_with_a_clear_detail(client, admin, monkeypatch):
    c = client.app.state.container
    _settle(c)
    want = health.expected_version()
    monkeypatch.setattr(health, "current_version", lambda db: want - 1)
    r = client.get("/health/ready", headers=admin)
    assert r.status_code == 503 and r.json()["status"] == "not_ready"
    checks = _checks(r.json())
    assert checks["metadata_store"]["status"] == "down"
    assert (
        checks["metadata_store"]["detail"] == f"schema version {want - 1}, expected {want}: apply migrations"
    )
    # judged from the same measurement: behind is not "newer than this code"
    assert checks["schema_version"]["status"] == "ok"
    assert checks["schema_version"]["detail"] == f"v{want - 1} (expected v{want})"
    status = client.get(f"{API}/system/status", headers=admin).json()
    assert status["schema_version"] == want - 1 and status["expected_schema_version"] == want
    monkeypatch.undo()
    assert client.get("/health/ready", headers=admin).status_code == 200
    _settle(c)


def test_start_up_warm_up_loads_in_the_background(client):
    """The warm-up flag is set before the thread starts and cleared when it ends, whatever the outcome."""
    models = client.app.state.container.models
    thread = models.start_warm_up()
    thread.join(60)
    assert not thread.is_alive() and models.warming is False
