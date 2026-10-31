"""Data-plane retries end with the request's own deadline (REQUEST_TIMEOUT_SECONDS).

The middleware answers 504 once the request budget is used up, but a synchronous endpoint keeps running
in its worker thread. Before this change a D1 read could keep retrying for 3 x 15 s and a KV read for
3 x 30 s after that answer. Only retries, back-off and reads are bounded: a write's first attempt still
starts after the deadline, so a write sequence (report files, then the report row; an action, then its
audit row) is never cut in the middle (review PLAT-B-01). The clock is a fake: nothing sleeps for real.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from icp_api.middleware import RequestContextMiddleware
from icp_api.settings import load_settings
from icp_api.storage import sql as sql_module
from icp_api.storage.objects import GatewayObjectStore
from icp_api.storage.sql import (
    D1HttpExecutor,
    GatewayD1Executor,
    RequestDeadlineExceeded,
    StorageError,
    remaining_request_seconds,
    request_deadline,
)

GATEWAY = "https://icp-gateway-staging.example.workers.dev"
SECRET = "test-gateway-secret-0123456789abcdefghijkl"


class FakeClock:
    """Replaces ``time`` in ``storage.sql``: monotonic() is fake, sleep() advances it."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(sql_module, "time", fake)  # the deadline and the back-off of both clients live there
    return fake


class Upstream:
    """A data-plane endpoint that takes ``takes`` (fake) seconds per call and then answers or fails."""

    def __init__(self, clock: FakeClock, takes: float, answer: Any) -> None:
        self.clock, self.takes, self.answer = clock, takes, answer
        self.timeouts: list[float] = []  # the read timeout each attempt was given

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.timeouts.append(request.extensions["timeout"]["read"])
        self.clock.now += self.takes
        result = self.answer(request) if callable(self.answer) else self.answer
        if isinstance(result, Exception):
            raise result
        return result


def _d1(up: Upstream) -> D1HttpExecutor:
    return D1HttpExecutor("acct", "db", "d1-token", transport=httpx.MockTransport(up.handler))


def _kv(up: Upstream) -> GatewayObjectStore:
    return GatewayObjectStore(GATEWAY, SECRET, transport=httpx.MockTransport(up.handler))


def _unavailable(_: httpx.Request) -> httpx.Response:
    return httpx.Response(503, json={"success": False, "errors": [{"message": "unavailable"}]})


def test_d1_read_retries_stop_at_the_request_deadline(clock):
    up = Upstream(clock, takes=6.0, answer=_unavailable)
    with request_deadline(10), pytest.raises(RequestDeadlineExceeded, match="within the request deadline"):
        _d1(up).query("SELECT 1")
    # attempt 1 at t=0 (6 s), back-off 0.25 s, attempt 2 at t=6.25 with the 3.75 s that were left;
    # it ends at t=12.25, after the deadline: no third attempt (the old code made one, up to t=27.75)
    assert len(up.timeouts) == 2
    assert up.timeouts[0] == pytest.approx(10.0)
    assert up.timeouts[1] == pytest.approx(3.75)
    assert clock.sleeps == [0.25]


def test_d1_read_retries_are_unchanged_outside_a_request(clock):
    up = Upstream(clock, takes=6.0, answer=_unavailable)
    with pytest.raises(StorageError, match="after 3 attempts"):
        _d1(up).query("SELECT 1")
    assert up.timeouts == [15.0, 15.0, 15.0]  # the configured timeout, as before
    assert clock.sleeps == [0.25, 0.5]
    assert remaining_request_seconds() is None


def test_no_read_starts_once_the_deadline_has_passed(clock):
    up = Upstream(clock, takes=0.0, answer=_unavailable)
    with request_deadline(5):
        clock.now += 5.0  # the endpoint used the whole budget before its first data-plane call
        with pytest.raises(RequestDeadlineExceeded, match="deadline passed"):
            _d1(up).query("SELECT 1")
        with pytest.raises(RequestDeadlineExceeded):
            _kv(up).get("datasets/icp/a.csv")
    assert up.timeouts == []  # nothing was sent


def test_a_write_attempt_keeps_its_full_timeout_but_is_not_retried_after_the_deadline(clock):
    up = Upstream(
        clock, takes=6.0, answer=lambda _: httpx.Response(429, json={"success": False, "errors": []})
    )
    with request_deadline(5), pytest.raises(RequestDeadlineExceeded):
        _d1(up).execute("INSERT INTO t VALUES (1)")
    # a write cut short could land after we report it as failed, so its timeout is not shortened
    assert up.timeouts == [15.0]
    kv = Upstream(clock, takes=6.0, answer=lambda _: httpx.Response(503, json={}))
    with request_deadline(5), pytest.raises(RequestDeadlineExceeded):
        _kv(kv).put("exports/reports/2026/10/rpt_x.csv", b"a,b\n", "text/csv")
    assert kv.timeouts == [30.0]


def test_a_write_that_starts_after_the_deadline_is_still_sent_once(clock):
    """Bounding the retries must not cut a write sequence in the middle (review PLAT-B-01)."""
    ok = {"success": True, "result": [{"success": True, "results": [], "meta": {"changes": 1}}]}
    up = Upstream(clock, takes=1.0, answer=httpx.Response(200, json=ok))
    kv = Upstream(clock, takes=1.0, answer=httpx.Response(201, json={"key": "k"}))
    with request_deadline(5):
        clock.now += 6.0  # the endpoint is already past its deadline
        assert _d1(up).execute("INSERT INTO t VALUES (1)") == 1
        _kv(kv).put("exports/reports/2026/10/rpt_x.pdf", b"%PDF", "application/pdf")
        with pytest.raises(RequestDeadlineExceeded):
            _d1(up).query("SELECT 1")  # a read still does not start
    assert up.timeouts == [15.0]  # the write, with its full timeout; the read was never sent
    assert kv.timeouts == [30.0]
    # its retries are bounded like any other: a write refused with 429 after the deadline is not repeated
    busy = Upstream(clock, takes=1.0, answer=httpx.Response(429, json={"success": False, "errors": []}))
    with request_deadline(5):
        clock.now += 6.0
        with pytest.raises(RequestDeadlineExceeded):
            _d1(busy).execute("INSERT INTO t VALUES (1)")
    assert busy.timeouts == [15.0]


def _report_service(gw: Any) -> Any:
    from icp_api.services.reports import ReportService
    from icp_api.storage.product import ProductRepository

    db = GatewayD1Executor(GATEWAY, gw.SECRET, transport=gw.transport)
    store = GatewayObjectStore(GATEWAY, gw.SECRET, transport=gw.transport)
    # only the storage collaborators matter here; _build/_pdf/_csv are replaced per test
    return ReportService(ProductRepository(db), None, None, None, None, None, store)  # type: ignore[arg-type]


def _report_rows(gw: Any) -> list[dict[str, Any]]:
    rows = gw.d1.conn.execute(
        "SELECT status, error_message, object_key_pdf, object_key_csv FROM report_runs"
    ).fetchall()
    return [dict(r) for r in rows]


def test_a_report_cut_off_by_the_deadline_still_leaves_a_failed_row(clock, tmp_path, gatewaysim, monkeypatch):
    from icp_api.errors import ApiError

    gw = gatewaysim(tmp_path / "gw.db")
    gw.apply_migrations()
    service = _report_service(gw)

    def slow_build(*_: Any, **__: Any) -> dict[str, Any]:
        clock.now += 31.0  # building the report took longer than the request budget ...
        service.repo.db.query("SELECT 1 AS ok")  # ... so its next read is refused
        raise AssertionError("unreachable")

    monkeypatch.setattr(service, "_build", slow_build)
    with request_deadline(30), pytest.raises(ApiError) as failed:
        service.generate("market_monthly", {}, "op", None, "req-deadline", formats=("pdf", "csv"))
    assert failed.value.code == "REPORT_FAILED"
    rows = _report_rows(gw)
    assert len(rows) == 1  # the old behaviour refused this insert too: no row at all
    assert rows[0]["status"] == "failed"
    assert rows[0]["error_message"].startswith("REPORT_FAILED")
    assert rows[0]["object_key_pdf"] is None and rows[0]["object_key_csv"] is None
    assert gw.objects == {}


def test_a_report_whose_files_are_written_after_the_deadline_is_recorded_as_succeeded(
    clock, tmp_path, gatewaysim, monkeypatch
):
    gw = gatewaysim(tmp_path / "gw.db")
    gw.apply_migrations()
    service = _report_service(gw)

    def slow_build(*_: Any, **__: Any) -> dict[str, Any]:
        clock.now += 31.0  # the content is ready only after the deadline
        return {"summary": {"title": "Uji"}}

    monkeypatch.setattr(service, "_build", slow_build)
    monkeypatch.setattr(service, "_pdf", lambda content: b"%PDF-1.4 uji")
    monkeypatch.setattr(service, "_csv", lambda content: b"a,b\n1,2\n")
    with request_deadline(30), pytest.raises(RequestDeadlineExceeded):
        # the files and the row are written; only the final read-back for the (already answered) request
        # is refused
        service.generate("market_monthly", {}, "op", None, "req-deadline", formats=("pdf", "csv"))
    rows = _report_rows(gw)
    assert [r["status"] for r in rows] == ["succeeded"]
    keys = {rows[0]["object_key_pdf"], rows[0]["object_key_csv"]}
    assert keys == set(gw.objects)  # no file without its row


def test_an_audit_row_written_after_the_deadline_lands(clock, tmp_path, gatewaysim):
    from icp_api.storage.product import ProductRepository

    gw = gatewaysim(tmp_path / "gw.db")
    gw.apply_migrations()
    repo = ProductRepository(GatewayD1Executor(GATEWAY, gw.SECRET, transport=gw.transport))
    with request_deadline(30):
        clock.now += 45.0
        repo.audit("operator:tester", "model.promote", "success", target="icp_forecaster/3")
    rows = gw.d1.conn.execute("SELECT actor, action, outcome FROM audit_events").fetchall()
    assert [tuple(r) for r in rows] == [("operator:tester", "model.promote", "success")]


def test_kv_read_timeouts_are_retried_only_within_the_deadline(clock):
    def slow(request: httpx.Request) -> Exception:
        return httpx.ReadTimeout("slow", request=request)

    up = Upstream(clock, takes=30.0, answer=slow)
    with request_deadline(30), pytest.raises(RequestDeadlineExceeded):
        _kv(up).get("datasets/icp/a.csv")
    assert up.timeouts == [30.0]  # the old code retried twice more: up to 90.75 s for one read
    outside = Upstream(clock, takes=30.0, answer=slow)
    with pytest.raises(StorageError, match="after 3 attempts"):
        _kv(outside).get("datasets/icp/a.csv")
    assert outside.timeouts == [30.0, 30.0, 30.0]


def test_a_retry_that_fits_the_deadline_still_succeeds(clock):
    calls = {"n": 0}

    def flaky(_: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, json={})
        return httpx.Response(
            200, json={"success": True, "result": [{"success": True, "results": [{"ok": 1}]}]}
        )

    up = Upstream(clock, takes=1.0, answer=flaky)
    with request_deadline(30):
        rows = GatewayD1Executor(GATEWAY, SECRET, transport=httpx.MockTransport(up.handler)).query(
            "SELECT 1 AS ok"
        )
    assert rows == [{"ok": 1}]
    assert up.timeouts == [15.0, 15.0]  # plenty of time left: the configured timeout applies


def test_the_middleware_gives_each_request_the_configured_deadline(env_dir):
    app = FastAPI()
    seen: list[float | None] = []

    @app.get("/api/v1/probe")
    def probe() -> dict[str, bool]:  # a sync endpoint runs in a worker thread, like the real ones
        seen.append(remaining_request_seconds())
        return {"ok": True}

    settings = dataclasses.replace(load_settings(), request_timeout_seconds=30, require_gateway_secret=False)
    client = TestClient(RequestContextMiddleware(app, settings))
    assert client.get("/api/v1/probe").status_code == 200
    assert seen[0] is not None and 25 < seen[0] <= 30
    assert remaining_request_seconds() is None  # nothing leaks out of the request
