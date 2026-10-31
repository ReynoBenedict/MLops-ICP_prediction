"""Per-request read cache (storage.repository.request_read_cache): fewer metadata statements per request
(each one is an HTTP round trip on D1), identical responses."""

from __future__ import annotations

import contextlib
from typing import Any

import pytest

from icp_api.storage.repository import request_read_cache

API = "/api/v1"
VOLATILE = {"generated_at", "request_id"}


def _strip(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip(v) for k, v in obj.items() if k not in VOLATILE}
    if isinstance(obj, list):
        return [_strip(v) for v in obj]
    return obj


class _Counting:
    """Counts statements sent to the metadata store."""

    def __init__(self, db: Any):
        self.db = db
        self.statements: list[str] = []

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for name in ("query", "execute"):
            orig = getattr(self.db, name)

            def wrapped(sql: str, params: Any = (), _orig: Any = orig) -> Any:
                self.statements.append(" ".join(sql.split()))
                return _orig(sql, params)

            monkeypatch.setattr(self.db, name, wrapped)


def test_scope_reads_once_returns_copies_and_forgets_on_writes(client):
    repo = client.app.state.container.repo
    counter = _Counting(repo.db)
    with pytest.MonkeyPatch.context() as mp:
        counter.install(mp)
        outside = [repo.get_active_dataset("icp") for _ in range(3)]
        assert len(counter.statements) == 3  # no request scope: no caching (jobs, CLI)
        counter.statements.clear()
        with request_read_cache():
            first = repo.get_active_dataset("icp")
            first["version"] = -1  # a caller mutating its copy never affects other callers
            again = [repo.get_active_dataset(ds) for ds in ("icp", "wti", "icp")]
            runs = [repo.latest_ingestion_runs() for _ in range(3)]
            assert len(counter.statements) == 2  # one statement for all active versions, one for runs
            assert again[0] == outside[0] and again[2] == outside[0]
            assert again[1] == repo._one(
                "SELECT * FROM dataset_versions WHERE dataset = ? AND is_active = 1", ("wti",)
            )
            assert runs[0] == runs[2]
            counter.statements.clear()
            # a write in this process clears the scope: the next read sees the database again
            repo.activate_dataset_version("icp", outside[0]["id"])
            assert repo.get_active_dataset("icp") == outside[0]
            assert sum("is_active = 1" in s for s in counter.statements) == 1
        assert repo.get_active_dataset("icp") == outside[0]


READS = [
    "/public/market-snapshot",
    "/plans",
    "/data/summary",
    "/data/history",
    "/data/verification",
    "/datasets/active",
    "/forecasts?limit=10",
    "/insights?lang=id",
    "/insights?lang=en",
    "/org",
    "/scenarios",
    "/reports",
    "/alerts/events",
]


def test_responses_are_identical_with_and_without_the_cache(client, customer, admin, monkeypatch):
    def snapshot() -> dict[str, Any]:
        out = {}
        for path in READS:
            for who, h in (("member", customer), ("operator", admin)):
                r = client.get(f"{API}{path}", headers=h)
                out[f"{who} {path}"] = (r.status_code, _strip(r.json()))
        return out

    with_cache = snapshot()
    monkeypatch.setattr("icp_api.middleware.request_read_cache", contextlib.nullcontext)
    without = snapshot()
    assert with_cache.keys() == without.keys()
    for key in with_cache:
        assert with_cache[key] == without[key], key


def test_forecast_and_report_outputs_are_identical_with_and_without_the_cache(client, admin, monkeypatch):
    def run() -> tuple[Any, ...]:
        fc = client.post(f"{API}/forecasts", json={"horizon": 2}, headers=admin)
        result = fc.json().get("result") if fc.status_code == 201 else fc.json()["error"]["code"]
        rep = client.post(
            f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=admin
        )
        assert rep.status_code == 201, rep.text
        csv = client.get(f"{API}/reports/{rep.json()['id']}/download?format=csv", headers=admin).content
        return fc.status_code, _strip(result), csv, _strip(rep.json()["summary"])

    with_cache = run()
    monkeypatch.setattr("icp_api.middleware.request_read_cache", contextlib.nullcontext)
    assert run() == with_cache


def test_statements_per_request_are_bounded(client, customer, monkeypatch):
    """Before the cache: 12 statements for the snapshot and the summary, 13 for /datasets/active."""
    from icp_api.services.public_snapshot import SnapshotMemo

    # The snapshot is also memoised across requests (SEC-C-01): measure the request that computes it
    # (a fresh memo), then check that a repeat runs no statement at all.
    monkeypatch.setattr(client.app.state.container, "snapshot_memo", SnapshotMemo())
    counter = _Counting(client.app.state.container.repo.db)
    counter.install(monkeypatch)
    for path, most in (("/public/market-snapshot", 4), ("/data/summary", 3), ("/datasets/active", 4)):
        counter.statements.clear()
        assert client.get(f"{API}{path}", headers=customer).status_code == 200
        active_reads = [s for s in counter.statements if "dataset_versions" in s and "is_active = 1" in s]
        assert len(active_reads) == 1, (path, counter.statements)
        assert len(counter.statements) <= most, (path, counter.statements)
    counter.statements.clear()
    assert client.get(f"{API}/public/market-snapshot", headers=customer).status_code == 200
    assert counter.statements == []
