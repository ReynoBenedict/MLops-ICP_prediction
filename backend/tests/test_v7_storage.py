"""Ambiguous-write handling and key parity (audit v7: DATA-01, DATA-05)."""

from __future__ import annotations

import httpx
import pytest

from icp_api.storage.objects import GatewayObjectStore, ObjectExists, validate_key
from icp_api.storage.sql import D1HttpExecutor, StorageError

GATEWAY = "https://icp-gateway-staging.example.workers.dev"
SECRET = "test-gateway-secret-0123456789abcdefghijkl"


def _d1(sim) -> D1HttpExecutor:
    return D1HttpExecutor("acct", "db", "d1-token", transport=httpx.MockTransport(sim.handler))


@pytest.mark.parametrize("status", [502, 503, 504])
def test_d1_write_is_not_replayed_after_an_ambiguous_5xx(tmp_path, d1sim, status):
    sim = d1sim(tmp_path / "d1.db", fail_first=1, status_on_fail=status)
    with pytest.raises(StorageError, match="may or may not"):
        _d1(sim).execute("CREATE TABLE t (x INTEGER)")
    assert sim.calls == 1


def test_d1_write_is_retried_after_429_and_reads_after_any_transient_status(tmp_path, d1sim):
    sim = d1sim(tmp_path / "d1.db", fail_first=1, status_on_fail=429)
    _d1(sim).execute("CREATE TABLE t (x INTEGER)")
    assert sim.calls == 2
    reads = d1sim(tmp_path / "d2.db", fail_first=2, status_on_fail=503)
    assert _d1(reads).query("  select 1 AS ok") == [{"ok": 1}]
    assert reads.calls == 3


class _FlakyKv:
    """KV via the Worker that stores the value and then answers 503 once (ambiguous write)."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        key = request.url.path.split("/api/internal/objects/", 1)[1]
        if request.method == "PUT":
            self.puts += 1
            if request.headers.get("if-none-match") == "*" and key in self.objects:
                return httpx.Response(412, json={"error": {"code": "OBJECT_EXISTS"}})
            self.objects[key] = request.content
            if self.puts == 1:
                return httpx.Response(503, json={"error": {"code": "UPSTREAM"}})
            return httpx.Response(201, json={"key": key})
        if key in self.objects:
            return httpx.Response(200, content=self.objects[key])
        return httpx.Response(404)


def test_write_once_put_that_landed_before_a_5xx_is_a_success():
    kv = _FlakyKv()
    store = GatewayObjectStore(GATEWAY, SECRET, transport=httpx.MockTransport(kv.handler))
    store.put("exports/reports/2026/10/rpt_x.csv", b"a,b\n", "text/csv")
    assert kv.puts == 2 and kv.objects["exports/reports/2026/10/rpt_x.csv"] == b"a,b\n"


def test_write_once_conflict_with_different_content_is_still_reported():
    kv = _FlakyKv()
    kv.objects["datasets/icp/a.csv"] = b"other"
    kv.puts = 1  # no injected 5xx
    store = GatewayObjectStore(GATEWAY, SECRET, transport=httpx.MockTransport(kv.handler))
    with pytest.raises(ObjectExists):
        store.put("datasets/icp/a.csv", b"mine", "text/csv")


@pytest.mark.parametrize("key", ["reports/a.csv\n", "a\n", "datasets/icp/x.csv\r\n"])
def test_object_keys_with_trailing_newlines_are_rejected_like_the_worker(key):
    with pytest.raises(ValueError):
        validate_key(key)


def test_valid_keys_still_pass():
    assert validate_key("models/bundles/5/model.skops") == "models/bundles/5/model.skops"
