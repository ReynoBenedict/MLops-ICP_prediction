"""Repository behaviour on both SQL transports (local SQLite and the D1 REST protocol)."""

from __future__ import annotations

import httpx
import pytest

from icp_api.storage.migrations import apply_local, current_version, expected_version
from icp_api.storage.objects import (
    GatewayObjectStore,
    LocalObjectStore,
    ObjectExists,
    ObjectNotFound,
    S3ObjectStore,
)
from icp_api.storage.repository import DuplicateActiveJob, InvalidCursor, MetadataRepository, new_id, utcnow
from icp_api.storage.sql import (
    D1HttpExecutor,
    GatewayD1Executor,
    IntegrityError,
    SqliteExecutor,
    StorageError,
    internal_url,
)

GATEWAY = "https://icp-gateway-staging.example.workers.dev"
SECRET = "test-gateway-secret-0123456789abcdefghijkl"


def _d1(sim, **kw) -> D1HttpExecutor:
    return D1HttpExecutor("acct", "db", "d1-token", transport=httpx.MockTransport(sim.handler), **kw)


def _gw(sim, **kw) -> GatewayD1Executor:
    return GatewayD1Executor(GATEWAY, SECRET, transport=sim.transport, **kw)


@pytest.fixture(params=["sqlite", "d1", "d1_gateway"])
def repo(request, tmp_path, d1sim, gatewaysim) -> MetadataRepository:
    if request.param == "sqlite":
        db = SqliteExecutor(tmp_path / "meta.db")
        apply_local(db)
    elif request.param == "d1":
        sim = d1sim(tmp_path / "d1.db")
        sim.apply_migrations()
        db = _d1(sim)
    else:
        gw = gatewaysim(tmp_path / "gw.db")
        gw.apply_migrations()
        db = _gw(gw)
    assert current_version(db) == expected_version()
    return MetadataRepository(db)


def _job(job_type="train", **kw):
    return {
        "id": new_id("job"),
        "job_type": job_type,
        "params": {"a": 1},
        "timeout_seconds": 60,
        "requested_by": "t",
        "created_at": utcnow(),
        **kw,
    }


def _dataset(sha="a" * 64, dataset="icp"):
    return {
        "id": new_id("dsv"),
        "dataset": dataset,
        "object_key": f"datasets/{dataset}/{sha}.csv",
        "sha256": sha,
        "size_bytes": 10,
        "row_count": 2,
        "period_start": "2024-01",
        "period_end": "2024-02",
        "source": "test",
        "quality": {"ok": True},
        "created_at": utcnow(),
        "created_by": "t",
    }


def test_dataset_versions_are_numbered_unique_and_single_active(repo):
    a = repo.insert_dataset_version(_dataset("a" * 64))
    b = repo.insert_dataset_version(_dataset("b" * 64))
    assert (a["version"], b["version"]) == (1, 2)
    with pytest.raises(IntegrityError):
        repo.insert_dataset_version(_dataset("a" * 64))  # same content twice
    repo.activate_dataset_version("icp", a["id"])
    repo.activate_dataset_version("icp", b["id"])
    assert repo.get_active_dataset("icp")["id"] == b["id"]
    assert repo.get_dataset_version(a["id"])["is_active"] == 0
    assert repo.get_active_dataset("wti") is None
    with pytest.raises(StorageError):
        repo.activate_dataset_version("wti", b["id"])  # wrong dataset


def test_one_active_job_per_type_and_lifecycle(repo):
    job = repo.create_job(_job())
    with pytest.raises(DuplicateActiveJob):
        repo.create_job(_job())
    repo.create_job(_job("ingest_wti"))  # other types are independent
    assert repo.mark_job_running(job["id"], "r1")
    assert not repo.mark_job_running(job["id"], "r2")  # no double start
    repo.request_job_cancel(job["id"])
    assert repo.heartbeat_job(job["id"]) is True
    assert repo.finish_job(job["id"], "cancelled")
    assert not repo.finish_job(job["id"], "succeeded")  # terminal states are final
    repo.create_job(_job())  # slot is free again


def test_idempotency_key_and_stale_reaping(repo):
    j = repo.create_job(_job(idempotency_key="key-12345678"))
    assert repo.find_job_by_idempotency_key("key-12345678")["id"] == j["id"]
    assert repo.fail_stale_jobs("9999-01-01T00:00:00.000Z", "gone") == 1
    assert repo.get_job(j["id"])["error_code"] == "INTERRUPTED"


def test_keyset_pagination(repo):
    for i in range(5):
        repo.audit("t", f"act{i}", "success")
    first = repo.list_audit(limit=2, cursor=None)
    second = repo.list_audit(limit=2, cursor=first.next_cursor)
    third = repo.list_audit(limit=2, cursor=second.next_cursor)
    seen = [e["id"] for p in (first, second, third) for e in p.items]
    assert len(seen) == len(set(seen)) == 5 and third.next_cursor is None
    with pytest.raises(InvalidCursor):
        repo.list_audit(limit=2, cursor="not-a-cursor")


def test_forecast_and_promotion_records_roundtrip(repo):
    fid = new_id("fc")
    repo.insert_forecast(
        {
            "id": fid,
            "created_at": utcnow(),
            "requested_by": "t",
            "status": "succeeded",
            "horizon": 2,
            "params": {"horizon": 2},
            "result": {"points": []},
        }
    )
    got = repo.get_forecast(fid)
    assert got["params"] == {"horizon": 2} and got["result"] == {"points": []}
    repo.insert_promotion(
        {
            "id": new_id("prm"),
            "created_at": utcnow(),
            "model_name": "M",
            "action": "promote",
            "from_version": None,
            "to_version": "1",
            "reason": "r",
            "actor": "t",
        }
    )
    assert repo.latest_promotion("M")["to_version"] == "1"


def test_d1_retries_transient_errors(tmp_path, d1sim):
    sim = d1sim(tmp_path / "d1.db", fail_first=2)
    db = _d1(sim)
    assert db.query("SELECT 1 AS ok") == [{"ok": 1}]
    assert sim.calls == 3


def test_d1_gives_up_and_maps_errors(tmp_path, d1sim):
    sim = d1sim(tmp_path / "d1.db", fail_first=10)
    with pytest.raises(StorageError):
        _d1(sim, max_attempts=2).query("SELECT 1")
    ok = d1sim(tmp_path / "d2.db")
    with pytest.raises(StorageError, match="no such table"):
        _d1(ok).query("SELECT * FROM missing")


def test_d1_timeouts_are_not_retried():
    calls = []

    def handler(request):
        calls.append(1)
        raise httpx.ReadTimeout("slow", request=request)

    db = D1HttpExecutor("a", "b", "t", transport=httpx.MockTransport(handler))
    with pytest.raises(StorageError, match="timed out"):
        db.execute("INSERT INTO x VALUES (1)")
    assert len(calls) == 1  # an ambiguous write must not be replayed


def test_gateway_d1_executor_uses_the_secret_and_the_same_envelope(tmp_path, gatewaysim):
    gw = gatewaysim(tmp_path / "gw.db", fail_first=2)
    db = _gw(gw)
    assert db.backend == "d1_gateway"
    assert db.query("SELECT 1 AS ok") == [{"ok": 1}]
    assert gw.d1.calls == 3  # transient 503s retried like the REST transport
    assert all(path == "/api/internal/d1/query" for _, path in gw.calls)
    wrong = GatewayD1Executor(GATEWAY, "x" * 40, transport=gw.transport, max_attempts=1)
    with pytest.raises(StorageError):
        wrong.query("SELECT 1")
    with pytest.raises(StorageError, match="wrangler"):
        db.executescript("CREATE TABLE t (x)")
    with pytest.raises(ValueError):
        GatewayD1Executor(GATEWAY, "")
    assert internal_url(GATEWAY + "/", "d1/query") == GATEWAY + "/api/internal/d1/query"
    assert internal_url(GATEWAY + "/api/internal", "objects/a/b") == GATEWAY + "/api/internal/objects/a/b"


def test_gateway_d1_executor_maps_integrity_errors(tmp_path, gatewaysim):
    gw = gatewaysim(tmp_path / "gw.db")
    gw.apply_migrations()
    repo = MetadataRepository(_gw(gw))
    repo.insert_dataset_version(_dataset("c" * 64))
    with pytest.raises(IntegrityError):
        repo.insert_dataset_version(_dataset("c" * 64))


# --- object stores ------------------------------------------------------------------------------
def test_gateway_object_store_matches_local_semantics(tmp_path, gatewaysim):
    gw = gatewaysim(tmp_path / "gw.db")
    store = GatewayObjectStore(GATEWAY, SECRET, transport=gw.transport)
    assert store.backend == "gateway"
    store.ping()
    assert not store.exists("datasets/icp/a.csv")
    store.put("datasets/icp/a.csv", b"x", "text/csv")
    with pytest.raises(ObjectExists):
        store.put("datasets/icp/a.csv", b"y", "text/csv")
    assert store.get("datasets/icp/a.csv") == b"x" and gw.objects["datasets/icp/a.csv"][1] == "text/csv"
    store.put("datasets/icp/a.csv", b"z", "text/csv", overwrite=True)
    assert store.get("datasets/icp/a.csv") == b"z" and store.exists("datasets/icp/a.csv")
    with pytest.raises(ObjectNotFound):
        store.get("nope.csv")
    for bad in ("../escape", "/abs", "a//b", "dir/"):
        with pytest.raises(ValueError):
            store.put(bad, b"", "text/plain")
    assert not any("escape" in path or "abs" in path for _, path in gw.calls)
    blob = bytes(range(256)) * 300
    store.put("models/bundles/5/model.skops", blob, "application/octet-stream")
    assert store.get("models/bundles/5/model.skops") == blob
    # Wrong secret, missing KV binding and connection failures surface as StorageError, never silently.
    with pytest.raises(StorageError, match="secret"):
        GatewayObjectStore(GATEWAY, "x" * 40, transport=gw.transport, max_attempts=1).exists("a.csv")
    nokv = gatewaysim(tmp_path / "gw2.db", kv=False)
    with pytest.raises(StorageError, match="KV"):
        GatewayObjectStore(GATEWAY, SECRET, transport=nokv.transport).ping()

    def down(request):
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(StorageError, match="unavailable"):
        GatewayObjectStore(GATEWAY, SECRET, transport=httpx.MockTransport(down), max_attempts=2).get("a.csv")


def test_local_object_store_is_write_once(tmp_path):
    store = LocalObjectStore(tmp_path / "obj")
    store.put("datasets/icp/a.csv", b"x", "text/csv")
    with pytest.raises(ObjectExists):
        store.put("datasets/icp/a.csv", b"y", "text/csv")
    assert store.get("datasets/icp/a.csv") == b"x"
    store.put("datasets/icp/a.csv", b"z", "text/csv", overwrite=True)
    assert store.get("datasets/icp/a.csv") == b"z" and store.exists("datasets/icp/a.csv")
    with pytest.raises(ObjectNotFound):
        store.get("nope.csv")
    for bad in ("../escape", "/abs", "a//b", "dir/"):
        with pytest.raises(ValueError):
            store.put(bad, b"", "text/plain")
    store.ping()


def test_s3_object_store_against_moto():
    moto = pytest.importorskip("moto")
    import boto3

    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="icp-test")
        store = S3ObjectStore("", "icp-test", "", "", client=client)
        store.ping()
        store.put("exports/forecasts/2026/10/fc_1.csv", b"a,b\n", "text/csv")
        assert store.get("exports/forecasts/2026/10/fc_1.csv") == b"a,b\n"
        assert store.exists("exports/forecasts/2026/10/fc_1.csv") and not store.exists("missing.csv")
        with pytest.raises(ObjectNotFound):
            store.get("missing.csv")
        with pytest.raises(ObjectExists):  # conditional PUT (If-None-Match: *)
            store.put("exports/forecasts/2026/10/fc_1.csv", b"other", "text/csv")
