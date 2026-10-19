"""API integration tests against real storage adapters, the real dataset and a really trained model.

Ordering matters for the model lifecycle section: tests before `trained_version` see no model.
"""

from __future__ import annotations

import csv
import io

import pytest

from icp_api.settings import ConfigError, _parse_admin_tokens, load_settings


# --- health, errors, security -----------------------------------------------------------------
def test_liveness_and_request_id(client):
    r = client.get("/health/live", headers={"X-Request-ID": "client-req-12345"})
    assert r.status_code == 200 and r.json() == {"status": "ok"}
    assert r.headers["x-request-id"] == "client-req-12345"
    generated = client.get("/health/live", headers={"X-Request-ID": "bad id!"}).headers["x-request-id"]
    assert generated.startswith("req_")


def test_readiness_reports_real_dependency_state_before_model(client):
    r = client.get("/health/ready")
    body = r.json()
    checks = {c["name"]: c for c in body["checks"]}
    assert r.status_code == 200 and body["status"] == "degraded"
    assert checks["metadata_store"]["status"] == "ok" and checks["object_store"]["status"] == "ok"
    assert checks["datasets"]["status"] == "ok"
    assert checks["production_model"]["status"] == "degraded"  # honest: nothing promoted yet


def test_forecast_without_model_is_503_and_recorded(client, admin, customer):
    r = client.post("/api/v1/forecasts", json={"horizon": 1}, headers=customer)
    assert r.status_code == 503 and r.json()["error"]["code"] == "MODEL_UNAVAILABLE"
    assert r.json()["error"]["request_id"] == r.headers["x-request-id"]
    # guests see no forecasts at all; members see their own runs (+ system forecasts); operators all
    assert client.get("/api/v1/forecasts").status_code == 401
    own = client.get("/api/v1/forecasts", params={"status": "failed"}, headers=customer).json()["items"]
    assert own and own[0]["error_code"] == "MODEL_UNAVAILABLE" and own[0]["org_id"]
    assert all(f["org_id"] == own[0]["org_id"] for f in own)
    failed = client.get("/api/v1/forecasts", params={"status": "failed"}, headers=admin).json()["items"]
    assert (
        failed and failed[0]["error_code"] == "MODEL_UNAVAILABLE" and failed[0]["export_available"] is False
    )


def test_validation_errors_use_the_envelope(client, customer):
    for body in (
        {"horizon": 0},
        {"horizon": 7},
        {"horizon": 1, "extra": 1},
        {"horizon": 1, "scenario": {"brent": {"2026-13": 50}}},
        {"horizon": 1, "scenario": {"wti": {"2026-04": -1}}},
        {"horizon": 1, "scenario": {"gold": {"2026-04": 1}}},
    ):
        r = client.post("/api/v1/forecasts", json=body, headers=customer)
        assert r.status_code == 422, body
        assert r.json()["error"]["code"] == "VALIDATION_ERROR"
    assert client.get("/api/v1/nope").json()["error"]["code"] == "NOT_FOUND"


def test_admin_endpoints_require_valid_token(client):
    assert client.get("/api/v1/jobs").status_code == 401
    r = client.get("/api/v1/jobs", headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401 and r.json()["error"]["code"] == "AUTH_INVALID"
    assert client.get("/api/v1/jobs", headers={"Authorization": "Basic abc"}).status_code == 401
    who = client.get("/api/v1/auth/whoami").json()
    assert (who["authenticated"], who["name"], who["role"], who["kind"]) == (False, None, "public", "public")


def test_denied_attempts_are_audited(client, admin):
    client.get("/api/v1/audit", headers={"Authorization": "Bearer wrong-token"})
    events = client.get("/api/v1/audit", params={"action": "auth.failed"}, headers=admin).json()["items"]
    assert events and events[0]["outcome"] == "denied" and "wrong-token" not in str(events)


def test_whoami_with_token(client, admin):
    who = client.get("/api/v1/auth/whoami", headers=admin).json()
    assert (who["authenticated"], who["name"], who["role"], who["kind"], who["horizon_max"]) == (
        True,
        "tester",
        "operator",
        "operator",
        6,
    )


def test_body_size_limit(client, admin):
    big = b"x" * 2_100_000
    r = client.post(
        "/api/v1/datasets/icp/imports", content=big, headers={**admin, "Content-Type": "text/csv"}
    )
    assert r.status_code == 413 and r.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"


def test_cors_allows_only_configured_origins(client):
    ok = client.options(
        "/api/v1/data/summary",
        headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"},
    )
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:5173"
    bad = client.options(
        "/api/v1/data/summary",
        headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"},
    )
    assert "access-control-allow-origin" not in bad.headers


def test_gateway_secret_and_rate_limit(env_dir, monkeypatch):
    from fastapi.testclient import TestClient

    from icp_api.main import create_app

    monkeypatch.setenv("REQUIRE_GATEWAY_SECRET", "true")
    monkeypatch.setenv("RATE_LIMIT_DEFAULT_PER_MINUTE", "3")
    app = create_app(load_settings(), enable_jobs=False)
    with TestClient(app) as c:
        assert c.get("/api/v1/data/summary").status_code == 403
        assert c.get("/health/live").status_code == 200  # probes stay reachable
        h = {
            "X-Gateway-Secret": "test-gateway-secret-0123456789abcdefghijkl",
            "CF-Connecting-IP": "203.0.113.9",
        }
        # a guest-accessible endpoint (the workspace endpoints now require sign-in)
        url = "/api/v1/public/market-snapshot"
        codes = [c.get(url, headers=h).status_code for _ in range(4)]
        assert codes == [200, 200, 200, 429]
        other = {**h, "CF-Connecting-IP": "203.0.113.10"}  # limits are per client
        assert c.get(url, headers=other).status_code == 200


def test_settings_validation_for_deployments(monkeypatch):
    with pytest.raises(ConfigError):
        _parse_admin_tokens("no-colon")
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(ConfigError) as exc:
        load_settings()
    msg = str(exc.value)
    assert "METADATA_BACKEND must be d1" in msg and "GATEWAY_SHARED_SECRET" in msg


def test_settings_accept_the_zero_cost_topology(monkeypatch):
    """Worker data plane (d1_gateway + gateway) with an exported bundle needs no MLflow/Postgres/R2."""
    secret = "z" * 40
    base = {
        "APP_ENV": "staging",
        "METADATA_BACKEND": "d1_gateway",
        "OBJECT_STORE_BACKEND": "gateway",
        "MODEL_SOURCE": "bundle",
        "GATEWAY_INTERNAL_URL": "https://icp-gateway-staging.example.workers.dev",
        "GATEWAY_SHARED_SECRET": secret,
        "REQUIRE_GATEWAY_SECRET": "true",
        "ADMIN_API_TOKENS": "ops:" + "t" * 40,
        "CORS_ALLOWED_ORIGINS": "https://icp-gateway-staging.example.workers.dev",
        "RENDER": "true",
        "PORT": "10000",
        "METRICS_TOKEN": "m" * 40,
    }
    for k, v in base.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    s = load_settings()
    assert (s.metadata_backend, s.object_store_backend, s.model_source) == ("d1_gateway", "gateway", "bundle")
    assert s.uses_gateway_data_plane and s.host == "0.0.0.0" and s.port == 10000
    # The same topology with MLflow serving still demands a persistent tracking store.
    monkeypatch.setenv("MODEL_SOURCE", "mlflow")
    with pytest.raises(ConfigError, match="MLFLOW_TRACKING_URI"):
        load_settings()
    monkeypatch.setenv("MODEL_SOURCE", "bundle")
    # /metrics must be token-protected once deployed (it bypasses the gateway secret).
    monkeypatch.setenv("METRICS_TOKEN", "")
    with pytest.raises(ConfigError, match="METRICS_TOKEN"):
        load_settings()
    monkeypatch.setenv("METRICS_TOKEN", base["METRICS_TOKEN"])
    # The data plane needs the Worker origin and a strong secret; local stores stay forbidden when deployed.
    monkeypatch.setenv("GATEWAY_INTERNAL_URL", "")
    with pytest.raises(ConfigError, match="GATEWAY_INTERNAL_URL"):
        load_settings()
    monkeypatch.setenv("GATEWAY_INTERNAL_URL", base["GATEWAY_INTERNAL_URL"])
    monkeypatch.setenv("GATEWAY_SHARED_SECRET", "short")
    with pytest.raises(ConfigError, match="32"):
        load_settings()
    monkeypatch.setenv("GATEWAY_SHARED_SECRET", secret)
    monkeypatch.setenv("OBJECT_STORE_BACKEND", "local")
    with pytest.raises(ConfigError, match="OBJECT_STORE_BACKEND must be s3"):
        load_settings()
    monkeypatch.setenv("OBJECT_STORE_BACKEND", "gateway")
    monkeypatch.setenv("MODEL_SOURCE", "dreams")
    with pytest.raises(ConfigError, match="MODEL_SOURCE"):
        load_settings()


# --- data -------------------------------------------------------------------------------------
def test_data_summary_history_quality_provenance(client, customer, admin):
    s = client.get("/api/v1/data/summary", headers=customer).json()
    icp = s["datasets"]["icp"]
    assert icp["available"] and icp["period_start"] == "2019-01" and icp["unit"] == "USD/bbl"
    assert "2024-07" in icp["missing_periods"] and icp["freshness"]["status"] in ("fresh", "lagging", "stale")
    assert 0.8 < s["relationships"]["wti"]["correlation"] <= 1
    assert s["datasets"]["brent"]["available"] is True and s["datasets"]["brent"]["provider"]
    assert s["icp_idr"]["icp_idr"] > 1_000_000 and s["icp_idr"]["period"] == icp["period_end"]
    hist = client.get(
        "/api/v1/data/history", params={"start": "2024-06", "end": "2024-08"}, headers=customer
    ).json()["points"]
    assert [p["period"] for p in hist] == ["2024-06", "2024-07", "2024-08"] and hist[1]["icp_price"] is None
    assert (
        hist[1]["icp_idr"] is None and hist[0]["icp_idr"] > 1_000_000
    )  # no ICP -> no rupiah equivalent is invented
    assert (
        client.get("/api/v1/data/history", params={"start": "2024-13"}, headers=customer).status_code == 422
    )
    q = client.get("/api/v1/data/quality", params={"dataset": "icp"}, headers=admin).json()
    assert q["report"]["ok"] and len(q["report"]["missing_periods"]) == 8
    assert client.get("/api/v1/data/quality", params={"dataset": "gold"}, headers=admin).status_code == 422
    act = client.get("/api/v1/datasets/active", headers=customer).json()["datasets"]
    assert (
        act["icp"]["active_version"]["source"] == "repository_seed"
        and act["icp"]["active_version"]["is_active"] is True
    )
    assert act["usd_idr"]["active_version"]["source"] == "repository_seed" and act["usd_idr"][
        "label_id"
    ].startswith("Kurs USD/IDR")
    ev = client.get("/api/v1/data/evidence", params={"dataset": "icp"}, headers=customer).json()
    assert ev["dataset"] == "icp" and isinstance(ev["items"], list)


def test_import_preview_guard_commit_and_download(client, admin, customer):
    csv_new = b"month,year,icp_price\n4,2026,99.5\n"
    url = "/api/v1/datasets/icp/imports"
    assert client.post(url, content=csv_new, headers={"Content-Type": "text/csv"}).status_code == 401
    assert (
        client.post(url, content=csv_new, headers={**admin, "Content-Type": "application/json"}).status_code
        == 415
    )
    preview = client.post(url, content=csv_new, headers={**admin, "Content-Type": "text/csv"}).json()
    assert preview["valid"] and not preview["committed"] and preview["diff"]["added"] == ["2026-04"]
    bad = client.post(
        url,
        params={"commit": "true"},
        content=b"month,year,icp_price\n4,2026,-3\n",
        headers={**admin, "Content-Type": "text/csv"},
    )
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "DATASET_INVALID"
    # Changing an existing month requires explicit confirmation.
    change = b"month,year,icp_price\n3,2026,101.0\n"
    r = client.post(
        url, params={"commit": "true"}, content=change, headers={**admin, "Content-Type": "text/csv"}
    )
    assert r.status_code == 409 and r.json()["error"]["code"] == "OVERWRITE_CONFIRMATION_REQUIRED"
    assert r.json()["error"]["details"]["diff"]["changed"][0]["period"] == "2026-03"
    # Replace mode that drops history is also guarded.
    r = client.post(
        url,
        params={"commit": "true", "mode": "replace"},
        content=csv_new,
        headers={**admin, "Content-Type": "text/csv"},
    )
    assert r.status_code == 409
    versions_before = client.get(
        "/api/v1/datasets/versions", params={"dataset": "icp"}, headers=admin
    ).json()["items"]
    original = next(v for v in versions_before if v["is_active"])
    committed = client.post(
        url, params={"commit": "true"}, content=csv_new, headers={**admin, "Content-Type": "text/csv"}
    ).json()
    assert committed["committed"] and committed["version"]["version"] == original["version"] + 1
    assert committed["version"]["parent_version_id"] == original["id"]
    # Restore the original version so later tests use the repository data.
    act = client.post(f"/api/v1/datasets/versions/{original['id']}/activate", headers=admin)
    assert act.status_code == 200 and act.json()["is_active"]
    dl = client.get(f"/api/v1/datasets/versions/{original['id']}/download", headers=customer)
    assert dl.status_code == 200 and dl.text.startswith("month,year,icp_price")
    # members download only active versions; operators any version; guests nothing
    inactive = committed["version"]["id"]
    assert client.get(f"/api/v1/datasets/versions/{inactive}/download", headers=customer).status_code == 404
    assert client.get(f"/api/v1/datasets/versions/{inactive}/download", headers=admin).status_code == 200
    assert client.get(f"/api/v1/datasets/versions/{original['id']}/download").status_code == 401
    audit = client.get("/api/v1/audit", params={"action": "dataset.icp.import"}, headers=admin).json()[
        "items"
    ]
    assert {e["outcome"] for e in audit} >= {"success", "failure"}


def test_dataset_pagination(client, admin):
    url = "/api/v1/datasets/versions"
    page = client.get(url, params={"limit": 1}, headers=admin).json()
    assert len(page["items"]) == 1 and page["next_cursor"]
    nxt = client.get(url, params={"limit": 1, "cursor": page["next_cursor"]}, headers=admin).json()
    assert nxt["items"][0]["id"] != page["items"][0]["id"]
    assert client.get(url, params={"cursor": "garbage"}, headers=admin).status_code == 400


# --- jobs ---------------------------------------------------------------------------------------
def _job(client, admin, **body):
    r = client.post("/api/v1/jobs", json=body, headers=admin)
    assert r.status_code == 202, r.text
    return client.app.state.jobs.wait(r.json()["id"], timeout=120)


def test_job_success_failure_and_transient_retries(client, admin):
    assert _job(client, admin, job_type="selftest")["status"] == "succeeded"
    failed = _job(client, admin, job_type="selftest", params={"mode": "fail"})
    assert (
        failed["status"] == "failed" and failed["error_code"] == "SELFTEST_FAILED" and failed["attempt"] == 1
    )
    transient = _job(client, admin, job_type="selftest", params={"mode": "transient"})
    assert transient["status"] == "failed" and transient["attempt"] == 2  # retried up to max_attempts
    retry = client.post(f"/api/v1/jobs/{failed['id']}/retry", headers=admin)
    assert retry.status_code == 202 and retry.json()["retry_of"] == failed["id"]
    client.app.state.jobs.wait(retry.json()["id"], 60)


def test_job_duplicate_guard_cancel_and_idempotency(client, admin):
    body = {"job_type": "selftest", "params": {"sleep_seconds": 20}, "idempotency_key": "idem-key-0001"}
    first = client.post("/api/v1/jobs", json=body, headers=admin).json()
    again = client.post("/api/v1/jobs", json=body, headers=admin).json()
    assert again["id"] == first["id"]  # idempotent resubmission
    dup = client.post("/api/v1/jobs", json={"job_type": "selftest"}, headers=admin)
    assert dup.status_code == 409 and dup.json()["error"]["details"]["active_job_id"] == first["id"]
    cancel = client.post(f"/api/v1/jobs/{first['id']}/cancel", headers=admin)
    assert cancel.status_code == 200
    done = client.app.state.jobs.wait(first["id"], 60)
    assert done["status"] == "cancelled"
    assert client.post(f"/api/v1/jobs/{first['id']}/cancel", headers=admin).status_code == 409


def test_job_params_are_validated(client, admin):
    r = client.post("/api/v1/jobs", json={"job_type": "train", "params": {"bogus": 1}}, headers=admin)
    assert r.status_code == 422
    assert client.post("/api/v1/jobs", json={"job_type": "nope"}, headers=admin).status_code == 422


# --- model lifecycle --------------------------------------------------------------------------
def test_training_registers_but_does_not_promote(client, trained_version, customer, admin):
    models = client.get("/api/v1/models", headers=customer).json()
    assert models["production_version"] is None
    v = next(m for m in models["versions"] if m["version"] == trained_version)
    assert v["metrics"]["mae"] > 0 and v["metrics"]["baseline_naive_last_mae"] > v["metrics"]["mae"]
    assert v["method"] and v["legacy"] is False and v["benchmarks_used"]
    assert {(h["mode"], h["horizon"]) for h in v["metrics"]["by_horizon"]} >= {
        ("observed", 1),
        ("hold_last", 1),
    }
    assert v["datasets"]["icp"]["version_id"].startswith("dsv_") and len(v["datasets"]["icp"]["sha256"]) == 64
    assert set(v["datasets"]) >= {"icp", "wti", "brent", "dubai", "usd_idr"}
    detail = client.get(f"/api/v1/models/versions/{trained_version}", headers=customer).json()
    assert detail["holdout_predictions"] and {"period", "actual", "predicted"} <= set(
        detail["holdout_predictions"][0]
    )
    assert detail["intervals"]["observed"]["1"]["0.8"][0] < 0 < detail["intervals"]["observed"]["1"]["0.8"][1]
    assert detail["model_card"] and detail["backtest_metrics"]
    runs = client.get("/api/v1/models/experiments/runs", params={"limit": 10}, headers=admin).json()["items"]
    assert {r["candidate"] for r in runs} >= {
        "naive_last",
        "spread_wti_12m",
        "spread_brent_6m",
        "ridge_small",
    }


def test_validate_promote_forecast_export_rollback(client, admin, trained_version):
    report = client.post(f"/api/v1/models/versions/{trained_version}/validate", headers=admin).json()
    names = {c["name"] for c in report["checks"]}
    assert {
        "has_method_spec",
        "signature_matches_features",
        "smoke_prediction",
        "beats_naive_baseline",
    } <= names
    promo = client.post(
        f"/api/v1/models/versions/{trained_version}/promote",
        json={"reason": "initial production model", "force": not report["passed"]},
        headers=admin,
    )
    assert promo.status_code == 200, promo.text
    assert client.get("/api/v1/models", headers=admin).json()["production_version"] == trained_version
    again = client.post(
        f"/api/v1/models/versions/{trained_version}/promote", json={"reason": "again please"}, headers=admin
    )
    assert again.status_code == 409

    run = client.post(
        "/api/v1/forecasts",
        json={"horizon": 4, "scenario": {"brent": {"2026-07": 80.0}, "wti": {"2026-07": 76.0}}},
        headers=admin,  # scenarios require a signed-in principal; operators may run them
    )
    assert run.status_code == 201, run.text
    fc = run.json()
    res = fc["result"]
    pts = res["points"]
    origin = fc["last_observed_period"]
    assert (
        fc["model_version"] == trained_version and fc["forecast_origin"] == origin == res["forecast_origin"]
    )
    assert (
        fc["issue_date"] == res["issue_date"]
        and fc["method"] == res["method"]
        and set(fc["dataset_versions"]) >= {"icp", "wti", "brent"}
    )
    assert [p["step"] for p in pts] == [1, 2, 3, 4] and pts[0]["period"] > origin
    assert pts[3]["period"] == "2026-07" and pts[3]["conditional_on_scenario"]
    used = next(b for b in ("brent", "wti") if pts[3]["benchmarks"][b]["used_by_model"])
    assert pts[3]["benchmarks"][used]["source"] == "scenario" and pts[3]["benchmarks"][used]["value"] in (
        80.0,
        76.0,
    )
    assert all(p["kind"] in ("estimate_awaiting_publication", "future_forecast") for p in pts)
    assert all(
        p["intervals"]["0.8"][0] < p["prediction"] < p["intervals"]["0.8"][1] for p in pts if p["intervals"]
    )
    assert pts[0]["prediction_idr"] and pts[0]["fx_rate"] > 1000
    exp = client.get(f"/api/v1/forecasts/{fc['id']}/export.csv", headers=admin)
    rows = list(csv.DictReader(io.StringIO(exp.text)))
    assert exp.headers["cache-control"] == "private, no-store"
    assert (
        exp.status_code == 200
        and len(rows) == 4
        and rows[0]["model_version"] == trained_version
        and rows[0]["kind"]
    )
    assert "attachment" in exp.headers["content-disposition"]
    assert client.get(f"/api/v1/forecasts/{fc['id']}", headers=admin).json()["id"] == fc["id"]

    beyond = client.post(
        "/api/v1/forecasts", json={"horizon": 2, "scenario": {"brent": {"2027-01": 70}}}, headers=admin
    )
    assert beyond.status_code == 422 and beyond.json()["error"]["code"] == "INVALID_FORECAST_INPUT"
    legacy_alias = client.post(
        "/api/v1/forecasts", json={"horizon": 1, "wti_overrides": {pts[0]["period"]: 70}}, headers=admin
    )
    assert legacy_alias.status_code == 201 and legacy_alias.json()["scenario"] == {
        "wti": {pts[0]["period"]: 70.0}
    }

    ready = client.get("/health/ready").json()
    assert {c["name"]: c["status"] for c in ready["checks"]}["production_model"] == "ok"
    drift = client.get("/api/v1/models/drift", headers=admin).json()
    assert (
        drift["model_version"] == trained_version
        and set(drift["features"])
        and drift["note"]
        and drift["method"]
    )

    # First promotion has no predecessor, so rollback is refused rather than guessed.
    rb = client.post("/api/v1/models/rollback", json={"reason": "test rollback"}, headers=admin)
    assert rb.status_code == 409 and rb.json()["error"]["code"] == "ROLLBACK_UNAVAILABLE"
    hist = client.get("/api/v1/models/promotions", headers=admin).json()["items"]
    assert hist[0]["to_version"] == trained_version and hist[0]["actor"] == "tester"


def test_retrain_compare_promote_and_rollback(client, admin, trained_version):
    r = client.post("/api/v1/jobs", json={"job_type": "train", "params": {}}, headers=admin)
    job = client.app.state.jobs.wait(r.json()["id"], 300)
    assert job["status"] == "succeeded", job
    v2 = str(job["result"]["registered_version"])
    cmp = client.get(
        "/api/v1/models/compare", params={"versions": f"{trained_version},{v2}"}, headers=admin
    ).json()
    assert [c["version"] for c in cmp] == [trained_version, v2]
    assert client.get("/api/v1/models/compare", params={"versions": "1"}, headers=admin).status_code == 422
    p = client.post(
        f"/api/v1/models/versions/{v2}/promote",
        json={"reason": "retrained on same data", "force": True},
        headers=admin,
    )
    assert p.status_code == 200 and p.json()["from_version"] == trained_version
    rb = client.post("/api/v1/models/rollback", json={"reason": "verify rollback path"}, headers=admin)
    assert rb.status_code == 200 and rb.json() == {
        "from_version": v2,
        "to_version": trained_version,
        "validation": None,
        "forced": False,
    }
    assert client.get("/api/v1/models", headers=admin).json()["production_version"] == trained_version
    actions = [e["action"] for e in client.get("/api/v1/audit", headers=admin).json()["items"]]
    assert "model.rollback" in actions and "model.promote" in actions


def test_system_status_and_metrics(client, admin, customer):
    assert client.get("/api/v1/system/status").status_code == 401
    denied = client.get("/api/v1/system/status", headers=customer)
    assert denied.status_code == 403 and denied.json()["error"]["code"] == "FORBIDDEN"
    st = client.get("/api/v1/system/status", headers=admin).json()
    assert st["metadata_backend"] == "sqlite" and st["schema_version"] == st["expected_schema_version"]
    assert st["admin_enabled"] is True
    metrics = client.get("/metrics").text
    assert "icp_forecast_runs_total" in metrics and "icp_http_requests_total" in metrics


def test_bundle_mode_serves_identical_forecasts_without_mlflow(client, admin, trained_version, monkeypatch):
    """MODEL_SOURCE=bundle: export the promoted version, serve it from the object store, refuse writes."""
    from fastapi.testclient import TestClient

    from icp_api.main import create_app
    from icp_ml.bundle import export_bundle

    c = client.app.state.container
    if c.models.registry.production_version() is None:
        p = client.post(
            f"/api/v1/models/versions/{trained_version}/promote",
            json={"reason": "bundle test", "force": True},
            headers=admin,
        )
        assert p.status_code == 200, p.text
    production = c.models.registry.production_version()
    out = export_bundle(c.models.registry, c.objects)  # local MLflow store -> the test object store
    assert out["version"] == production
    reference = client.post("/api/v1/forecasts", json={"horizon": 2}, headers=admin)
    assert reference.status_code == 201, reference.text

    monkeypatch.setenv("MODEL_SOURCE", "bundle")
    app = create_app(load_settings(), enable_jobs=True)
    assert app.state.container.models.bundle_mode
    with TestClient(app) as b:
        st = b.get("/api/v1/system/status", headers=admin).json()
        assert st["model_source"] == "bundle"
        checks = {ch["name"]: ch for ch in st["checks"]}
        assert checks["model_bundle"]["status"] == "ok" and checks["production_model"]["status"] == "ok"

        models = b.get("/api/v1/models", headers=admin).json()
        assert models["production_version"] == production
        assert [v["version"] for v in models["versions"]] == [production]
        detail = b.get(f"/api/v1/models/versions/{production}", headers=admin).json()
        assert detail["intervals"] and detail["holdout_predictions"] and detail["model_card"]
        assert b.get("/api/v1/models/experiments/runs", headers=admin).json()["items"] == []

        fc = b.post("/api/v1/forecasts", json={"horizon": 2}, headers=admin)
        assert fc.status_code == 201, fc.text
        a, bb = reference.json(), fc.json()
        assert bb["model_version"] == a["model_version"] == production and bb["method"] == a["method"]
        assert [p["prediction"] for p in bb["result"]["points"]] == [
            p["prediction"] for p in a["result"]["points"]
        ]
        assert [p["intervals"] for p in bb["result"]["points"]] == [
            p["intervals"] for p in a["result"]["points"]
        ]

        for call in (
            lambda: b.post(f"/api/v1/models/versions/{production}/validate", headers=admin),
            lambda: b.post(
                f"/api/v1/models/versions/{production}/promote",
                json={"reason": "refused in bundle mode"},
                headers=admin,
            ),
            lambda: b.post(
                "/api/v1/models/rollback", json={"reason": "refused in bundle mode"}, headers=admin
            ),
            lambda: b.post("/api/v1/jobs", json={"job_type": "train", "params": {}}, headers=admin),
        ):
            r = call()
            assert r.status_code == 409 and r.json()["error"]["code"] == "OPERATION_UNAVAILABLE", r.text
        # Read-only product paths and ingestion-type jobs stay available.
        assert b.get("/api/v1/public/market-snapshot").status_code == 200
        r = b.post("/api/v1/jobs", json={"job_type": "selftest", "params": {}}, headers=admin)
        assert r.status_code == 202, r.text
        assert b.app.state.jobs.wait(r.json()["id"], 60)["status"] == "succeeded"
