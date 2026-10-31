"""Wave C requests applied in Wave A files: Idempotency-Key on POST /reports (DATA-02), report issue
date in Jakarta time (ML-04) and the maintained WTI seed as default (DATA-06)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

API = "/api/v1"
PW = "Laporan-Kunci-v7-2026!"
BODY = {"kind": "market_monthly", "formats": ["csv"]}


@pytest.fixture(scope="module")
def tenant(client: Any) -> dict[str, str]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Laporan Ulang (uji lokal v7)",
            "email": "owner@laporan-ulang.uji-lokal.test",
            "name": "L",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}", "org": r.json()["organization"]["id"]}


def _h(tenant: dict[str, str], key: str | None = None) -> dict[str, str]:
    h = {"Authorization": tenant["Authorization"]}
    if key:
        h["Idempotency-Key"] = key
    return h


def _usage(client: Any, org: str) -> int:
    rows = client.app.state.container.db.query(
        "SELECT COUNT(*) AS n FROM usage_events WHERE org_id = ? AND action = 'report.generate'", (org,)
    )
    return int(rows[0]["n"])


def test_report_same_key_and_body_replays_without_new_usage(client, tenant):
    first = client.post(f"{API}/reports", json=BODY, headers=_h(tenant, "rpt-retry-0001"))
    assert first.status_code == 201, first.text
    used = _usage(client, tenant["org"])
    again = client.post(f"{API}/reports", json=BODY, headers=_h(tenant, "rpt-retry-0001"))
    assert again.status_code == 201
    assert again.json()["id"] == first.json()["id"]
    assert again.headers.get("idempotent-replayed") == "true"
    assert _usage(client, tenant["org"]) == used


def test_report_same_key_with_a_different_body_is_rejected(client, tenant):
    client.post(f"{API}/reports", json=BODY, headers=_h(tenant, "rpt-retry-0002"))
    other = {"kind": "data_provenance", "formats": ["csv"]}
    r = client.post(f"{API}/reports", json=other, headers=_h(tenant, "rpt-retry-0002"))
    assert r.status_code == 409 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_report_without_a_key_is_always_new_and_keys_are_per_principal(client, tenant, customer):
    a = client.post(f"{API}/reports", json=BODY, headers=_h(tenant))
    b = client.post(f"{API}/reports", json=BODY, headers=_h(tenant))
    assert a.status_code == b.status_code == 201 and a.json()["id"] != b.json()["id"]
    mine = client.post(f"{API}/reports", json=BODY, headers=_h(tenant, "rpt-shared-0003"))
    theirs = client.post(
        f"{API}/reports", json=BODY, headers={**customer, "Idempotency-Key": "rpt-shared-0003"}
    )
    assert theirs.status_code == 201 and theirs.json()["id"] != mine.json()["id"]
    assert client.post(f"{API}/reports", json=BODY, headers=_h(tenant, "short")).status_code == 422


def _stored_with_key(client: Any, org: str, key: str) -> int:
    rows = client.app.state.container.db.query(
        "SELECT COUNT(*) AS n FROM report_runs WHERE org_id = ? "
        "AND json_extract(params_json, '$.idempotency_key') = ?",
        (org, key),
    )
    return int(rows[0]["n"])


def test_report_retry_while_the_first_attempt_runs_is_refused_not_duplicated(client, tenant, monkeypatch):
    """SEC-C-03: the report row is stored only when generation ends, and the request deadline answers 504
    while the generation goes on. A retry with the same key in that window gets 409
    IDEMPOTENCY_IN_PROGRESS (no second report, no second usage event); after the first attempt ends the
    retry replays its report."""
    import threading

    reports = client.app.state.container.reports
    real = reports.generate
    started, release = threading.Event(), threading.Event()

    def slow(*args: Any, **kwargs: Any) -> Any:
        started.set()
        assert release.wait(60)
        return real(*args, **kwargs)

    monkeypatch.setattr(reports, "generate", slow)
    key = "rpt-inflight-0004"
    out: dict[str, Any] = {}
    first = threading.Thread(
        target=lambda: out.setdefault(
            "first", client.post(f"{API}/reports", json=BODY, headers=_h(tenant, key))
        )
    )
    first.start()
    try:
        assert started.wait(60)
        used = _usage(client, tenant["org"])
        retry = client.post(f"{API}/reports", json=BODY, headers=_h(tenant, key))
        assert retry.status_code == 409, retry.text
        assert retry.json()["error"]["code"] == "IDEMPOTENCY_IN_PROGRESS"
        assert retry.json()["error"]["details"] == {"retry_after_seconds": 15}
        assert retry.headers.get("retry-after") == "15"
    finally:
        release.set()
        first.join(120)
    assert out["first"].status_code == 201, out["first"].text
    assert _usage(client, tenant["org"]) == used + 1  # the first attempt only
    replay = client.post(f"{API}/reports", json=BODY, headers=_h(tenant, key))
    assert replay.status_code == 201 and replay.json()["id"] == out["first"].json()["id"]
    assert replay.headers.get("idempotent-replayed") == "true"
    assert _stored_with_key(client, tenant["org"], key) == 1


def test_report_reservation_is_per_caller_and_key_and_always_released(client):
    from icp_api.errors import ApiError

    reports = client.app.state.container.reports
    with reports.reserve_idempotency("rpt-slot-0001", "alice", "org_a"):
        with pytest.raises(ApiError) as busy:
            with reports.reserve_idempotency("rpt-slot-0001", "alice", "org_a"):
                pass
        assert busy.value.status == 409 and busy.value.code == "IDEMPOTENCY_IN_PROGRESS"
        # another key, another caller, another organisation and an operator (no org) are independent
        for key, actor, org in (
            ("rpt-slot-0002", "alice", "org_a"),
            ("rpt-slot-0001", "bob", "org_a"),
            ("rpt-slot-0001", "alice", "org_b"),
            ("rpt-slot-0001", "alice", None),
        ):
            with reports.reserve_idempotency(key, actor, org):
                pass
    with pytest.raises(RuntimeError):  # a failed attempt frees its key for the next one
        with reports.reserve_idempotency("rpt-slot-0001", "alice", "org_a"):
            raise RuntimeError("generation failed")
    with reports.reserve_idempotency("rpt-slot-0001", "alice", "org_a"):
        pass


def test_report_issue_date_is_the_jakarta_date(client, monkeypatch):
    import icp_ml.sources.base as base

    class Frozen(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
            fixed = datetime(2026, 9, 30, 20, 0, tzinfo=timezone.utc)  # 2026-10-01 03:00 WIB
            return fixed if tz is None else fixed.astimezone(tz)

    monkeypatch.setattr(base, "datetime", Frozen)
    common = client.app.state.container.reports._common()
    assert common["issue_date"] == "2026-10-01"


def test_wti_seed_default_is_the_maintained_series(monkeypatch):
    from icp_api.settings import load_settings

    monkeypatch.setenv("APP_ENV", "test")  # never read a local .env here
    monkeypatch.delenv("SEED_WTI_PATH", raising=False)
    assert load_settings().seed_wti_path.parts[-2:] == ("seed", "wti.csv")
