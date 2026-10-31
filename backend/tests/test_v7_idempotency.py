"""Idempotency-Key on POST /forecasts (audit v7 DATA-02)."""

from __future__ import annotations

from typing import Any

import pytest

API = "/api/v1"
PW = "Kata-sandi-uji-v7-0123"


@pytest.fixture(scope="module")
def tenant(client: Any, trained_version: str) -> dict[str, str]:
    op = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    client.post(
        f"{API}/models/versions/{trained_version}/promote",
        json={"reason": "idempotency tests", "force": True},
        headers=op,
    )
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Kunci Ulang (uji lokal v7)",
            "email": "owner@kunci-ulang.uji-lokal.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}", "org": r.json()["organization"]["id"]}


def _usage(client: Any, org: str) -> int:
    rows = client.app.state.container.db.query(
        "SELECT COUNT(*) AS n FROM usage_events WHERE org_id = ? AND action = 'forecast.run'", (org,)
    )
    return int(rows[0]["n"])


def _h(tenant: dict[str, str], key: str | None = None) -> dict[str, str]:
    h = {"Authorization": tenant["Authorization"]}
    if key:
        h["Idempotency-Key"] = key
    return h


def test_same_key_and_body_returns_the_original_run_without_new_usage(client, tenant):
    body = {"horizon": 1}
    first = client.post(f"{API}/forecasts", json=body, headers=_h(tenant, "fc-retry-0001"))
    assert first.status_code == 201, first.text
    used = _usage(client, tenant["org"])
    again = client.post(f"{API}/forecasts", json=body, headers=_h(tenant, "fc-retry-0001"))
    assert again.status_code == 201
    assert again.json()["id"] == first.json()["id"]
    assert again.headers.get("idempotent-replayed") == "true"
    assert _usage(client, tenant["org"]) == used


def test_same_key_with_a_different_body_is_rejected(client, tenant):
    client.post(f"{API}/forecasts", json={"horizon": 1}, headers=_h(tenant, "fc-retry-0002"))
    r = client.post(f"{API}/forecasts", json={"horizon": 2}, headers=_h(tenant, "fc-retry-0002"))
    assert r.status_code == 409 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_without_a_key_every_request_is_a_new_run(client, tenant):
    a = client.post(f"{API}/forecasts", json={"horizon": 1}, headers=_h(tenant))
    b = client.post(f"{API}/forecasts", json={"horizon": 1}, headers=_h(tenant))
    assert a.status_code == b.status_code == 201 and a.json()["id"] != b.json()["id"]


def test_a_key_is_scoped_to_its_principal(client, tenant, customer):
    mine = client.post(f"{API}/forecasts", json={"horizon": 1}, headers=_h(tenant, "fc-shared-0003"))
    other = client.post(
        f"{API}/forecasts", json={"horizon": 1}, headers={**customer, "Idempotency-Key": "fc-shared-0003"}
    )
    assert other.status_code == 201 and other.json()["id"] != mine.json()["id"]


def test_malformed_keys_are_rejected(client, tenant):
    r = client.post(f"{API}/forecasts", json={"horizon": 1}, headers=_h(tenant, "short"))
    assert r.status_code == 422


# --- DATA-05: export clipping fails closed -------------------------------------------------------------
def test_clip_export_without_a_step_column_derives_the_step_or_drops_the_row():
    from icp_api.routers.forecasts import clip_export

    legacy = b"period,prediction\r\n2026-10,90\r\n2026-11,91\r\n2026-12,92\r\nbogus,1\r\n"
    out = clip_export(legacy, 2, origin="2026-09").decode()
    assert "2026-10" in out and "2026-11" in out and "2026-12" not in out and "bogus" not in out
    # no origin known: nothing beyond the header is released
    assert clip_export(legacy, 2, origin=None).decode().strip() == "period,prediction"
    with_step = b"step,period\r\n1,2026-10\r\n3,2026-12\r\n"
    assert clip_export(with_step, 2).decode().count("2026-") == 1
