"""Provenance: evidence persistence, /data/verification, and agreement across surfaces."""

from __future__ import annotations

import csv
import io
from typing import Any

API = "/api/v1"
PW = "Verifikasi-Bukti-2026!"


def _owner(client: Any, slug: str) -> dict[str, str]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT Verifikasi {slug}",
            "email": f"owner@{slug}.verifikasi.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_evidence_is_recorded_for_unchanged_months_and_is_idempotent(client: Any):
    c = client.app.state.container
    active = c.datasets.active("icp")
    series = active.series
    periods = [str(p) for p in series.index[-3:]]
    ev = [
        {
            "period": p,
            "value": float(series[series.index[-3 + i]]),
            "kind": kind,
            "verification_status": "verified",
            "source_url": f"https://www.esdm.go.id/test/{kind}/{p}",
            "document_sha256": None,
            "reference": f"TEST/{p}",
            "retrieved_at": "2026-10-19T00:00:00Z",
        }
        for i, p in enumerate(periods)
        for kind in ("announcement", "decree_text")
    ]
    before = len(c.repo.all_evidence("icp"))
    r1 = c.datasets.ingest_series(
        "icp",
        series,
        source="test",
        source_detail="same values",
        actor="test",
        job_id=None,
        connector="test",
        provenance={},
        evidence=ev,
    )
    assert r1["committed"] is False  # values unchanged ...
    after = len(c.repo.all_evidence("icp"))
    assert after == before + len(ev)  # ... yet every month's evidence is stored
    c.datasets.ingest_series(
        "icp",
        series,
        source="test",
        source_detail="same values",
        actor="test",
        job_id=None,
        connector="test",
        provenance={},
        evidence=ev,
    )
    assert len(c.repo.all_evidence("icp")) == after  # idempotent
    by = c.verification.by_period("icp")
    assert all(by[p]["basis"] == "two_documents" and by[p]["documents"] == 2 for p in periods)


def test_verification_endpoint_is_plan_clipped_and_labelled(client: Any, admin: dict[str, str]):
    h = _owner(client, "klip")
    assert client.get(f"{API}/data/verification").status_code == 401
    r = client.get(f"{API}/data/verification", headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    floor = body["earliest_period"]
    assert body["history_years"] == 3 and floor
    assert body["months"] and all(m["period"] >= floor for m in body["months"])
    assert sum(body["counts"].values()) == len(body["months"])
    for m in body["months"]:
        assert m["label"] == body["legend"][m["basis"]]["label"]
        if m["basis"] == "two_documents":
            assert m["documents"] >= 2
        assert "Terverifikasi ganda" not in m["label"]["id"]
    full = client.get(f"{API}/data/verification", headers=admin).json()
    assert full["earliest_period"] is None and len(full["months"]) >= len(body["months"])
    brent = client.get(f"{API}/data/verification", params={"dataset": "brent"}, headers=h).json()
    assert brent["months"] and {m["basis"] for m in brent["months"]} == {"provider_published"}
    assert "ESDM" not in brent["months"][0]["label"]["en"]
    bad = client.get(f"{API}/data/verification", params={"dataset": "nope"}, headers=h)
    assert bad.status_code == 422


def test_provider_evidence_is_never_reported_as_verified(client: Any, admin: dict[str, str]):
    items = client.get(f"{API}/data/evidence", params={"dataset": "brent"}, headers=admin).json()["items"]
    assert all(i["verification_status"] != "verified" for i in items)


def test_provenance_report_matches_verification_month_by_month(client: Any, admin: dict[str, str]):
    """F-2b: the report used to read a wrong key and labelled every month legacy_unverified."""
    rep = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=admin)
    assert rep.status_code == 201, rep.text
    raw = client.get(f"{API}/reports/{rep.json()['id']}/download?format=csv", headers=admin).content
    rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    ver = client.get(f"{API}/data/verification", headers=admin).json()["months"]
    assert len(rows) == len(ver)
    assert [r["Dasar nilai"] for r in rows] == [m["label"]["id"] for m in ver]
    assert not any("legacy_unverified" in r["Dasar nilai"] for r in rows)
