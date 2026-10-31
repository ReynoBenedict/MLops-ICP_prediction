"""Alert events carry the subject period, a deep link and English text; kinds/plans have English labels."""

from __future__ import annotations

from typing import Any

API = "/api/v1"
PW = "Peringatan-Label-2026!"


def test_icp_published_event_semantics_and_english_labels(client: Any):
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Peringatan Label",
            "email": "owner@peringatan-label.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    h = {"Authorization": f"Bearer {r.json()['token']}"}
    kinds = client.get(f"{API}/alerts/kinds", headers=h).json()
    assert kinds and all(k["label_en"] and k["label_id"] for k in kinds)
    plans = client.get(f"{API}/plans").json()
    assert all(p["price_note_en"] for p in plans)
    rule = client.post(f"{API}/alerts/rules", json={"kind": "icp_published"}, headers=h)
    assert rule.status_code == 201 and rule.json()["label_en"] == "New official ICP month detected"
    events = client.get(f"{API}/alerts/events", headers=h).json()["items"]
    ev = next(e for e in events if e["rule_id"] == rule.json()["id"])
    last = str(client.app.state.container.datasets.active("icp").series.index.max())
    assert ev["period"] == last and ev["link"] == {"kind": "period", "period": last, "dataset": "icp"}
    assert "terdeteksi" in ev["title"] and "detected" in ev["title_en"]
    assert "waktu deteksi" in ev["body"] and "detection time" in ev["body_en"]
    assert "_meta" not in (ev["data"] or {})
    rules = client.get(f"{API}/alerts/rules", headers=h).json()
    assert rules[0]["label_en"]
