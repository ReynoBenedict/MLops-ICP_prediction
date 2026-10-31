"""Plan history depth applies to every data path (history table and dataset downloads)."""

from __future__ import annotations

import csv
import io
from typing import Any

API = "/api/v1"
PW = "Riwayat-Paket-2026!"


def _first_period(dataset: str, text: str) -> str:
    rows = list(csv.DictReader(io.StringIO(text)))
    assert rows
    if dataset == "icp":
        return f"{int(rows[0]['year']):04d}-{int(rows[0]['month']):02d}"
    return str(rows[0]["period"])


def test_trial_dataset_downloads_are_clipped_like_history(client: Any, admin: dict[str, str]):
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Riwayat Uji",
            "email": "owner@riwayat-paket.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    h = {"Authorization": f"Bearer {r.json()['token']}"}
    hist = client.get(f"{API}/data/history", headers=h).json()
    floor = hist["earliest_period"]
    assert hist["history_years"] == 3 and floor
    active = client.get(f"{API}/datasets/active", headers=h).json()["datasets"]
    for ds in ("icp", "brent"):
        vid = active[ds]["active_version"]["id"]
        dl = client.get(f"{API}/datasets/versions/{vid}/download", headers=h)
        assert dl.status_code == 200
        assert dl.headers["cache-control"] == "private, no-store"
        assert f"_from-{floor}.csv" in dl.headers["content-disposition"]
        assert _first_period(ds, dl.text) >= floor
        full = client.get(f"{API}/datasets/versions/{vid}/download", headers=admin)
        assert full.status_code == 200 and _first_period(ds, full.text) < floor  # operators unclipped
        assert "_from-" not in full.headers["content-disposition"]
