"""Verification labels <-> stored evidence, and transparent dissenting source readings (Agent D, v6)."""

from __future__ import annotations

import csv
import io
import json
from typing import Any

import pytest

from icp_api.services.provenance import (
    backfill_dissent,
    dissent_rows,
    month_checks,
    verify_provenance,
)
from icp_api.services.provenance import (
    main as provenance_main,
)
from icp_api.services.verification import LEGEND, classify_icp_month, dissent_view
from icp_ml.sources.esdm_icp import ReconciledMonth

API = "/api/v1"
PW = "Sumber-Berbeda-2026!"


def _ev(
    kind: str, value: float, status: str = "verified", url: str | None = None, **kw: Any
) -> dict[str, Any]:
    return {
        "kind": kind,
        "value": value,
        "verification_status": status,
        "source_url": url or f"https://www.esdm.go.id/{kind}/{value}",
        "document_sha256": kw.get("sha"),
        "reference": kw.get("reference"),
        "created_at": kw.get("created_at", "2026-10-19T00:00:00Z"),
        "note": kw.get("note"),
    }


# -- pure model -----------------------------------------------------------------------------------
def test_dissent_rows_do_not_change_the_month_basis():
    rows = [_ev("decree_ocr", 68.31, sha="a"), _ev("announcement_backref", 68.31, url="u2")]
    base = classify_icp_month(68.31, rows)
    with_dissent = classify_icp_month(
        68.31,
        [*rows, _ev("announcement", 63.81, "dissent", created_at="2026-10-21T00:00:00Z", note="outvoted")],
    )
    assert base == {**with_dissent}
    assert base["basis"] == "two_documents"


def test_dissent_view_explains_without_overclaiming():
    rows = [
        _ev("decree_ocr", 68.31),
        _ev(
            "announcement",
            63.81,
            "dissent",
            reference="ICP April 2019",
            note="announcement differs (63.81) - outvoted by 2 agreeing sources",
        ),
        _ev("legacy_dataset", 76.16, "rejected", note="superseded by verified official value 81.8"),
        _ev("announcement", 68.31, "dissent"),  # equal to the value: not a dissent, ignored
    ]
    view = dissent_view(68.31, rows, include_urls=False)
    assert [d["value"] for d in view["dissent"]] == [63.81, 76.16]
    assert all(d["source_url"] is None for d in view["dissent"])
    for d in view["dissent"]:
        for lang in ("id", "en"):
            assert d["reason"][lang] and d["kind_label"][lang]
            low = d["reason"][lang].lower()
            assert "salah" not in low and "error" not in low and "terverifikasi" not in low
    lab = view["dissent_label"]
    assert "63,81" in lab["id"] and "63.81" in lab["en"] and "2" in lab["en"]
    assert "2 other readings" in lab["en"] and "(s)" not in lab["en"]
    one = dissent_view(68.31, rows[:2], include_urls=False)["dissent_label"]
    assert "1 other reading (" in one["en"]
    ops = dissent_view(68.31, rows, include_urls=True)
    assert ops["dissent"][0]["source_url"].startswith("https://")
    assert dissent_view(68.31, rows[:1], include_urls=False) == {"dissent": [], "dissent_label": None}


def _month(
    period: str, value: float | None, status: str, sources: list[dict[str, Any]], note: str = ""
) -> ReconciledMonth:
    return ReconciledMonth(period, value, status, sources, note)


def _src(kind: str, value: float, url: str) -> dict[str, Any]:
    return {"kind": kind, "value": value, "url": url, "sha256": None, "reference": None}


RECON = [
    _month(
        "2019-04",
        68.31,
        "verified",
        [
            _src("decree_ocr", 68.31, "d"),
            _src("announcement_backref", 68.31, "b"),
            _src("announcement", 63.81, "a"),
        ],
        "announcement differs (63.81) - outvoted by 2 agreeing sources",
    ),
    _month("2018-12", 54.81, "verified", [_src("announcement", 54.81, "x")]),  # outside the active range
    _month("2019-05", 60.0, "verified", [_src("decree_text", 60.0, "t")]),
]


def test_dissent_rows_come_only_from_months_in_the_active_series():
    import pandas as pd

    series = pd.Series([68.31, 60.0], index=pd.PeriodIndex(["2019-04", "2019-05"], freq="M"))
    rows, stats = dissent_rows(RECON, series, "dsv_x")
    assert [(r["period"], r["kind"], r["value"]) for r in rows] == [("2019-04", "announcement", 63.81)]
    assert rows[0]["verification_status"] == "dissent" and rows[0]["dataset_version_id"] == "dsv_x"
    assert "outvoted" in rows[0]["note"]
    assert stats == {"dissent": 1, "outside_active_range": 1}


@pytest.mark.parametrize(
    ("basis_rows", "value", "ok"),
    [
        ([_ev("announcement", 70.0, sha="a"), _ev("decree_ocr", 70.0, sha="b")], 70.0, True),
        ([_ev("announcement", 71.0, sha="a")], 70.0, False),  # no stored reading supports the value
    ],
)
def test_month_checks_flag_values_without_supporting_evidence(basis_rows, value, ok):
    shown = classify_icp_month(value, basis_rows)
    issues = month_checks(
        "2020-01", value, basis_rows, {**shown, "label": LEGEND[shown["basis"]]["label"]}, None
    )
    assert (issues == []) is ok


def test_month_checks_detect_a_label_that_disagrees_with_evidence():
    rows = [_ev("announcement", 70.0, "single_source")]
    shown = {
        "basis": "two_documents",
        "status": "verified",
        "documents": 2,
        "label": LEGEND["two_documents"]["label"],
    }
    assert any("basis" in i for i in month_checks("2020-01", 70.0, rows, shown, None))


# -- local data model (session store) ---------------------------------------------------------------
@pytest.fixture(scope="module")
def recon_state(client: Any, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    c = client.app.state.container
    series = c.datasets.active("icp").series
    p = series.index[-5]
    v = float(series[p])
    state = tmp_path_factory.mktemp("esdm")
    months = [
        {
            "period": str(p),
            "value": v,
            "status": "verified",
            "sources": [
                {"kind": "decree_text", "value": v, "url": "https://www.esdm.go.id/kepmen", "sha256": "s1"},
                {"kind": "announcement", "value": round(v + 4.5, 2), "url": "https://migas.esdm.go.id/p"},
            ],
            "note": f"announcement differs ({round(v + 4.5, 2)}) - outvoted by 2 agreeing sources",
        }
    ]
    (state / "reconciliation.json").write_text(json.dumps(months), encoding="utf-8")
    return {"state": state, "period": str(p), "value": v, "dissent": round(v + 4.5, 2)}


def test_backfill_dissent_is_additive_and_idempotent(client, recon_state):
    c = client.app.state.container
    before = c.repo.all_evidence("icp")
    dry = backfill_dissent(c, recon_state["state"], dry_run=True)
    assert dry["would_insert"] == 1 and len(c.repo.all_evidence("icp")) == len(before)
    first = backfill_dissent(c, recon_state["state"], dry_run=False)
    second = backfill_dissent(c, recon_state["state"], dry_run=False)
    assert first["inserted"] == 1 and second["inserted"] == 0
    after = c.repo.all_evidence("icp")
    assert len(after) == len(before) + 1
    old_keys = {c.repo.evidence_key(e) for e in before}
    assert old_keys <= {c.repo.evidence_key(e) for e in after}  # nothing removed or changed


def _owner(client: Any) -> dict[str, str]:
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": "PT Sumber Berbeda (uji lokal)",
            "email": "owner@sumber-berbeda.test",
            "name": "U",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_verification_endpoint_surfaces_dissent(client, admin, recon_state):
    c = client.app.state.container
    backfill_dissent(c, recon_state["state"], dry_run=False)
    h = _owner(client)
    cust = client.get(f"{API}/data/verification", headers=h).json()
    month = next(m for m in cust["months"] if m["period"] == recon_state["period"])
    assert [d["value"] for d in month["dissent"]] == [recon_state["dissent"]]
    assert month["dissent"][0]["source_url"] is None and month["dissent_label"]["id"]
    assert cust["dissent_months"] >= 1
    ops = client.get(f"{API}/data/verification", headers=admin).json()
    om = next(m for m in ops["months"] if m["period"] == recon_state["period"])
    assert om["dissent"][0]["source_url"] == "https://migas.esdm.go.id/p"


def test_provenance_report_lists_dissent(client, recon_state):
    c = client.app.state.container
    backfill_dissent(c, recon_state["state"], dry_run=False)
    op = {"Authorization": f"Bearer {c.settings.admin_tokens['tester']}"}  # operator: no plan clip
    r = client.post(f"{API}/reports", json={"kind": "data_provenance", "formats": ["csv"]}, headers=op)
    assert r.status_code == 201, r.text
    text = client.get(f"{API}/reports/{r.json()['id']}/download?format=csv", headers=op).content.decode(
        "utf-8-sig"
    )
    rows = {row["Bulan"]: row for row in csv.DictReader(io.StringIO(text))}
    assert "Sumber berbeda" in next(iter(rows.values()))
    shown = f"{recon_state['dissent']:.2f}".replace(".", ",")
    assert any(shown in r["Sumber berbeda"] for r in rows.values())


def test_verify_provenance_agrees_month_by_month_on_the_store(client, recon_state, capsys):
    c = client.app.state.container
    backfill_dissent(c, recon_state["state"], dry_run=False)
    report = verify_provenance(c, recon_state["state"])
    assert report["months"] == len(c.datasets.active("icp").series)
    assert report["disagreements"] == [], report["disagreements"][:5]
    assert report["dissent_months"] >= 1
    surfaces = report["surfaces"]
    assert surfaces["verification_api"] == surfaces["provenance_report"] == surfaces["insight_counts"]


def test_cli_entry_points(client, recon_state, capsys, monkeypatch):
    c = client.app.state.container
    monkeypatch.setattr("icp_api.services.provenance._container", lambda: c)
    assert provenance_main(["verify-provenance", "--state-dir", str(recon_state["state"])]) == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["disagreements"] == 0 and out["months"] > 0
    assert provenance_main(["backfill-dissent", "--dry-run", "--state-dir", str(recon_state["state"])]) == 0
    assert "would_insert" in capsys.readouterr().out


# -- readings stored before v6 with the month's status ("verified") although they differ -----------
def test_live_rows_that_differ_from_the_value_are_shown_as_dissent():
    rows = [
        _ev("announcement", 81.8, sha="a"),
        _ev("decree_ocr", 81.8, sha="b"),
        _ev("announcement_backref", 81.52, note="referenced as previous month in 2021-11 announcement"),
    ]
    assert classify_icp_month(81.8, rows)["basis"] == "two_documents"
    view = dissent_view(81.8, rows, include_urls=False)
    assert [(d["kind"], d["value"]) for d in view["dissent"]] == [("announcement_backref", 81.52)]


def test_ingestion_marks_outvoted_readings_as_dissent():
    from icp_api.services.icp_official import OfficialIcpIngestion

    m = RECON[0]
    _, evidence, _ = OfficialIcpIngestion.accepted_series(None, [m])  # type: ignore[arg-type]
    status = {(e["extraction_method"], e["value"]): e["verification_status"] for e in evidence}
    assert status[("announcement", 63.81)] == "dissent"
    assert status[("decree_ocr", 68.31)] == "verified"


def test_evidence_endpoint_relabels_differing_icp_readings(client, admin):
    c = client.app.state.container
    series = c.datasets.active("icp").series
    p = series.index[-7]
    c.repo.insert_evidence(
        [
            {
                "dataset": "icp",
                "period": str(p),
                "value": float(series[p]) + 0.3,
                "kind": "announcement_backref",
                "verification_status": "verified",
                "source_url": "https://migas.esdm.go.id/backref-test",
                "retrieved_at": "2026-10-16T00:00:00Z",
            }
        ]
    )
    items = client.get(f"{API}/data/evidence?dataset=icp&period={p}", headers=admin).json()["items"]
    hit = [e for e in items if e["source_url"] == "https://migas.esdm.go.id/backref-test"]
    assert hit and hit[0]["verification_status"] == "dissent"
