"""Public market snapshot extension (contract C1) and the decree-number repair (2026-10-25).

Pure functions are tested directly; endpoint behaviour runs against an isolated store (own SQLite file,
object directory and seed files), so nothing here changes the shared session store.
"""

from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from icp_api.services.public_snapshot import (
    coverage,
    latest_evidence,
    recent_months,
    trusted_decree_references,
)
from icp_api.settings import ConfigError, load_settings
from icp_api.storage.repository import new_id, utcnow

API = "/api/v1"
SENTINEL = 777.123456  # forecast value that must never reach a guest
FORECAST_KEYS = {"points", "prediction", "predictions", "lower", "upper", "interval", "intervals", "result"}


def _series(values: dict[str, float]) -> pd.Series:
    return pd.Series(list(values.values()), index=pd.PeriodIndex(list(values), freq="M"), dtype=float)


def _keys(obj: Any) -> set[str]:
    if isinstance(obj, dict):
        return set(obj) | {k for v in obj.values() for k in _keys(v)}
    if isinstance(obj, list):
        return {k for v in obj for k in _keys(v)}
    return set()


# -- recent ---------------------------------------------------------------------------------------
def test_recent_marks_gaps_as_null_never_zero():
    icp = _series({"2024-01": 80.0, "2024-02": 81.5, "2024-04": 83.0})  # 2024-03 not published
    brent = _series({"2024-01": 82.0, "2024-03": 84.0, "2024-04": 85.0, "2024-05": 86.0})
    out = recent_months(icp, brent, 24)
    # never earlier than the first official month, ends at the latest official month (not Brent's)
    assert out == [
        {"period": "2024-01", "icp_usd": 80.0, "brent_usd": 82.0},
        {"period": "2024-02", "icp_usd": 81.5, "brent_usd": None},
        {"period": "2024-03", "icp_usd": None, "brent_usd": 84.0},
        {"period": "2024-04", "icp_usd": 83.0, "brent_usd": 85.0},
    ]
    assert recent_months(icp, brent, 2) == out[-2:]
    assert recent_months(icp, None, 1) == [{"period": "2024-04", "icp_usd": 83.0, "brent_usd": None}]
    # a NaN observation is a gap as well, and never the "latest official month"
    with_nan = _series({"2024-01": 80.0, "2024-02": float("nan"), "2024-03": 82.0, "2024-04": float("nan")})
    assert recent_months(with_nan, None, 3) == [
        {"period": "2024-01", "icp_usd": 80.0, "brent_usd": None},
        {"period": "2024-02", "icp_usd": None, "brent_usd": None},
        {"period": "2024-03", "icp_usd": 82.0, "brent_usd": None},
    ]


def test_recent_withholds_a_month_whose_documents_disagree():
    """A month with verification basis "conflict" is "not used until the difference is resolved": guests
    get null for it, never the disputed dataset value (BE-R9)."""
    icp = _series({"2024-01": 80.0, "2024-02": 81.5, "2024-03": 82.0})
    out = recent_months(icp, None, 3, withheld={"2024-02"})
    assert [m["icp_usd"] for m in out] == [80.0, None, 82.0]
    assert recent_months(icp, None, 3) == [
        {"period": p, "icp_usd": v, "brent_usd": None}
        for p, v in (("2024-01", 80.0), ("2024-02", 81.5), ("2024-03", 82.0))
    ]


def test_recent_disabled_or_without_icp_is_empty():
    icp = _series({"2024-01": 80.0})
    assert recent_months(icp, None, 0) == []
    assert recent_months(None, icp, 24) == []
    assert recent_months(_series({}), icp, 24) == []


def test_coverage_counts_the_confirmed_verification_bases():
    icp = _series({"2024-01": 80.0, "2024-02": 81.5, "2024-04": 83.0})
    months = [
        {"period": "2024-01", "value": 80.0, "basis": "two_documents"},
        {"period": "2024-02", "value": 81.5, "basis": "decree_text"},
        {"period": "2024-04", "value": 83.0, "basis": "single_document"},
    ]
    assert coverage(icp, months) == {
        "first_period": "2024-01",
        "last_period": "2024-04",
        "months": 3,
        "confirmed_months": 2,
    }
    assert coverage(None, months) is None
    assert coverage(_series({}), []) is None


# -- evidence -------------------------------------------------------------------------------------
def _row(kind: str, value: float | None, period: str = "2026-09", **kw: Any) -> dict[str, Any]:
    return {
        "period": period,
        "value": value,
        "kind": kind,
        "verification_status": kw.get("status", "verified"),
        "source_url": kw.get("url", f"https://migas.esdm.go.id/cms/uploads/{kind}.pdf"),
        "document_sha256": kw.get("sha", kind),
        "reference": kw.get("ref"),
        "created_at": kw.get("at", "2026-10-16T16:42:54.072Z"),
        "note": kw.get("note"),
    }


SEP = {"period": "2026-09", "value": 112.81, "basis": "two_documents"}
ANN_URL = "https://www.migas.esdm.go.id/post/Tensi-Geopolitik-Memanas-ICP-September-2026"


def _stored_before_repair() -> list[dict[str, Any]]:
    """The local store before the 2026-10-25 repair: both decrees carried the cited formula decree."""
    later = "2026-10-16T17:27:31.885Z"
    return [
        _row("decree_text", 89.43, "2026-08", sha="aug", ref="276.K/MG.03/MEM.M/2026"),
        _row("announcement", 89.43, "2026-08", sha="ann-aug", ref="352.K/MG.03/MEM.M/2026"),
        _row("decree_text", 112.81, sha="sep", ref="276.K/MG.03/MEM.M/2026"),
        _row("decree_text", 112.81, sha="sep", ref="276.K/MG.03/MEM.M/2026", at=later),  # re-retrieved
        _row(
            "announcement",
            112.81,
            sha="ann",
            url=ANN_URL,
            ref="395.K/MG.03/MEM.M/2026",
            note="13 Oktober 2026",
        ),
        _row(
            "announcement",
            112.81,
            sha="ann",
            url=ANN_URL,
            ref="395.K/MG.03/MEM.M/2026",
            note="13 Oktober 2026",
            at=later,
        ),
        _row("legacy_csv", 112.81, sha=None, url="file://C:/repo/data/raw/dataset.csv"),
        _row("announcement", 120.0, sha="other", status="dissent", url="https://www.migas.esdm.go.id/post/o"),
    ]


def test_evidence_deduplicates_documents_and_never_shows_a_cited_decree_number():
    ev = latest_evidence("2026-09", 112.81, SEP, _stored_before_repair())
    assert ev == {
        "period": "2026-09",
        "basis": "two_documents",
        "label": {"id": "Dua dokumen ESDM sepakat", "en": "Two ESDM documents agree"},
        "documents": [
            # 276.K is attached to the decrees of two months: a cited regulation, never shown
            {
                "kind": "decree",
                "reference": None,
                "published_on": None,
                "url": "https://migas.esdm.go.id/cms/uploads/decree_text.pdf",
            },
            # an announcement has no number of its own (it cites the decree's)
            {"kind": "announcement", "reference": None, "published_on": "2026-10-13", "url": ANN_URL},
        ],
    }
    repaired = [
        {
            **r,
            "reference": {"aug": "352.K/MG.03/MEM.M/2026", "sep": "395.K/MG.03/MEM.M/2026"}[
                str(r["document_sha256"])
            ],
        }
        if r["document_sha256"] in ("aug", "sep")
        else r
        for r in _stored_before_repair()
    ]
    docs = latest_evidence("2026-09", 112.81, SEP, repaired)["documents"]
    assert docs[0]["reference"] == "395.K/MG.03/MEM.M/2026" and docs[1]["reference"] is None


def test_evidence_shows_only_verifiable_facts():
    rows = [
        _row(
            "decree_text", 112.81, sha="d", ref="395.K/MG.03/MEM.M/2026", url="http://migas.esdm.go.id/x.pdf"
        ),
        _row(
            "announcement",
            112.81,
            sha="a",
            ref="999.K/MG.03/MEM.M/2026",
            url="https://example.com/p",
            note="Oktober 2026",
        ),
    ]
    assert latest_evidence("2026-09", 112.81, SEP, rows)["documents"] == [
        # the month's announcement names another decree number: the documents disagree -> no number
        {"kind": "decree", "reference": None, "published_on": None, "url": None},  # not https
        {"kind": "announcement", "reference": None, "published_on": None, "url": None},  # not a .go.id page
    ]
    assert latest_evidence("2026-09", 112.81, SEP, [_row("legacy_csv", 112.81)]) is None
    assert latest_evidence("2026-09", 112.81, SEP, [_row("decree_text", 100.0)]) is None  # differing reading
    assert latest_evidence("2026-09", 112.81, None, rows) is None
    assert latest_evidence("2026-09", None, SEP, rows) is None
    refs = trusted_decree_references(
        [
            _row("decree_ocr", 1.0, "2026-01", sha="x", ref="Formula 2026"),
            _row("decree_ocr", 1.0, "2026-02", sha="y", ref="98.K/MG.03/MEM.M/2026"),
            _row(
                "decree_ocr", 1.0, "2026-02", sha="y", ref="99.K/MG.03/MEM.M/2026", at="2026-10-17T00:00:00Z"
            ),
        ]
    )
    assert refs == {("2026-01", "x"): None, ("2026-02", "y"): None}  # not a number / rows disagree


def test_evidence_of_a_conflict_month_lists_no_document():
    """SEC-C-04: the value of a conflict month is withheld, so neither side's documents are listed (an
    announcement link can name the value); the basis and label say why."""
    conflict = {"period": "2026-09", "value": 112.81, "basis": "conflict"}
    rows = [*_stored_before_repair(), _row("announcement", 115.0, sha="other-ann", status="conflict")]
    assert latest_evidence("2026-09", 112.81, conflict, rows) == {
        "period": "2026-09",
        "basis": "conflict",
        "label": {"id": "Sumber tidak sepakat", "en": "Sources disagree"},
        "documents": [],
    }


# -- endpoint (isolated store) ------------------------------------------------------------------------
def _isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, icp_csv: str | None) -> Iterator[Any]:
    from icp_api.main import create_app

    seeds = tmp_path / "seed"
    seeds.mkdir()
    if icp_csv is not None:
        (seeds / "icp.csv").write_text(icp_csv, encoding="utf-8")
    (seeds / "brent.csv").write_text(
        "period,brent_price\n2023-12,79.0\n2024-01,82.0\n2024-03,84.0\n2024-04,85.0\n2024-05,86.0\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "meta.db"))
    monkeypatch.setenv("LOCAL_OBJECT_DIR", str(tmp_path / "objects"))
    monkeypatch.setenv("SEED_ICP_PATH", str(seeds / "icp.csv"))
    monkeypatch.setenv("SEED_WTI_PATH", str(seeds / "absent-wti.csv"))
    monkeypatch.setenv("SEED_DIR", str(seeds))
    monkeypatch.setenv("PUBLIC_HISTORY_MONTHS", "3")
    app = create_app(load_settings(), enable_jobs=False)
    with TestClient(app) as client:
        yield client


@pytest.fixture
def iso(env_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    yield from _isolated(
        tmp_path, monkeypatch, "month,year,icp_price\n1,2024,80.0\n2,2024,81.5\n4,2024,83.0\n"
    )


def _decree_rows(sha: str = "decree-apr") -> list[dict[str, Any]]:
    common = {"dataset": "icp", "period": "2024-04", "value": 83.0, "verification_status": "verified"}
    return [
        {
            **common,
            "kind": "decree_text",
            "source_url": "https://migas.esdm.go.id/cms/uploads/Kepmen_ICP_April_2024.pdf",
            "document_sha256": sha,
            "reference": "50.K/MG.03/MEM.M/2024",
        },
        {
            **common,
            "kind": "announcement",
            "source_url": "https://www.migas.esdm.go.id/post/icp-april-2024",
            "document_sha256": "ann-apr",
            "reference": "50.K/MG.03/MEM.M/2024",
            "note": "8 Mei 2024",
        },
    ]


def test_guest_snapshot_endpoint_with_gaps_cap_evidence_and_no_forecast_values(iso, admin, monkeypatch):
    c = iso.app.state.container
    c.repo.insert_forecast(
        {
            "id": new_id("fc"),
            "created_at": utcnow(),
            "requested_by": "system",
            "status": "succeeded",
            "horizon": 1,
            "params": {"horizon": 1, "scenario": {}},
            "result": {
                "points": [{"period": "2024-05", "prediction": SENTINEL, "lower": 1.5, "upper": 999.5}]
            },
            "issue_date": "2024-05-03",
            "forecast_origin": "2024-04",
            "last_observed_period": "2024-04",
            "workflow": "system",
            "org_id": None,
        }
    )
    c.repo.insert_evidence(_decree_rows() + _decree_rows())  # stored twice: repeated retrieval
    r = iso.get(f"{API}/public/market-snapshot")  # a guest: no Authorization header
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["recent"] == [  # PUBLIC_HISTORY_MONTHS=3; the unpublished month is null, never 0
        {"period": "2024-02", "icp_usd": 81.5, "brent_usd": None},
        {"period": "2024-03", "icp_usd": None, "brent_usd": 84.0},
        {"period": "2024-04", "icp_usd": 83.0, "brent_usd": 85.0},
    ]
    assert body["coverage"] == {
        "first_period": "2024-01",
        "last_period": "2024-04",
        "months": 3,
        "confirmed_months": 1,
    }
    verification = iso.get(f"{API}/data/verification", headers=admin).json()  # operator: unclipped
    assert body["coverage"]["confirmed_months"] == sum(
        verification["counts"][b] for b in ("two_documents", "decree_text")
    )
    assert body["icp_evidence"] == {
        "period": "2024-04",
        "basis": "two_documents",
        "label": {"id": "Dua dokumen ESDM sepakat", "en": "Two ESDM documents agree"},
        "documents": [
            {
                "kind": "decree",
                "reference": "50.K/MG.03/MEM.M/2024",
                "published_on": None,
                "url": "https://migas.esdm.go.id/cms/uploads/Kepmen_ICP_April_2024.pdf",
            },
            {
                "kind": "announcement",
                "reference": None,
                "published_on": "2024-05-08",
                "url": "https://www.migas.esdm.go.id/post/icp-april-2024",
            },
        ],
    }
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z", body["generated_at"])
    # the forecast is announced as a teaser only: no value anywhere in the payload
    assert body["latest_forecast"] == {
        "available": True,
        "issue_date": "2024-05-03",
        "forecast_origin": "2024-04",
        "horizon": 1,
    }
    assert str(SENTINEL) not in r.text and "999.5" not in r.text
    assert not (_keys(body) & FORECAST_KEYS)
    # existing fields are unchanged
    assert body["icp_period"] == "2024-04" and body["icp_usd"] == 83.0 and body["previous_usd"] is None

    monkeypatch.setattr(c, "settings", dataclasses.replace(c.settings, public_history_months=0))
    off = iso.get(f"{API}/public/market-snapshot").json()
    assert off["recent"] == [] and off["coverage"]["months"] == 3 and off["icp_evidence"] is not None


def test_guest_snapshot_withholds_a_conflict_month(iso, admin):
    c = iso.app.state.container
    c.repo.insert_evidence(
        [
            {
                "dataset": "icp",
                "period": "2024-02",
                "value": 90.0,
                "verification_status": "conflict",
                "kind": "announcement",
                "source_url": "https://www.migas.esdm.go.id/post/icp-februari-2024",
                "document_sha256": "ann-feb",
            }
        ]
    )
    months = {m["period"]: m for m in iso.get(f"{API}/data/verification", headers=admin).json()["months"]}
    assert months["2024-02"]["basis"] == "conflict"
    recent = {m["period"]: m["icp_usd"] for m in iso.get(f"{API}/public/market-snapshot").json()["recent"]}
    assert recent == {"2024-02": None, "2024-03": None, "2024-04": 83.0}


def _conflict_row(period: str, value: float = 90.0) -> dict[str, Any]:
    return {
        "dataset": "icp",
        "period": period,
        "value": value,
        "verification_status": "conflict",
        "kind": "announcement",
        "source_url": f"https://www.migas.esdm.go.id/post/icp-{period}",
        "document_sha256": f"ann-{period}",
    }


def test_guest_snapshot_withholds_a_conflict_month_in_the_headline_too(iso, admin):
    """SEC-C-04: when the latest official month is a conflict month, guests get neither its value in
    ``recent`` nor as the headline ICP (with its change and rupiah equivalent); ``icp_evidence`` names
    the basis so the landing can say why. A conflict previous month removes the comparison."""
    c = iso.app.state.container
    plain = iso.get(f"{API}/public/market-snapshot").json()
    assert plain["icp_usd"] == 83.0 and plain["recent"][-1]["icp_usd"] == plain["icp_usd"]

    c.repo.insert_evidence([_conflict_row("2024-04")])
    months = {m["period"]: m for m in iso.get(f"{API}/data/verification", headers=admin).json()["months"]}
    assert months["2024-04"]["basis"] == "conflict"
    body = iso.get(f"{API}/public/market-snapshot").json()
    assert body["icp_period"] == "2024-04"  # the month stays named, its value does not
    assert body["icp_usd"] is None and body["mom_change_pct"] is None and body["icp_idr"] is None
    assert body["recent"][-1] == {"period": "2024-04", "icp_usd": None, "brent_usd": 85.0}
    assert body["icp_evidence"] == {
        "period": "2024-04",
        "basis": "conflict",
        "label": {"id": "Sumber tidak sepakat", "en": "Sources disagree"},
        "documents": [],  # the disagreeing announcement is never presented as the month's source
    }
    assert "83.0" not in json.dumps({k: v for k, v in body.items() if k != "coverage"})
    assert body["coverage"]["months"] == 3  # coverage counts official months; confirmed ones exclude it

    head = {"icp_period": "2024-04", "icp_usd": 83.0, "mom_change_pct": 1.8, "icp_idr": 1.0}
    prev = {"previous_period": "2024-03", "previous_usd": 81.5}
    from icp_api.services.public_snapshot import withhold_headline

    assert withhold_headline({**head, **prev}, {"2024-03"}) == {
        **head,
        "mom_change_pct": None,
        "previous_period": None,
        "previous_usd": None,
    }
    assert withhold_headline({**head, **prev}, set()) == {**head, **prev}


def _count_statements(monkeypatch: pytest.MonkeyPatch, db: Any) -> list[str]:
    seen: list[str] = []
    for name in ("query", "execute"):
        real = getattr(db, name)

        def counted(sql: str, *args: Any, _real: Any = real, **kwargs: Any) -> Any:
            seen.append(sql)
            return _real(sql, *args, **kwargs)

        monkeypatch.setattr(db, name, counted)
    return seen


def test_guest_snapshot_is_memoised_and_a_repeat_reads_nothing(iso, monkeypatch):
    """SEC-C-01: the anonymous snapshot reads every ICP evidence row; repeating it must not, so one
    client cannot spend the D1 daily rows-read allowance through it. Each answer is still stamped with
    its own ``generated_at``."""
    c = iso.app.state.container
    statements = _count_statements(monkeypatch, c.db)
    first = iso.get(f"{API}/public/market-snapshot").json()
    assert any("FROM source_evidence" in s for s in statements)  # computed once
    statements.clear()
    for _ in range(5):
        again = iso.get(f"{API}/public/market-snapshot").json()
    assert statements == []  # a memo hit runs no SQL at all
    assert {k: v for k, v in again.items() if k != "generated_at"} == {
        k: v for k, v in first.items() if k != "generated_at"
    }
    assert again["generated_at"] >= first["generated_at"]


def test_guest_snapshot_memo_follows_every_write_it_depends_on(iso, monkeypatch):
    """Evidence, a system forecast and a dataset activation written in this process show at once."""
    from icp_api.storage.repository import snapshot_data_generation

    c = iso.app.state.container
    get = lambda: iso.get(f"{API}/public/market-snapshot").json()  # noqa: E731
    assert get()["icp_evidence"] is None and get()["latest_forecast"] is None
    c.repo.insert_evidence(_decree_rows())
    assert get()["icp_evidence"]["basis"] == "two_documents"
    c.repo.insert_forecast(
        {
            "id": new_id("fc"),
            "created_at": utcnow(),
            "requested_by": "system",
            "status": "succeeded",
            "horizon": 1,
            "params": {"horizon": 1},
            "result": {"points": [{"period": "2024-05", "prediction": SENTINEL}]},
            "issue_date": "2024-05-03",
            "forecast_origin": "2024-04",
            "workflow": "system",
        }
    )
    assert get()["latest_forecast"]["issue_date"] == "2024-05-03"
    before = get()
    generation = snapshot_data_generation()
    c.repo.activate_dataset_version("brent", c.repo.get_active_dataset("brent")["id"])  # a rollback, say
    assert snapshot_data_generation() > generation
    statements = _count_statements(monkeypatch, c.db)
    after = get()
    assert any("FROM source_evidence" in s for s in statements)  # computed afresh, not served from memo
    assert after["brent_usd"] == before["brent_usd"] and after["icp_usd"] == 83.0


def test_snapshot_memo_expires_and_computes_concurrent_misses_once():
    import threading

    from icp_api.services.public_snapshot import SnapshotMemo

    now = [100.0]
    memo = SnapshotMemo(ttl_seconds=60, clock=lambda: now[0])
    builds: list[int] = []

    def build() -> dict[str, Any]:
        builds.append(1)
        return {"n": len(builds)}

    assert memo.get(("k", 1), build) == {"n": 1}
    now[0] += 59.9
    assert memo.get(("k", 1), build) == {"n": 1}  # within the TTL
    assert memo.get(("k", 2), build) == {"n": 2}  # another key (a write in this process)
    now[0] += 60.0
    assert memo.get(("k", 2), build) == {"n": 3}  # expired: a write by another process shows up
    assert SnapshotMemo(ttl_seconds=0).get("k", build) == {"n": 4}  # TTL 0 stores nothing

    gate = threading.Event()
    slow_builds: list[int] = []

    def slow() -> dict[str, Any]:
        slow_builds.append(1)
        gate.wait(5)
        return {"slow": True}

    shared = SnapshotMemo(ttl_seconds=60)
    results: list[dict[str, Any]] = []
    threads = [threading.Thread(target=lambda: results.append(shared.get("k", slow))) for _ in range(4)]
    for t in threads:
        t.start()
    gate.set()
    for t in threads:
        t.join(10)
    assert results == [{"slow": True}] * 4 and len(slow_builds) == 1


def test_snapshot_without_icp_data(env_dir, tmp_path, monkeypatch):
    for client in _isolated(tmp_path, monkeypatch, None):
        r = client.get(f"{API}/public/market-snapshot")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["recent"] == [] and body["coverage"] is None and body["icp_evidence"] is None
        assert body["icp_usd"] is None and body["icp_period"] is None and body["generated_at"]


@pytest.mark.parametrize("value", ["-1", "37"])
def test_public_history_months_is_validated_at_start_up(env_dir, monkeypatch, value):
    monkeypatch.setenv("PUBLIC_HISTORY_MONTHS", value)
    with pytest.raises(ConfigError, match="PUBLIC_HISTORY_MONTHS"):
        load_settings()


def test_public_history_months_default_and_bounds(env_dir, monkeypatch):
    monkeypatch.delenv("PUBLIC_HISTORY_MONTHS", raising=False)
    assert load_settings().public_history_months == 24
    for value in (0, 36):
        monkeypatch.setenv("PUBLIC_HISTORY_MONTHS", str(value))
        assert load_settings().public_history_months == value


# -- decree-number repair (icp-api repair-decree-references) -----------------------------------------
DECREE_APRIL_2024 = """KEPUTUSAN MENTERI ENERGI DAN SUMBER DAYA MINERAL
REPUBLIK INDONESIA
NOMOR 5 0 .  K/MG. 03/MEM.M/2024
TENTANG
HARGA MINYAK MENTAH INDONESIA BULAN APRIL 2024
Mengingat 9. Keputusan Menteri Energi dan Sumber Daya Mineral
Nomor 7.K/MG.01/MEM.M/2024 tanggal 2 Januari 2024
tentang Formula Harga Minyak Mentah Indonesia;
KEDUA : Harga rata-rata Minyak Mentah Indonesia untuk bulan
April 2024 ditetapkan sebesar US$83,00/barrel.
"""


def _pdf(text: str) -> bytes:
    import fitz

    doc = fitz.open()
    page = doc.new_page()
    for i, line in enumerate((text + "padding " * 30).splitlines()):
        page.insert_text((40, 40 + 10 * i), line, fontsize=8)
    data = bytes(doc.tobytes())
    doc.close()
    return data


WRONG_DECREE = "7.K/MG.01/MEM.M/2024"  # the formula decree cited in the considerations


def _repair_setup(c: Any, tmp_path: Path) -> Path:
    """A stored April 2024 decree whose evidence and state carry the cited decree's number."""
    from icp_ml.sources.base import sha256_bytes

    body = _pdf(DECREE_APRIL_2024)
    sha = sha256_bytes(body)
    state = tmp_path / "esdm"
    (state / "pdf").mkdir(parents=True)
    (state / "pdf" / f"2024-04_{sha[:8]}.pdf").write_bytes(body)
    wrong = WRONG_DECREE
    (state / "decrees.json").write_text(
        json.dumps(
            {
                "2024-04": {
                    "url": "https://migas.esdm.go.id/cms/uploads/Kepmen_ICP_April_2024.pdf",
                    "sha256": sha,
                    "local_path": f"D:/old/place/2024-04_{sha[:8]}.pdf",
                    "extracted_period": "2024-04",
                    "value": 83.0,
                    "method": "text-layer",
                    "decree_number": wrong,
                }
            }
        ),
        encoding="utf-8",
    )
    (state / "reconciliation.json").write_text(
        json.dumps(
            [
                {
                    "period": "2024-04",
                    "value": 83.0,
                    "status": "verified",
                    "note": "",
                    "sources": [{"kind": "decree_text", "value": 83.0, "sha256": sha, "reference": wrong}],
                }
            ]
        ),
        encoding="utf-8",
    )
    rows = _decree_rows(sha)
    rows[0]["reference"] = wrong
    c.repo.insert_evidence([*rows, {**rows[0], "verification_status": "single_source"}])
    return state


def _repair_audit(c: Any) -> list[dict[str, Any]]:
    return c.repo.list_audit(5, None, "dataset.icp.decree_reference_repair").items


def test_repair_decree_references_corrects_numbers_only(iso, tmp_path):
    from icp_api.services.provenance import repair_decree_references

    c = iso.app.state.container
    state = _repair_setup(c, tmp_path)
    wrong = WRONG_DECREE
    before = c.repo.all_evidence("icp")

    dry = repair_decree_references(c, state, apply=False)
    assert dry["applied"] is False and dry["documents"] == 1 and dry["evidence_rows"] == 2
    assert dry["changes"][0] | {} == {
        "period": "2024-04",
        "method": "text-layer",
        "old": wrong,
        "new": "50.K/MG.03/MEM.M/2024",
        "evidence_rows": 2,
    }
    assert c.repo.all_evidence("icp") == before  # a dry run writes nothing

    done = repair_decree_references(c, state, apply=True)
    assert done["applied"] is True and done["evidence_rows_updated"] == 2
    assert done["state_updated"] == {"decrees": 1, "reconciliation_sources": 1}
    after = c.repo.all_evidence("icp")
    assert len(after) == len(before)
    for old, new in zip(before, after, strict=True):
        changed = {k for k in old if old[k] != new[k]}
        assert changed <= {"reference"}  # values, statuses, sources and timestamps untouched
        assert new["value"] == old["value"]
    decree_refs = {r["reference"] for r in after if r["kind"] == "decree_text"}
    assert decree_refs == {"50.K/MG.03/MEM.M/2024"}
    audit = c.repo.list_audit(5, None, "dataset.icp.decree_reference_repair").items
    assert audit and audit[0]["outcome"] == "success"
    again = repair_decree_references(c, state, apply=True)
    assert again["documents"] == 0 and again["applied"] is False  # idempotent


def test_repair_audits_what_was_applied_when_the_state_write_fails(iso, tmp_path, monkeypatch):
    """SEC-C-07: the audit row is written even when the run fails part-way, with the rows actually
    corrected, so a re-run that finds nothing left in the database cannot hide the first change."""
    from icp_api.services.provenance import repair_decree_references
    from icp_ml.sources import esdm_backfill

    c = iso.app.state.container
    state = _repair_setup(c, tmp_path)

    def broken(*_: Any, **__: Any) -> Any:
        raise OSError("disk full")

    monkeypatch.setattr(esdm_backfill, "apply_decree_numbers", broken)
    with pytest.raises(OSError, match="disk full"):
        repair_decree_references(c, state, apply=True, before_apply=lambda: "backup-x")
    audit = _repair_audit(c)
    assert audit and audit[0]["outcome"] == "failure"
    assert audit[0]["details"] == {
        "documents": 1,
        "evidence_rows": 2,
        "state_files_written": False,
        "backup": "backup-x",
    }
    assert WRONG_DECREE in (state / "decrees.json").read_text(encoding="utf-8")  # state untouched


def test_repair_writes_nothing_when_the_backup_fails(iso, tmp_path):
    from icp_api.services.provenance import BackupRequired, repair_decree_references

    c = iso.app.state.container
    state = _repair_setup(c, tmp_path)
    before = c.repo.all_evidence("icp")
    decrees = (state / "decrees.json").read_bytes()

    def no_backup() -> str:
        raise BackupRequired("no backup")

    with pytest.raises(BackupRequired):
        repair_decree_references(c, state, apply=True, before_apply=no_backup)
    assert c.repo.all_evidence("icp") == before and (state / "decrees.json").read_bytes() == decrees
    assert _repair_audit(c) == []
    calls: list[int] = []
    dry = repair_decree_references(c, state, apply=False, before_apply=lambda: str(calls.append(1)))
    assert dry["applied"] is False and calls == []  # a dry run never takes a backup


def test_repair_cli_apply_takes_and_verifies_a_backup_first(iso, tmp_path, monkeypatch, capsys):
    """``repair-decree-references --apply`` on a local store backs up the database, objects and both
    state files, verifies the copy with restore-check, then repairs and records the backup."""
    from icp_api.services import provenance

    c = iso.app.state.container
    state = _repair_setup(c, tmp_path)
    decrees = (state / "decrees.json").read_bytes()
    # no MLflow store in this isolated environment (the backup copies a sqlite:/// store when present)
    monkeypatch.setattr(c, "settings", dataclasses.replace(c.settings, mlflow_tracking_uri="file:./none"))
    monkeypatch.setattr(provenance, "_container", lambda: c)
    backups = tmp_path / "backups"
    code = provenance.main(
        ["repair-decree-references", "--apply", "--state-dir", str(state), "--backup-dir", str(backups)]
    )
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 0 and out["applied"] is True and out["evidence_rows_updated"] == 2
    (dest,) = list(backups.iterdir())
    assert out["backup"] == str(dest)
    assert (dest / "metadata.db").is_file() and (dest / "objects").is_dir()
    assert (dest / "esdm-state" / "decrees.json").read_bytes() == decrees  # the state before the repair
    audit = _repair_audit(c)
    assert audit[0]["outcome"] == "success" and audit[0]["details"]["backup"] == str(dest)

    from icp_api.restore_check import restore_check

    with_evidence = restore_check(dest)
    assert with_evidence["ok"] is True
    import sqlite3

    con = sqlite3.connect(f"{(dest / 'metadata.db').resolve().as_uri()}?mode=ro", uri=True)
    try:
        refs = {r[0] for r in con.execute("SELECT reference FROM source_evidence WHERE kind = 'decree_text'")}
    finally:
        con.close()
    assert refs == {WRONG_DECREE}  # the backup holds the rows as they were before the repair


def test_repair_cli_refuses_on_a_deployed_store_without_a_confirmed_backup(
    iso, tmp_path, monkeypatch, capsys
):
    from icp_api.services import provenance

    c = iso.app.state.container
    state = _repair_setup(c, tmp_path)
    before = c.repo.all_evidence("icp")
    monkeypatch.setattr(c, "settings", dataclasses.replace(c.settings, metadata_backend="d1"))
    monkeypatch.setattr(provenance, "_container", lambda: c)
    args = ["repair-decree-references", "--apply", "--state-dir", str(state)]
    assert provenance.main(args) == 2
    refused = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert refused["applied"] is False and "--backup-taken" in refused["error"]
    assert c.repo.all_evidence("icp") == before and _repair_audit(c) == []
    assert provenance.main([*args, "--backup-taken"]) == 0
    done = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert done["applied"] is True and done["backup"] == "confirmed by operator (--backup-taken)"
