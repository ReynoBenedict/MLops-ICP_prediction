"""Offline tests for the data-source connectors (parsing, aggregation, completeness, reconciliation)."""

from __future__ import annotations

import io
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from icp_ml.schema import DATASETS, parse_csv_bytes, series_to_csv_bytes, spec
from icp_ml.sources import bi_jisdor, esdm_icp, oil_benchmarks, worldbank_cmo
from icp_ml.sources.base import HttpClient, SourceDocument, aggregate_daily_to_monthly, business_days_in_month

ROOT = Path(__file__).resolve().parents[2]


class FakeClient(HttpClient):
    """HttpClient whose GET is served from a dict (no network)."""

    def __init__(self, responses: dict[str, bytes], tmp_path):
        super().__init__(cache_dir=tmp_path / "cache")
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url, *, max_age_s=None, params=None):  # type: ignore[override]
        import requests

        if params:
            url = requests.Request("GET", url, params=params).prepare().url or url
        self.calls.append(url)
        for key, body in self.responses.items():
            if url.startswith(key) or key in url:
                return body, SourceDocument(url, "f" * 64, len(body), "text/plain", "2026-10-16T00:00:00Z")
        raise AssertionError(f"unexpected URL {url}")


# ---------------------------------------------------------------------------------------------
def test_dataset_specs_cover_all_datasets_and_csv_roundtrip():
    for ds in DATASETS:
        sp = spec(ds)
        idx = pd.period_range("2024-01", periods=3, freq="M")
        val = 15000.0 if sp.kind == "fx" else 70.0
        s = pd.Series([val, val + 1, val + 2], index=idx, name=sp.value_column)
        back, report = parse_csv_bytes(series_to_csv_bytes(s, ds), ds)
        assert report.ok and back is not None and back.equals(s.rename(back.name)), ds


def test_fx_bounds_reject_oil_sized_values():
    _, report = parse_csv_bytes(b"period,usd_idr\n2024-01,70.5\n", "usd_idr")
    assert not report.ok and report.errors[0].code == "VALUE_OUT_OF_RANGE"


def test_monthly_aggregation_marks_partial_and_sparse_months():
    days = pd.bdate_range("2026-08-03", "2026-10-14")
    daily = pd.DataFrame({"date": days.strftime("%Y-%m-%d"), "value": 80.0})
    out = aggregate_daily_to_monthly(daily, "value", "mean", today=date(2026, 10, 16))
    by = {r["period"]: r for r in out.to_dict("records")}
    assert by["2026-08"]["complete"] and by["2026-09"]["complete"]
    assert not by["2026-10"]["complete"] and by["2026-10"]["n_obs"] == 10
    sparse = aggregate_daily_to_monthly(
        daily[daily["date"] < "2026-09-10"], "value", "mean", today=date(2026, 10, 16)
    )
    sep = next(r for r in sparse.to_dict("records") if r["period"] == "2026-09")
    assert not sep["complete"]  # only ~7 of 22 business days observed
    assert business_days_in_month(pd.Period("2026-09", freq="M")) == 22


# ---------------------------------------------------------------------------------------------
JISDOR_XML = b"""<?xml version="1.0" encoding="utf-8"?><DataSet xmlns="http://tempuri.org/">
<diffgr:diffgram xmlns:msdata="urn:schemas-microsoft-com:xml-msdata" xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1"><NewDataSet xmlns="">
<Table diffgr:id="Table1" msdata:rowOrder="0"><id_subkursasing>1</id_subkursasing><nil_subkursasing>1.00</nil_subkursasing>
<beli_subkursasing>17948.00</beli_subkursasing><jual_subkursasing>17948.00</jual_subkursasing><tgl_subkursasing>2026-09-29T00:00:00+07:00</tgl_subkursasing><mts_subkursasing>USD</mts_subkursasing></Table>
<Table diffgr:id="Table2" msdata:rowOrder="1"><id_subkursasing>2</id_subkursasing><nil_subkursasing>1.00</nil_subkursasing>
<beli_subkursasing>17877.00</beli_subkursasing><jual_subkursasing>17877.00</jual_subkursasing><tgl_subkursasing>2026-09-30T00:00:00+07:00</tgl_subkursasing><mts_subkursasing>USD</mts_subkursasing></Table>
</NewDataSet></diffgr:diffgram></DataSet>"""


def test_jisdor_parsing_and_quarter_windows(tmp_path):
    df = bi_jisdor._parse_dataset_xml(JISDOR_XML)
    assert df["date"].tolist() == ["2026-09-29", "2026-09-30"] and df["usd_idr"].tolist() == [
        17948.0,
        17877.0,
    ]
    qs = bi_jisdor._quarters(date(2026, 2, 10), date(2026, 10, 16))
    assert (
        qs[0] == (date(2026, 2, 10), date(2026, 3, 31))
        and qs[-1] == (date(2026, 10, 1), date(2026, 10, 16))
        and len(qs) == 4
    )
    client = FakeClient({bi_jisdor.ENDPOINT: JISDOR_XML}, tmp_path)
    fx = bi_jisdor.fetch_usd_idr(
        client, start=date(2026, 7, 1), end=date(2026, 10, 16), today=date(2026, 10, 16)
    )
    m = fx["usd_idr"].monthly.set_index("period")
    assert m.loc["2026-09", "value"] == pytest.approx((17948 + 17877) / 2)
    assert not m.loc["2026-09", "complete"]  # 2 of 22 business days -> never silently complete
    assert fx["usd_idr_eom"].monthly.set_index("period").loc["2026-09", "value"] == 17877.0
    assert any("only" in w for w in fx["usd_idr"].warnings)  # truncated closed quarter is flagged


# ---------------------------------------------------------------------------------------------
def _cmo_workbook() -> bytes:
    rows = [
        ["World Bank Commodity Price Data"],
        ["monthly prices"],
        ["(monthly)"],
        ["Updated on October 02, 2026"],
        [None, "Crude oil, average", "Crude oil, Brent", "Crude oil, Dubai", "Crude oil, WTI"],
        [None, "($/bbl)", "($/bbl)", "($/bbl)", "($/bbl)"],
        ["2026M08", 84.4, 90.9, 79.7, 82.7],
        ["2026M09", 102.3, 116.8, 93.3, 96.9],
    ]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        pd.DataFrame(rows).to_excel(xw, sheet_name="Monthly Prices", header=False, index=False)
    return buf.getvalue()


def test_worldbank_cmo_parsing(tmp_path):
    page = (
        b'<a href="https://thedocs.worldbank.org/en/doc/x/related/CMO-Historical-Data-Monthly.xlsx">xlsx</a>'
    )
    client = FakeClient(
        {worldbank_cmo.PAGE_URL: page, "CMO-Historical-Data-Monthly.xlsx": _cmo_workbook()}, tmp_path
    )
    out = worldbank_cmo.fetch_cmo_monthly(client, today=date(2026, 10, 16))
    assert set(out) == {"brent", "dubai", "wti_cmo", "crude_avg"}
    dubai = out["dubai"]
    assert dubai.complete_series()[pd.Period("2026-09", freq="M")] == 93.3
    assert dubai.evidence[0].reference.startswith("Monthly Prices!Crude oil, Dubai")
    assert "Updated on October 02, 2026" in dubai.definition


def test_oil_benchmark_mirror_and_partial_month(tmp_path):
    csv = b"Date,Price\n2026-08-31,80\n2026-09-01,90\n2026-09-30,110\n2026-10-01,120\n"
    client = FakeClient({oil_benchmarks.MIRROR["wti"]: csv}, tmp_path)
    f = oil_benchmarks.fetch_benchmark(client, "wti", start="2026-08-01", today=date(2026, 10, 2), api_key="")
    assert f.provider.startswith("datasets/oil-prices") and "PDDL" in f.license_note
    m = f.monthly.set_index("period")
    assert m.loc["2026-09", "value"] == 100.0 and not m.loc["2026-09", "complete"]  # 2 of 22 days
    assert "2026-10" in f.partial_months()
    with pytest.raises(ValueError):
        oil_benchmarks.fetch_benchmark(client, "gold")


# ---------------------------------------------------------------------------------------------
ARCHIVE_HTML = """<table><tr><th>Bulan</th><th>2025</th><th>2026</th></tr>
<tr><td>Januari</td><td><a href="/cms/uploads/x/Jan-2025.pdf">Januari</a></td><td><a href="https://migas.esdm.go.id/cms/u/Jan%202026.pdf">Januari</a></td></tr>
<tr><td>Februari</td><td><a href="/cms/uploads/x/Feb-2025.pdf">Februari</a></td><td></td></tr></table>"""


def test_archive_table_and_decree_text():
    entries = esdm_icp.parse_archive_table(ARCHIVE_HTML)
    assert [(e.period, e.url.startswith("https://")) for e in entries] == [
        ("2025-01", True),
        ("2026-01", True),
        ("2025-02", True),
    ]
    period, value, number = esdm_icp.extract_from_text(
        "KEPUTUSAN ... NOMOR : 395.K/MG.03/MEM.M/2026 ... KEDUA : Harga rata-rata Minyak Mentah Indonesia untuk bulan September 2026 "
        "ditetapkan sebesar US$112,81/barrel. KETIGA"
    )
    assert (period, value, number) == ("2026-09", 112.81, "395.K/MG.03/MEM.M/2026")
    # OCR variants: 'US$ 57.27', 'USS63.26', 'US5 40,67'
    for txt, exp in [
        ("untuk bulan Agustus 2019 ditetapkan sebesar US$ 57.27/barrel", 57.27),
        ("untuk bulan November 2019 ditetapkan sebesar USS63.26/barrel", 63.26),
        ("untuk bulan November 2020 ditetapkan sebesar US5 40,67/barrel", 40.67),
    ]:
        assert esdm_icp.extract_from_text(txt)[1] == exp, txt
    # annex grade prices and provisional figures must never be mistaken for the ICP
    assert (
        esdm_icp.extract_from_text(
            "Minas/SLC ditetapkan US$113,50 per barel. perkembangan sementara ICP per tanggal 24 US$114,55"
        )[1]
        is None
    )


ARTICLE_HTML = """<html><head><title>ICP Januari 2022 Terkerek Jadi US$85,89 per Barel</title></head><body>
<p>7 Februari 2022</p><p>Harga rata-rata minyak mentah Indonesia untuk bulan Januari 2022 ditetapkan sebesar US$85,89/barel,
naik US$12,53 per barel dibandingkan Desember 2021 yang mencapai US$73,36 per barel. Penetapan ini tertuang dalam Keputusan Menteri
ESDM Nomor 11.K/MG.03/DJM/2022.</p><p>Dated Brent naik sebesar US$13,12 per barel dari US$74,10 per barel menjadi US$87,22 per barel.
WTI (Nymex) naik sebesar US$11,29 per barel dari US$71,69 per barel menjadi US$82,98 per barel.</p></body></html>"""


def test_article_parsing_and_reconciliation():
    a = esdm_icp.parse_article(
        "https://www.migas.esdm.go.id/post/icp-januari-2022-terkerek-jadi-us-85-89-per-barel", ARTICLE_HTML
    )
    assert (a.period, a.value, a.previous_period, a.previous_value) == ("2022-01", 85.89, "2021-12", 73.36)
    assert a.decree_number == "11.K/MG.03/DJM/2022" and a.benchmarks == {
        "dated_brent": 87.22,
        "wti_nymex": 82.98,
    }
    obs = {
        "2022-01": [
            {"kind": "decree_ocr", "value": 85.89, "url": "u1"},
            {"kind": "announcement", "value": 85.89, "url": "u2"},
        ],
        "2022-03": [{"kind": "decree_ocr", "value": 113.5, "url": "u3"}],
        "2022-04": [{"kind": "decree_text", "value": 102.51, "url": "u4"}],
        "2022-05": [
            {"kind": "decree_ocr", "value": 109.61, "url": "u5"},
            {"kind": "announcement", "value": 99.12, "url": "u6"},
        ],
        "2022-06": [],
    }
    status = {m.period: (m.status, m.value) for m in esdm_icp.reconcile(obs)}
    assert status == {
        "2022-01": ("verified", 85.89),
        "2022-03": ("single_source", 113.5),
        "2022-04": ("verified", 102.51),
        "2022-05": ("conflict", None),
        "2022-06": ("missing", None),
    }


@pytest.mark.skipif(
    not (ROOT / "data" / "raw" / "Desember 2020.pdf").exists(),
    reason="repository PDFs not present (dvc pull)",
)
def test_scanned_decree_ocr_reaches_the_operative_clause():
    """2019-2021 decrees are scans whose operative clause sits on page 3-4 (regression for a 3-page OCR cap)."""
    pytest.importorskip("pytesseract")
    pytest.importorskip("fitz")
    import shutil

    cmd = os.getenv("TESSERACT_CMD", r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    if not (shutil.which("tesseract") or os.path.exists(cmd)):
        pytest.skip("tesseract binary not available")
    result = esdm_icp.extract_decree((ROOT / "data" / "raw" / "Desember 2020.pdf").read_bytes())
    assert result.period == "2020-12" and result.value == pytest.approx(47.78) and result.method == "ocr"


def test_announcement_parser_separates_levels_from_changes():
    html_page = """<html><head><title>Naik US$ 2,69, Menteri ESDM Tetapkan ICP Juli 2024 sebesar US$ 82,00 per Barel</title></head>
    <body><p>Jakarta, 6 Agustus 2024.</p><p>Harga rata-rata minyak mentah Indonesia (ICP) bulan Juli 2024 naik sebesar
    US$ 2,69 per barel dibandingkan Juni 2024 yang sebesar US$ 79,31 per barel menjadi US$ 82,00 per barel.</p>
    <p>Dated Brent naik dari US$ 82,61/bbl menjadi US$ 85,31/bbl.</p>
    <p>Keputusan Menteri ESDM Nomor 333.K/MG.03/DJM/2024.</p></body></html>"""
    a = esdm_icp.parse_article("https://www.migas.esdm.go.id/post/Naik-US-2-69-ICP-Juli-2024", html_page)
    assert (a.period, a.value) == ("2024-07", 82.0)
    assert (a.previous_period, a.previous_value) == ("2024-06", 79.31)  # not the Brent level, not the change
    assert a.decree_number == "333.K/MG.03/DJM/2024"
    # Title without a unit and without a year: "ICP April Turun Menjadi USD65,29" published in May 2025.
    html_page2 = """<html><head><title>ICP April Turun Menjadi USD65,29 per Barel</title></head>
    <body><p>27 Mei 2025</p><p>ICP April 2025 turun US$5,82 per barel dari US$71,11 per barel pada Maret 2025.</p></body></html>"""
    b = esdm_icp.parse_article("https://www.migas.esdm.go.id/post/icp-april-turun", html_page2)
    assert (b.period, b.value, b.previous_period, b.previous_value) == ("2025-04", 65.29, "2025-03", 71.11)


def test_reconcile_weak_sources_corroborate_but_never_conflict():
    obs = {
        "2022-01": [
            {"kind": "decree_ocr", "value": 85.89, "url": "u1", "sha256": "a"},
            {"kind": "legacy_csv", "value": 84.54, "url": "file", "sha256": None},
        ],
        "2022-02": [
            {"kind": "decree_ocr", "value": 95.72, "url": "u2", "sha256": "b"},
            {"kind": "legacy_csv", "value": 95.72, "url": "file", "sha256": None},
        ],
        "2024-05": [
            {"kind": "announcement", "value": 79.78, "url": "u3", "sha256": "c"},
            {"kind": "decree_text", "value": 79.78, "url": "u4", "sha256": "d"},
            {"kind": "announcement_backref", "value": 0.47, "url": "u5", "sha256": "e"},
        ],
        "2020-11": [
            {"kind": "announcement", "value": 3.16, "url": "u6", "sha256": "f"},
            {"kind": "decree_text", "value": 40.67, "url": "u7", "sha256": "g"},
        ],
        "2023-07": [{"kind": "legacy_csv", "value": 80.0, "url": "file", "sha256": None}],
    }
    by_period = {m.period: m for m in esdm_icp.reconcile(obs)}
    assert (
        by_period["2022-01"].status == "single_source"
        and "corrects legacy value 84.54" in by_period["2022-01"].note
    )
    assert by_period["2022-02"].status == "verified"  # OCR corroborated by the curated legacy value
    assert (
        by_period["2024-05"].status == "verified"
        and "announcement_backref differs" in by_period["2024-05"].note
    )
    assert by_period["2020-11"].status == "conflict" and by_period["2020-11"].value is None
    assert by_period["2023-07"].status == "single_source" and by_period["2023-07"].note == "only legacy_csv"


def test_reconcile_majority_rule_and_title_without_indonesia():
    obs = {
        "2019-04": [
            {"kind": "decree_text", "value": 68.31, "url": "d", "sha256": "1"},
            {"kind": "announcement_backref", "value": 68.31, "url": "b", "sha256": "2"},
            {"kind": "announcement", "value": 63.81, "url": "a", "sha256": "3"},
        ],
        "2019-05": [
            {"kind": "decree_ocr", "value": 68.07, "url": "d", "sha256": "1"},
            {"kind": "announcement", "value": 63.81, "url": "a", "sha256": "3"},
        ],
    }
    by_period = {m.period: m for m in esdm_icp.reconcile(obs)}
    assert by_period["2019-04"].status == "verified" and by_period["2019-04"].value == 68.31
    assert "outvoted" in by_period["2019-04"].note
    assert by_period["2019-05"].status == "conflict"  # one against one is still a conflict
    page = """<html><head><title>ICP Minyak Mentah Juli 2023 Meningkat ke US$ 75,06 per Barel</title></head>
    <body><p>3 Agustus 2023</p><p>ICP Juli 2023 naik US$ 5,10 per barel dari US$ 69,96 per barel pada Juni 2023.</p></body></html>"""
    a = esdm_icp.parse_article("https://www.migas.esdm.go.id/post/icp-minyak-mentah-juli-2023", page)
    assert (a.period, a.value, a.previous_period, a.previous_value) == ("2023-07", 75.06, "2023-06", 69.96)
