from __future__ import annotations

from datetime import date

import pandas as pd

from icp_ml.schema import diff_series, freshness, parse_csv_bytes, series_to_csv_bytes


def test_valid_icp_csv_with_gap_reports_missing_months():
    data = b"month,year,icp_price\n1,2024,70.1\n2,2024,71.2\n4,2024,72.0\n"
    series, report = parse_csv_bytes(data, "icp")
    assert report.ok and series is not None
    assert report.missing_periods == ["2024-03"]
    assert [w.code for w in report.warnings] == ["MISSING_PERIODS"]
    assert report.period_start == "2024-01" and report.period_end == "2024-04"


def test_period_column_layout_is_supported():
    series, report = parse_csv_bytes(b"period,icp_price\n2024-01,70\n2024-02,71\n", "icp")
    assert report.ok and list(series.index.astype(str)) == ["2024-01", "2024-02"]


def test_duplicates_nulls_and_ranges_are_errors():
    data = b"month,year,icp_price\n1,2024,70\n1,2024,71\n2,2024,\n3,2024,-5\n13,2024,70\n"
    series, report = parse_csv_bytes(data, "icp")
    assert series is None and not report.ok
    codes = {e.code for e in report.errors}
    assert {"DUPLICATE_PERIODS", "NULL_VALUES", "VALUE_OUT_OF_RANGE", "INVALID_PERIOD"} <= codes
    assert report.duplicate_periods == ["2024-01"]


def test_missing_columns_and_garbage():
    assert parse_csv_bytes(b"foo,bar\n1,2\n", "icp")[1].errors[0].code == "SCHEMA_MISSING_COLUMNS"
    assert parse_csv_bytes(b"date,wti_price\n", "wti")[1].errors[0].code == "EMPTY"
    assert not parse_csv_bytes(b"\x00\x01\x02", "wti")[1].ok


def test_unsorted_input_is_sorted_with_warning():
    series, report = parse_csv_bytes(b"date,wti_price\n2024-02,71\n2024-01,70\n", "wti")
    assert report.ok and series.index.is_monotonic_increasing
    assert "UNSORTED" in {w.code for w in report.warnings}


def test_large_jump_only_between_consecutive_months():
    data = b"date,wti_price\n2024-01,50\n2024-02,80\n2024-04,30\n"
    _, report = parse_csv_bytes(data, "wti")
    jumps = [w for w in report.warnings if w.code == "LARGE_MOM_CHANGE"]
    assert jumps and jumps[0].periods == ["2024-02"]  # 2024-04 follows a gap, not a jump


def test_roundtrip_canonical_csv():
    series, _ = parse_csv_bytes(b"month,year,icp_price\n1,2024,70.12\n2,2024,71\n", "icp")
    again, report = parse_csv_bytes(series_to_csv_bytes(series, "icp"), "icp")
    assert report.ok and again.equals(series)


def test_freshness_statuses():
    assert freshness("2026-09", today=date(2026, 10, 16))["status"] == "fresh"
    assert freshness("2026-08", today=date(2026, 10, 16))["status"] == "lagging"
    stale = freshness("2026-03", today=date(2026, 10, 16))
    assert stale["status"] == "stale" and stale["months_behind"] == 6
    assert freshness(None)["status"] == "unknown"


def test_diff_series_detects_changes():
    idx = pd.period_range("2024-01", periods=3, freq="M")
    old = pd.Series([1.0, 2.0, 3.0], index=idx)
    new = pd.Series([1.0, 2.5], index=idx[:2]).combine_first(
        pd.Series([4.0], index=[pd.Period("2024-04", "M")])
    )
    d = diff_series(old, new)
    assert d["added"] == ["2024-04"] and d["removed"] == ["2024-03"]
    assert d["changed"] == [{"period": "2024-02", "old": 2.0, "new": 2.5}] and d["unchanged"] == 1
