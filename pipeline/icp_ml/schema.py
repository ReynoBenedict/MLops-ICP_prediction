"""Dataset registry, CSV parsing and data-quality validation.

Canonical in-memory representation of a monthly series: a ``pd.Series`` of floats indexed by a
sorted, unique ``PeriodIndex`` with monthly frequency. Gaps are *not* filled here; they are
reported so callers can decide what to do (feature engineering is calendar-aware).

Datasets
--------
icp          Official Indonesian Crude Price, USD/bbl, monthly average set by ministerial decree.
wti          WTI Cushing spot, monthly mean of daily prices, USD/bbl (EIA RWTC).
brent        Europe Brent spot, monthly mean of daily prices, USD/bbl (EIA RBRTE).
dubai        Dubai Fateh, monthly average, USD/bbl (World Bank Pink Sheet).
usd_idr      JISDOR USD/IDR, monthly average of daily fixings, IDR per USD (Bank Indonesia).
usd_idr_eom  JISDOR USD/IDR, last fixing of the month, IDR per USD (Bank Indonesia).

CSV layouts accepted: ``month,year,<value>`` (repository ICP format), ``period,<value>`` or
``date,<value>`` with ``YYYY-MM`` periods. The value column name is per dataset (below).
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    value_column: str
    unit: str
    label_id: str  # Indonesian business label
    label_en: str
    kind: str  # price | fx
    hard_bounds: tuple[float, float]  # values outside (lo, hi] are rejected
    soft_bounds: tuple[float, float]  # values outside produce a warning
    max_mom_change: float  # month-over-month change that triggers an investigation flag
    publication_lag_months: int  # months after period end until the value is normally available
    provider: str
    definition: str
    definition_id: str = ""  # Indonesian business definition shown in the product


DATASET_SPECS: dict[str, DatasetSpec] = {
    "icp": DatasetSpec(
        "icp",
        "icp_price",
        "USD/bbl",
        "Harga Minyak Mentah Indonesia (ICP)",
        "Indonesian Crude Price",
        "price",
        (0.0, 500.0),
        (5.0, 250.0),
        0.40,
        1,
        "Kementerian ESDM (Ditjen Migas)",
        "Official monthly average Indonesian Crude Price set by ministerial decree (Kepmen ESDM), USD per barrel.",
        definition_id="Rata-rata bulanan Harga Minyak Mentah Indonesia yang ditetapkan resmi melalui Keputusan Menteri ESDM, dalam USD per barel.",
    ),
    "wti": DatasetSpec(
        "wti",
        "wti_price",
        "USD/bbl",
        "Harga WTI (rata-rata bulanan)",
        "WTI Cushing spot, monthly mean",
        "price",
        (0.0, 500.0),
        (5.0, 250.0),
        0.40,
        0,
        "U.S. EIA (RWTC) / datasets/oil-prices mirror",
        "Cushing, OK WTI spot price FOB, arithmetic mean of daily prices in the calendar month, USD per barrel.",
        definition_id="Harga spot WTI di Cushing, Oklahoma (FOB), rata-rata aritmetika harga harian dalam bulan kalender, USD per barel.",
    ),
    "brent": DatasetSpec(
        "brent",
        "brent_price",
        "USD/bbl",
        "Harga Brent (rata-rata bulanan)",
        "Europe Brent spot, monthly mean",
        "price",
        (0.0, 500.0),
        (5.0, 250.0),
        0.40,
        0,
        "U.S. EIA (RBRTE) / datasets/oil-prices mirror",
        "Europe Brent spot price FOB, arithmetic mean of daily prices in the calendar month, USD per barrel.",
        definition_id="Harga spot Brent Eropa (FOB), rata-rata aritmetika harga harian dalam bulan kalender, USD per barel.",
    ),
    "dubai": DatasetSpec(
        "dubai",
        "dubai_price",
        "USD/bbl",
        "Harga Dubai (rata-rata bulanan)",
        "Dubai Fateh, monthly average",
        "price",
        (0.0, 500.0),
        (5.0, 250.0),
        0.40,
        0,
        "World Bank Commodity Price Data (Pink Sheet)",
        "Dubai Fateh 32 API f.o.b. Dubai, monthly average, USD per barrel (World Bank Pink Sheet).",
        definition_id="Dubai Fateh 32 API f.o.b. Dubai, rata-rata bulanan, USD per barel (World Bank Pink Sheet).",
    ),
    "usd_idr": DatasetSpec(
        "usd_idr",
        "usd_idr",
        "IDR/USD",
        "Kurs USD/IDR JISDOR (rata-rata bulanan)",
        "JISDOR USD/IDR, monthly average",
        "fx",
        (1000.0, 100000.0),
        (8000.0, 25000.0),
        0.15,
        0,
        "Bank Indonesia (JISDOR)",
        "Arithmetic mean of daily JISDOR USD/IDR fixings over the calendar month, IDR per USD.",
        definition_id="Rata-rata aritmetika kurs JISDOR USD/IDR harian dalam bulan kalender, Rp per USD.",
    ),
    "usd_idr_eom": DatasetSpec(
        "usd_idr_eom",
        "usd_idr_eom",
        "IDR/USD",
        "Kurs USD/IDR JISDOR (akhir bulan)",
        "JISDOR USD/IDR, month-end fixing",
        "fx",
        (1000.0, 100000.0),
        (8000.0, 25000.0),
        0.15,
        0,
        "Bank Indonesia (JISDOR)",
        "Last JISDOR USD/IDR fixing published in the calendar month, IDR per USD.",
        definition_id="Kurs JISDOR USD/IDR terakhir yang diterbitkan dalam bulan kalender, Rp per USD.",
    ),
}
DATASETS: tuple[str, ...] = tuple(DATASET_SPECS)
DatasetName = str  # one of DATASETS; validated at runtime via spec()
VALUE_COLUMN: dict[str, str] = {k: v.value_column for k, v in DATASET_SPECS.items()}
_MIN_YEAR, _MAX_YEAR = 1960, 2100


def spec(dataset: str) -> DatasetSpec:
    try:
        return DATASET_SPECS[dataset]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset '{dataset}'. Known: {', '.join(DATASETS)}") from exc


@dataclass
class Issue:
    code: str
    message: str
    periods: list[str] = field(default_factory=list)


@dataclass
class QualityReport:
    dataset: str
    ok: bool
    row_count: int
    period_start: str | None
    period_end: str | None
    errors: list[Issue] = field(default_factory=list)
    warnings: list[Issue] = field(default_factory=list)
    missing_periods: list[str] = field(default_factory=list)
    duplicate_periods: list[str] = field(default_factory=list)
    stats: dict[str, float | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DatasetValidationError(ValueError):
    """Raised when a dataset cannot be used because it fails validation."""

    def __init__(self, report: QualityReport):
        self.report = report
        msgs = "; ".join(e.message for e in report.errors) or "unknown validation error"
        super().__init__(f"{report.dataset} dataset is invalid: {msgs}")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _empty_report(dataset: str, issue: Issue) -> QualityReport:
    return QualityReport(
        dataset=dataset, ok=False, row_count=0, period_start=None, period_end=None, errors=[issue]
    )


def _parse_periods(df: pd.DataFrame, value_col: str) -> tuple[pd.Series | None, Issue | None]:
    """Return a Series of pd.Period (NaT for unparseable rows) or a schema error."""
    cols = set(df.columns)
    if {"year", "month"} <= cols:
        year = pd.to_numeric(df["year"], errors="coerce")
        month = pd.to_numeric(df["month"], errors="coerce")
        periods = []
        for y, m in zip(year, month, strict=True):
            if pd.isna(y) or pd.isna(m) or int(m) != m or int(y) != y or not 1 <= m <= 12:
                periods.append(pd.NaT)
            elif not _MIN_YEAR <= y <= _MAX_YEAR:
                periods.append(pd.NaT)
            else:
                periods.append(pd.Period(year=int(y), month=int(m), freq="M"))
        return pd.Series(periods, index=df.index, dtype="object"), None
    for key in ("period", "date"):
        if key in cols:
            raw = df[key].astype(str).str.strip()
            ok = raw.str.fullmatch(r"\d{4}-\d{2}")
            periods = []
            for s, good in zip(raw, ok, strict=True):
                if not good:
                    periods.append(pd.NaT)
                    continue
                y, m = int(s[:4]), int(s[5:7])
                periods.append(
                    pd.Period(year=y, month=m, freq="M")
                    if 1 <= m <= 12 and _MIN_YEAR <= y <= _MAX_YEAR
                    else pd.NaT
                )
            return pd.Series(periods, index=df.index, dtype="object"), None
    return None, Issue(
        "SCHEMA_MISSING_COLUMNS",
        f"Missing period columns; expected month,year,{value_col} or period,{value_col}.",
    )


def validate_frame(df: pd.DataFrame, dataset: DatasetName) -> tuple[pd.Series | None, QualityReport]:
    """Validate a raw dataframe and return (series, report). ``series`` is None when invalid."""
    sp = spec(dataset)
    value_col = sp.value_column
    df = df.rename(columns={c: str(c).strip().lower() for c in df.columns})
    # Accept a generic "value" column as the dataset's value column.
    if value_col not in df.columns and "value" in df.columns:
        df = df.rename(columns={"value": value_col})
    if df.empty:
        return None, _empty_report(dataset, Issue("EMPTY", "Dataset contains no rows."))
    if value_col not in df.columns:
        return None, _empty_report(
            dataset, Issue("SCHEMA_MISSING_COLUMNS", f"Missing value column '{value_col}'.")
        )
    periods, schema_issue = _parse_periods(df, value_col)
    if schema_issue or periods is None:
        return None, _empty_report(dataset, schema_issue or Issue("SCHEMA", "Unparseable dataset."))

    errors: list[Issue] = []
    warnings: list[Issue] = []

    bad_period_rows = [int(i) + 2 for i in df.index[periods.isna()]]  # +2: header + 1-based
    if bad_period_rows:
        errors.append(
            Issue(
                "INVALID_PERIOD",
                f"{len(bad_period_rows)} row(s) have an invalid year/month or date (CSV lines {bad_period_rows[:10]}).",
            )
        )

    values = pd.to_numeric(df[value_col], errors="coerce")
    null_mask = values.isna() & periods.notna()
    if null_mask.any():
        errors.append(
            Issue(
                "NULL_VALUES",
                f"{int(null_mask.sum())} row(s) have a missing or non-numeric {value_col}.",
                sorted(str(p) for p in periods[null_mask]),
            )
        )

    valid = periods.notna() & values.notna()
    work = pd.DataFrame({"period": periods[valid], "value": values[valid].astype(float)})

    dup_mask = work["period"].duplicated(keep=False)
    duplicate_periods = sorted({str(p) for p in work.loc[dup_mask, "period"]})
    if duplicate_periods:
        errors.append(
            Issue(
                "DUPLICATE_PERIODS",
                f"{len(duplicate_periods)} period(s) appear more than once.",
                duplicate_periods,
            )
        )

    lo, hi = sp.hard_bounds
    out_hard = work[(work["value"] <= lo) | (work["value"] > hi)]
    if not out_hard.empty:
        errors.append(
            Issue(
                "VALUE_OUT_OF_RANGE",
                f"{len(out_hard)} value(s) outside ({lo:g}, {hi:g}] {sp.unit}.",
                sorted(str(p) for p in out_hard["period"]),
            )
        )
    slo, shi = sp.soft_bounds
    out_soft = work[
        ((work["value"] < slo) | (work["value"] > shi)) & (work["value"] > lo) & (work["value"] <= hi)
    ]
    if not out_soft.empty:
        warnings.append(
            Issue(
                "VALUE_UNUSUAL",
                f"{len(out_soft)} value(s) outside the typical {slo:g}-{shi:g} {sp.unit} range.",
                sorted(str(p) for p in out_soft["period"]),
            )
        )

    if len(work) and not pd.PeriodIndex(list(work["period"]), freq="M").is_monotonic_increasing:
        warnings.append(Issue("UNSORTED", "Rows are not in chronological order; they were sorted."))

    work = work.drop_duplicates("period", keep=False).sort_values("period")
    series = pd.Series(
        work["value"].to_numpy(), index=pd.PeriodIndex(work["period"], freq="M"), name=value_col
    )

    missing: list[str] = []
    if len(series) > 1:
        full = pd.period_range(series.index.min(), series.index.max(), freq="M")
        missing = [str(p) for p in full.difference(series.index)]
        if missing:
            warnings.append(
                Issue(
                    "MISSING_PERIODS", f"{len(missing)} month(s) missing inside the covered range.", missing
                )
            )
        # Calendar-aware: only consecutive months are compared (a gap yields NaN, not a fake jump).
        change = series.reindex(full).pct_change(fill_method=None).abs()
        jump_idx = list(change[change > sp.max_mom_change].index)
        if jump_idx:
            warnings.append(
                Issue(
                    "LARGE_MOM_CHANGE",
                    f"{len(jump_idx)} month-over-month change(s) larger than {int(sp.max_mom_change * 100)}%.",
                    [str(p) for p in jump_idx],
                )
            )

    stats: dict[str, float | None] = {
        "min": float(series.min()) if len(series) else None,
        "max": float(series.max()) if len(series) else None,
        "mean": float(series.mean()) if len(series) else None,
        "std": float(series.std()) if len(series) > 1 else None,
        "latest": float(series.iloc[-1]) if len(series) else None,
    }
    report = QualityReport(
        dataset=dataset,
        ok=not errors,
        row_count=len(df),
        period_start=str(series.index.min()) if len(series) else None,
        period_end=str(series.index.max()) if len(series) else None,
        errors=errors,
        warnings=warnings,
        missing_periods=missing,
        duplicate_periods=duplicate_periods,
        stats=stats,
    )
    return (series if report.ok else None), report


def parse_csv_bytes(data: bytes, dataset: DatasetName) -> tuple[pd.Series | None, QualityReport]:
    try:
        df = pd.read_csv(io.BytesIO(data))
    except Exception as exc:  # noqa: BLE001 - surface any parser failure as a validation error
        return None, _empty_report(
            dataset, Issue("CSV_PARSE_ERROR", f"Could not parse CSV: {type(exc).__name__}.")
        )
    return validate_frame(df, dataset)


def load_series(data: bytes, dataset: DatasetName) -> pd.Series:
    """Parse and validate; raise DatasetValidationError when the data is unusable."""
    series, report = parse_csv_bytes(data, dataset)
    if series is None:
        raise DatasetValidationError(report)
    return series


def series_to_csv_bytes(series: pd.Series, dataset: DatasetName) -> bytes:
    """Serialise a validated series to the canonical CSV layout for that dataset."""
    value_col = spec(dataset).value_column
    if dataset == "icp":
        df = pd.DataFrame(
            {
                "month": [p.month for p in series.index],
                "year": [p.year for p in series.index],
                value_col: series.to_numpy(),
            }
        )
    else:
        df = pd.DataFrame({"period": [str(p) for p in series.index], value_col: series.to_numpy()})
    return df.to_csv(index=False, float_format="%.4f").encode()


def freshness(
    period_end: str | None, today: date | None = None, expected_lag_months: int = 1, grace_months: int = 1
) -> dict[str, Any]:
    """Describe how stale a monthly dataset is relative to its publication schedule.

    The latest expected period is ``today's month - expected_lag_months`` (ICP for month M is
    published early in M+1, so lag 1; daily-derived monthly series complete at month end, lag 0
    but only once the month has ended, which the caller enforces by never storing partial
    months). One extra month of grace is allowed before "stale".
    """
    today = today or date.today()
    if not period_end:
        return {"status": "unknown", "latest_period": None, "expected_period": None, "months_behind": None}
    current = pd.Period(year=today.year, month=today.month, freq="M")
    expected = (
        current - max(1, expected_lag_months) if expected_lag_months == 0 else current - expected_lag_months
    )
    latest = pd.Period(period_end, freq="M")
    behind = max(0, (expected - latest).n)
    status = "fresh" if behind == 0 else ("lagging" if behind <= grace_months else "stale")
    return {
        "status": status,
        "latest_period": str(latest),
        "expected_period": str(expected),
        "months_behind": behind,
    }


def diff_series(old: pd.Series | None, new: pd.Series, tol: float = 1e-6) -> dict[str, Any]:
    """Compare two series: added, removed and changed periods (used to guard destructive imports)."""
    old = old if old is not None else pd.Series(dtype=float, index=pd.PeriodIndex([], freq="M"))
    added = sorted(str(p) for p in new.index.difference(old.index))
    removed = sorted(str(p) for p in old.index.difference(new.index))
    common = new.index.intersection(old.index)
    changed = [
        {"period": str(p), "old": float(old[p]), "new": float(new[p])}
        for p in sorted(common)
        if not np.isclose(float(old[p]), float(new[p]), atol=tol)
    ]
    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": int(len(common) - len(changed)),
    }
