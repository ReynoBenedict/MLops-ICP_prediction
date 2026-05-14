# fetch_wti.py
# Download WTI crude oil monthly average prices.
# Source (series): FRED DCOILWTICO — https://fred.stlouisfed.org/series/DCOILWTICO
#
# Fetch strategy (tried in order):
#   1. GitHub raw CDN  — fast globally, no API key, same FRED data
#   2. FRED graph CSV  — direct endpoint, lighter than download page
#
# Data is daily -> aggregated to monthly average here.
# Output: data/raw/wti.csv
# Schema: date (YYYY-MM), wti_price

from __future__ import annotations

import logging
import time
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
WTI_OUT_CSV = _PROJECT_ROOT / "data" / "raw" / "wti.csv"

# Source priority list — first reachable source wins.
# GitHub CDN is globally fast; FRED is US-centric and can stall from Asia.
_SOURCES: List[dict] = [
    {
        "name": "GitHub (datasets/oil-prices)",
        "url": "https://raw.githubusercontent.com/datasets/oil-prices/master/data/wti-daily.csv",
        "parser": "_parse_github_csv",
    },
    {
        "name": "FRED graph endpoint",
        "url": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO",
        "parser": "_parse_fred_csv",
    },
]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ICP-MLOps-Pipeline/1.0)"}

MAX_RETRIES = 2
RETRY_DELAY = 3.0
# (connect_timeout, read_timeout) — avoids hanging on stalled government servers
_TIMEOUT = (10, 30)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def _fetch_with_retry(url: str) -> requests.Response:
    """GET with automatic retry. Uses (connect, read) timeout tuple to avoid hangs."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            logger.warning("[WTI] Attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"[WTI] All {MAX_RETRIES} attempts failed for {url}")


# ---------------------------------------------------------------------------
# Parsers (one per source format)
# ---------------------------------------------------------------------------


def _parse_github_csv(text: str) -> pd.DataFrame:
    """
    Parse GitHub datasets/oil-prices WTI daily CSV.

    Format:
        Date,Price
        2019-01-02,50.54
        ...
    """
    df = pd.read_csv(StringIO(text))
    df.columns = [c.strip() for c in df.columns]

    if "Date" not in df.columns or "Price" not in df.columns:
        raise ValueError(f"[WTI] Unexpected GitHub CSV columns: {list(df.columns)}")

    df = df.rename(columns={"Date": "date", "Price": "wti_price"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["wti_price"] = pd.to_numeric(df["wti_price"], errors="coerce")
    df = df.dropna(subset=["date", "wti_price"])
    return df


def _parse_fred_csv(text: str) -> pd.DataFrame:
    """
    Parse FRED graph endpoint CSV.

    Format:
        DATE,DCOILWTICO
        2019-01-02,50.54
        ...
        (missing values represented as '.')
    """
    df = pd.read_csv(StringIO(text))
    df.columns = [c.strip() for c in df.columns]

    if "DATE" not in df.columns or "DCOILWTICO" not in df.columns:
        raise ValueError(f"[WTI] Unexpected FRED columns: {list(df.columns)}")

    df = df.rename(columns={"DATE": "date", "DCOILWTICO": "wti_price"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["wti_price"] = pd.to_numeric(df["wti_price"], errors="coerce")  # '.' -> NaN

    before = len(df)
    df = df.dropna(subset=["date", "wti_price"])
    dropped = before - len(df)
    if dropped > 0:
        logger.info("[WTI] Dropped %d rows with missing values (FRED '.' entries).", dropped)

    return df


_PARSERS = {
    "_parse_github_csv": _parse_github_csv,
    "_parse_fred_csv": _parse_fred_csv,
}


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


def _aggregate_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate daily WTI prices to monthly averages.

    Input:  date (daily), wti_price
    Output: date (YYYY-MM string), wti_price (monthly mean, 2 decimal places)
    """
    df = df.copy()
    df["year_month"] = df["date"].dt.to_period("M")

    monthly = df.groupby("year_month", as_index=False)["wti_price"].mean().round(2)
    monthly = monthly.rename(columns={"year_month": "date"})
    monthly["date"] = monthly["date"].astype(str)  # -> "YYYY-MM"
    monthly = monthly.sort_values(by="date").reset_index(drop=True)

    return monthly[["date", "wti_price"]]


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------


def _validate(df: pd.DataFrame) -> None:
    """Lightweight validation — warnings only, no hard failures."""

    # Missing values
    nulls = df["wti_price"].isna().sum()
    if nulls > 0:
        logger.warning("[WTI] %d rows with NULL wti_price after aggregation.", nulls)

    # Duplicates
    dupes = df.duplicated(subset=["date"]).sum()
    if dupes > 0:
        logger.warning("[WTI] %d duplicate date rows found.", dupes)

    # Chronological order
    if not df["date"].is_monotonic_increasing:
        logger.warning("[WTI] Dates are not in chronological order.")

    # Suspicious price range (crude oil: $5–$200 is reasonable history)
    out_of_range = df[(df["wti_price"] < 5) | (df["wti_price"] > 200)]
    if not out_of_range.empty:
        logger.warning("[WTI] %d rows with price outside $5–$200 range:\n%s", len(out_of_range), out_of_range)

    # Suspicious spikes: month-over-month change > 50%
    pct_change = df["wti_price"].pct_change().abs()
    spikes = df[pct_change > 0.5]
    if not spikes.empty:
        logger.warning("[WTI] %d rows with >50%% month-over-month price spike:\n%s", len(spikes), spikes)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _fetch_from_sources() -> pd.DataFrame:
    """Try each source in order; return daily DataFrame from first that succeeds."""
    last_exc: Exception = RuntimeError("No sources defined.")
    for source in _SOURCES:
        name = source["name"]
        url = source["url"]
        parser = _PARSERS[source["parser"]]
        try:
            logger.info("[WTI] Trying source: %s ...", name)
            resp = _fetch_with_retry(url)
            df = parser(resp.text)
            logger.info("[WTI] Success — %d daily rows from %s", len(df), name)
            return df
        except Exception as exc:
            logger.warning("[WTI] Source failed (%s): %s", name, exc)
            last_exc = exc
    raise RuntimeError(f"[WTI] All sources exhausted. Last error: {last_exc}")


def fetch_wti_monthly(out_csv: Path = WTI_OUT_CSV) -> pd.DataFrame:
    """
    Fetch WTI daily data, aggregate to monthly average, save to CSV.
    No temporary files written — all processing is in-memory.

    Returns DataFrame with columns: date (YYYY-MM), wti_price
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    daily_df = _fetch_from_sources()

    logger.info("[WTI] Aggregating to monthly averages ...")
    monthly_df = _aggregate_to_monthly(daily_df)
    logger.info("[WTI] %d monthly rows after aggregation.", len(monthly_df))

    _validate(monthly_df)

    monthly_df.to_csv(out_csv, index=False)
    logger.info("[WTI] Saved %d rows -> %s", len(monthly_df), out_csv)

    return monthly_df


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------


def validate_wti(df: pd.DataFrame) -> None:
    """Print a brief validation report for the WTI dataset."""
    print("\n" + "=" * 50)
    print("WTI VALIDATION REPORT")
    print("=" * 50)
    print(f"Total rows        : {len(df)}")
    print(f"Date range        : {df['date'].min()} -> {df['date'].max()}")
    print(f"Min price         : ${df['wti_price'].min():.2f}/bbl")
    print(f"Max price         : ${df['wti_price'].max():.2f}/bbl")
    print(f"Mean price        : ${df['wti_price'].mean():.2f}/bbl")
    print(f"NULL values       : {df['wti_price'].isna().sum()}")
    print(f"Duplicates        : {df.duplicated(subset=['date']).sum()}")

    # Sanity check: COVID 2020 avg should be lower than 2022 oil shock avg
    covid = df[df["date"].str.startswith("2020")]["wti_price"].mean()
    shock = df[df["date"].str.startswith("2022")]["wti_price"].mean()
    if pd.notna(covid) and pd.notna(shock):
        tag = "[OK]" if covid < shock else "[WARNING]"
        print(f"{tag} Sanity: 2020 avg (${covid:.1f}) vs 2022 avg (${shock:.1f})")

    print("=" * 50)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    df = fetch_wti_monthly()
    validate_wti(df)
    print("\nLast 12 rows:")
    print(df.tail(12).to_string(index=False))
