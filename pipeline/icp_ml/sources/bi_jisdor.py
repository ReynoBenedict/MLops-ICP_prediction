"""Bank Indonesia JISDOR (Jakarta Interbank Spot Dollar Rate), the official USD/IDR reference rate.

Source: Bank Indonesia public web service ``wskursbi.asmx`` (operation ``getSubKursJisdor3``),
the same data shown at https://www.bi.go.id/id/statistik/informasi-kurs/jisdor/Default.aspx.
* Series: JISDOR USD/IDR, one fixing per Jakarta business day, unit IDR per 1 USD.
* History: from 20 May 2013 (JISDOR launch).
* Date convention: Jakarta (WIB, UTC+7) fixing date.
* Publication: same business day around 10:00 WIB. Not revised.
* Terms: Bank Indonesia publishes JISDOR as public statistical information; cite
  "Bank Indonesia - JISDOR". Commercial redistribution of the raw daily series should be
  reviewed against BI's website terms (see docs/DATA_SOURCES.md).

The service is queried in quarterly windows (a full-year request was observed to return a
truncated range), and every window's row count is checked against the business-day count.
"""

from __future__ import annotations

from datetime import date

import defusedxml.ElementTree as ET  # hardened parser: external entities / billion-laughs are rejected
import pandas as pd

from icp_ml.sources.base import (
    HttpClient,
    SourceDocument,
    SourceError,
    SourceFetch,
    aggregate_daily_to_monthly,
    utcnow_iso,
)

ENDPOINT = "https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursJisdor3"
PAGE_URL = "https://www.bi.go.id/id/statistik/informasi-kurs/jisdor/Default.aspx"
FIRST_DATE = date(2013, 5, 20)
LICENSE_NOTE = "Bank Indonesia JISDOR, public statistical release; attribution 'Bank Indonesia'. Review BI terms before redistributing raw daily data."


def _parse_dataset_xml(body: bytes) -> pd.DataFrame:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise SourceError(f"JISDOR response is not XML: {exc}") from exc
    rows = []
    for table in root.iter():
        if table.tag.split("}")[-1] != "Table":
            continue
        rec = {child.tag.split("}")[-1]: (child.text or "").strip() for child in table}
        if rec.get("mts_subkursasing", "USD") != "USD":
            continue
        rows.append(
            {
                "date": rec["tgl_subkursasing"][:10],
                "usd_idr": float(rec["beli_subkursasing"]) / float(rec.get("nil_subkursasing") or 1),
            }
        )
    return pd.DataFrame(rows, columns=["date", "usd_idr"])


def _quarters(start: date, end: date) -> list[tuple[date, date]]:
    out = []
    cur = date(start.year, 3 * ((start.month - 1) // 3) + 1, 1)
    while cur <= end:
        q_end_month = cur.month + 2
        last = date(cur.year, q_end_month, pd.Period(f"{cur.year}-{q_end_month:02d}", freq="M").days_in_month)
        out.append((max(cur, start), min(last, end)))
        cur = date(cur.year + (1 if q_end_month == 12 else 0), 1 if q_end_month == 12 else q_end_month + 1, 1)
    return out


def fetch_jisdor_daily(
    client: HttpClient,
    start: date = FIRST_DATE,
    end: date | None = None,
    cache_closed_quarters_s: int = 30 * 86400,
) -> tuple[pd.DataFrame, list[SourceDocument], list[str]]:
    end = end or date.today()
    frames, docs, warnings = [], [], []
    for q_start, q_end in _quarters(start, end):
        closed = q_end < end and (end - q_end).days > 10
        body, doc = client.get(
            ENDPOINT,
            params={"mts": "USD", "startDate": q_start.isoformat(), "endDate": q_end.isoformat()},
            max_age_s=cache_closed_quarters_s if closed else 6 * 3600,
        )
        part = _parse_dataset_xml(body)
        docs.append(doc)
        expected = len(pd.bdate_range(q_start, q_end))
        if closed and len(part) < 0.7 * expected:
            warnings.append(
                f"JISDOR window {q_start}..{q_end}: only {len(part)} of ~{expected} business days returned"
            )
        frames.append(part)
    daily = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date", "usd_idr"])
    daily = daily.drop_duplicates("date").sort_values("date").reset_index(drop=True)
    bad = daily[(daily["usd_idr"] < 5000) | (daily["usd_idr"] > 30000)]
    if not bad.empty:
        warnings.append(
            f"{len(bad)} JISDOR rows outside 5,000-30,000 IDR/USD were dropped: {bad['date'].tolist()[:5]}"
        )
        daily = daily.drop(bad.index)
    return daily, docs, warnings


def fetch_usd_idr(
    client: HttpClient, start: date = FIRST_DATE, end: date | None = None, today: date | None = None
) -> dict[str, SourceFetch]:
    """Return two monthly datasets: ``usd_idr`` (monthly average) and ``usd_idr_eom`` (month-end fixing)."""
    daily, docs, warnings = fetch_jisdor_daily(client, start, end)
    today = today or date.today()
    out = {}
    for name, how, definition in (
        (
            "usd_idr",
            "mean",
            "Arithmetic mean of daily JISDOR USD/IDR fixings over the calendar month (Jakarta business days).",
        ),
        ("usd_idr_eom", "last", "Last JISDOR USD/IDR fixing published in the calendar month."),
    ):
        monthly = aggregate_daily_to_monthly(
            daily.rename(columns={"usd_idr": "value"}),
            "value",
            how,
            today=today,
            min_coverage=0.6,
            settle_days=1,
        )
        out[name] = SourceFetch(
            dataset=name,
            provider="Bank Indonesia",
            series_id="JISDOR USD/IDR",
            unit="IDR per USD",
            frequency="daily->monthly",
            definition=definition,
            monthly=monthly,
            documents=docs,
            retrieved_at=utcnow_iso(),
            license_note=LICENSE_NOTE,
            warnings=list(warnings),
        )
    return out


__all__ = ["ENDPOINT", "FIRST_DATE", "PAGE_URL", "fetch_jisdor_daily", "fetch_usd_idr"]
