"""Daily WTI (Cushing) and Brent (Europe) spot prices -> calendar-month averages.

Primary: U.S. EIA Open Data API v2 (public domain, requires a free API key in ``EIA_API_KEY``).
  * series RWTC = Cushing, OK WTI spot price FOB, USD/bbl, daily
  * series RBRTE = Europe Brent spot price FOB, USD/bbl, daily
  * route: /v2/petroleum/pri/spt/data/ ; publication delay ~1 week; occasional revisions.
Fallback (no key): the ``datasets/oil-prices`` mirror of the same EIA series (ODC-PDDL-1.0,
  https://github.com/datasets/oil-prices), refreshed daily by its maintainers.
FRED (DCOILWTICO / DCOILBRENTEU) carries the same EIA data but FRED's terms are non-commercial,
so it is deliberately not used by this product.

Monthly aggregation: arithmetic mean of the available daily closes in the calendar month. A
month is reported ``complete`` only after it has ended, with >= 60 % of business days observed
and either the last observation in its final week or >= 7 days of settling time.
"""

from __future__ import annotations

import io
import os
from datetime import date
from typing import Any

import pandas as pd

from icp_ml.sources.base import (
    HttpClient,
    SourceDocument,
    SourceError,
    SourceFetch,
    aggregate_daily_to_monthly,
    utcnow_iso,
)

EIA_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
MIRROR = {
    "wti": "https://raw.githubusercontent.com/datasets/oil-prices/master/data/wti-daily.csv",
    "brent": "https://raw.githubusercontent.com/datasets/oil-prices/master/data/brent-daily.csv",
}
EIA_SERIES = {"wti": "RWTC", "brent": "RBRTE"}
DEFINITION = {
    "wti": "Cushing, OK WTI spot price FOB (EIA RWTC), monthly mean of daily prices, USD/bbl.",
    "brent": "Europe Brent spot price FOB (EIA RBRTE), monthly mean of daily prices, USD/bbl.",
}


def _fetch_eia_daily(
    client: HttpClient, series: str, api_key: str, start: str
) -> tuple[pd.DataFrame, list[SourceDocument]]:
    rows: list[dict[str, Any]] = []
    docs: list[SourceDocument] = []
    offset = 0
    while True:
        params = {
            "api_key": api_key,
            "frequency": "daily",
            "data[0]": "value",
            "facets[series][]": series,
            "start": start,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": 5000,
        }
        body, doc = client.get(EIA_URL, params=params, max_age_s=6 * 3600)
        docs.append(doc)
        import json

        payload = json.loads(body)
        if "response" not in payload:
            raise SourceError(f"EIA error: {payload.get('error') or payload}")
        data = payload["response"]["data"]
        rows.extend({"date": d["period"], "value": d["value"]} for d in data if d.get("value") is not None)
        total = int(payload["response"].get("total", len(rows)))
        offset += len(data)
        if not data or offset >= total:
            break
    df = pd.DataFrame(rows, columns=["date", "value"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df, docs


def _fetch_mirror_daily(client: HttpClient, dataset: str) -> tuple[pd.DataFrame, list[SourceDocument]]:
    body, doc = client.get(MIRROR[dataset], max_age_s=6 * 3600)
    df = pd.read_csv(io.BytesIO(body))
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns or "price" not in df.columns:
        raise SourceError(f"Unexpected columns in {MIRROR[dataset]}: {list(df.columns)}")
    df = df.rename(columns={"price": "value"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df[["date", "value"]], [doc]


def fetch_benchmark(
    client: HttpClient,
    dataset: str,
    start: str = "1986-01-01",
    today: date | None = None,
    api_key: str | None = None,
) -> SourceFetch:
    if dataset not in EIA_SERIES:
        raise ValueError(dataset)
    api_key = api_key if api_key is not None else os.getenv("EIA_API_KEY", "")
    warnings: list[str] = []
    if api_key:
        try:
            daily, docs = _fetch_eia_daily(client, EIA_SERIES[dataset], api_key, start)
            provider, license_note = (
                "U.S. Energy Information Administration (EIA Open Data v2)",
                "U.S. Government work, public domain; cite EIA.",
            )
        except SourceError as exc:
            warnings.append(f"EIA API failed ({exc}); used the datasets/oil-prices mirror instead")
            daily, docs = _fetch_mirror_daily(client, dataset)
            provider, license_note = (
                "datasets/oil-prices mirror of EIA",
                "ODC-PDDL-1.0 (mirror of EIA public-domain data).",
            )
    else:
        daily, docs = _fetch_mirror_daily(client, dataset)
        provider, license_note = (
            "datasets/oil-prices mirror of EIA",
            "ODC-PDDL-1.0 (mirror of EIA public-domain data); set EIA_API_KEY to read EIA directly.",
        )
    daily = daily.dropna().drop_duplicates("date").sort_values("date")
    daily = daily[daily["date"] >= start]
    bad = daily[(daily["value"] <= 0) | (daily["value"] > 500)]
    if not bad.empty:
        warnings.append(
            f"{len(bad)} daily {dataset} rows outside (0, 500] dropped: {bad['date'].tolist()[:5]}"
        )
        daily = daily.drop(bad.index)
    monthly = aggregate_daily_to_monthly(daily, "value", "mean", today=today)
    return SourceFetch(
        dataset=dataset,
        provider=provider,
        series_id=EIA_SERIES[dataset],
        unit="USD per barrel",
        frequency="daily->monthly",
        definition=DEFINITION[dataset],
        monthly=monthly,
        documents=docs,
        retrieved_at=utcnow_iso(),
        license_note=license_note,
        warnings=warnings,
    )


__all__ = ["EIA_SERIES", "MIRROR", "fetch_benchmark"]
