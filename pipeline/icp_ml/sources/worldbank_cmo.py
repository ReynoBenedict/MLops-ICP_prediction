"""World Bank Commodity Markets "Pink Sheet": monthly average crude prices (Brent, Dubai, WTI, average).

Source: CMO-Historical-Data-Monthly.xlsx linked from
https://www.worldbank.org/en/research/commodity-markets (sheet "Monthly Prices").
* Unit: nominal US$ per barrel, monthly averages of daily quotations.
* Frequency: monthly; published in the first week of the following month (the file header
  carries an "Updated on <date>" line which is recorded as the publication reference).
* History: 1960M01 onward (WTI from 1982; Dubai from 1960).
* Revisions: the latest month can be revised in the next release; earlier months are stable.
* Definitions (World Bank): Brent = light blend 38 API, f.o.b. U.K.; Dubai = Fateh 32 API,
  f.o.b. Dubai; WTI = 40 API, f.o.b. Midland Texas; average = equally weighted Brent/Dubai/WTI.
* License: World Bank Data Catalog "Commodity Price Data" - Creative Commons Attribution 4.0
  (CC BY 4.0). Attribution: "World Bank Commodity Price Data (The Pink Sheet)".
"""

from __future__ import annotations

import re
from datetime import date

import pandas as pd

from icp_ml.sources.base import HttpClient, SourceError, SourceFetch, ValueEvidence, utcnow_iso

PAGE_URL = "https://www.worldbank.org/en/research/commodity-markets"
LICENSE_NOTE = "World Bank Commodity Price Data (The Pink Sheet), CC BY 4.0; attribution required."
COLUMNS = {
    "brent": ("Crude oil, Brent", "brent_price"),
    "dubai": ("Crude oil, Dubai", "dubai_price"),
    "wti_cmo": ("Crude oil, WTI", "wti_price"),
    "crude_avg": ("Crude oil, average", "crude_avg_price"),
}


def discover_workbook_url(client: HttpClient) -> str:
    body, _ = client.get(PAGE_URL, max_age_s=6 * 3600)
    m = re.search(
        r'href="(https://thedocs\.worldbank\.org/[^"]*CMO-Historical-Data-Monthly\.xlsx)"',
        body.decode("utf-8", "replace"),
    )
    if not m:
        raise SourceError(
            "Could not find CMO-Historical-Data-Monthly.xlsx link on the World Bank commodity page"
        )
    return m.group(1)


def fetch_cmo_monthly(client: HttpClient, today: date | None = None) -> dict[str, SourceFetch]:
    import io

    url = discover_workbook_url(client)
    body, doc = client.get(url, max_age_s=24 * 3600)
    raw = pd.read_excel(io.BytesIO(body), sheet_name="Monthly Prices", header=None)
    updated = next((str(v) for v in raw.iloc[:5, 0].tolist() if str(v).lower().startswith("updated on")), "")
    header = raw.iloc[4].tolist()
    data = raw.iloc[6:].copy()
    data = data.rename(columns={0: "period_raw"})
    data = data[data["period_raw"].astype(str).str.match(r"^\d{4}M\d{2}$")]
    data["period"] = data["period_raw"].astype(str).str.replace("M", "-", regex=False)
    today = today or date.today()
    current = f"{today.year}-{today.month:02d}"
    out: dict[str, SourceFetch] = {}
    for dataset, (label, _) in COLUMNS.items():
        try:
            col = header.index(label)
        except ValueError as exc:
            raise SourceError(f"Column '{label}' not found in World Bank workbook") from exc
        series = pd.to_numeric(data[col], errors="coerce")
        df = pd.DataFrame({"period": data["period"].to_numpy(), "value": series.to_numpy()}).dropna()
        df["value"] = df["value"].astype(float).round(4)
        df["n_obs"] = 1
        df["expected_obs"] = 1
        # World Bank publishes only completed months; guard anyway against a current-month row.
        df["complete"] = df["period"] < current
        evidence = [
            ValueEvidence(
                str(p),
                float(v),
                url,
                doc.sha256,
                "xlsx",
                "verified",
                doc.retrieved_at,
                reference=f"Monthly Prices!{label}; {updated}",
            )
            for p, v in zip(df["period"], df["value"], strict=True)
        ]
        out[dataset] = SourceFetch(
            dataset=dataset,
            provider="World Bank",
            series_id=label,
            unit="USD per barrel",
            frequency="monthly",
            definition=f"World Bank Pink Sheet monthly average, {label} ({updated}).",
            monthly=df.reset_index(drop=True),
            documents=[doc],
            evidence=evidence,
            retrieved_at=utcnow_iso(),
            license_note=LICENSE_NOTE,
        )
    return out


__all__ = ["COLUMNS", "PAGE_URL", "discover_workbook_url", "fetch_cmo_monthly"]
