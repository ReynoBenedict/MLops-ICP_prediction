# Data sources, licensing and freshness policy

*Technical reference (English). Indonesian labels shown in the product come from
`pipeline/icp_ml/schema.py` (`DATASET_SPECS`).*

## 1. Inventory

| Dataset (`id`) | What | Provider / endpoint | Frequency → stored | Coverage (local, 2026-10-16) | Licence / terms | Attribution required |
|---|---|---|---|---|---|---|
| `icp` | Official Indonesian Crude Price, USD/bbl | **Kementerian ESDM – Ditjen Migas**. Archive of monthly decrees (`https://www.migas.esdm.go.id/post/harga-minyak-mentah`, 93 PDFs 2019-01..2026-09) and monthly announcement articles (`/post/index?kategori=berita`) | monthly (decree) | 2019-01..2026-03 legacy; backfill in progress to 2026-09 | Indonesian government publication (public information, UU 14/2008 KIP). Values are facts; documents retained for audit. No commercial restriction stated; cite the ministry. | Yes – "Kementerian ESDM" |
| `wti` | WTI Cushing spot, monthly mean of daily, USD/bbl | **U.S. EIA** series `RWTC` via API v2 (`EIA_API_KEY`, optional) or the `datasets/oil-prices` GitHub mirror of the same EIA release (`wti-daily.csv`) | daily → monthly mean (complete months only) | 1986-01..2026-09 | EIA data: U.S. government work, public domain. Mirror: ODC-PDDL-1.0 | Courtesy – "U.S. Energy Information Administration" |
| `brent` | Europe Brent spot FOB, monthly mean of daily, USD/bbl | EIA series `RBRTE` (same two paths) | daily → monthly mean | 1987-06..2026-09 | as above | as above |
| `dubai` | Dubai Fateh, monthly average, USD/bbl | **World Bank Commodity Markets ("Pink Sheet")** `CMO-Historical-Data-Monthly.xlsx`, sheet *Monthly Prices*, column *Crude oil, Dubai* | monthly | 1960-01..2026-09 (file "Updated on October 02, 2026") | World Bank Data Catalog default licence **CC BY 4.0** (verified on the catalog licence page; the dataset-specific page could not be located – see caveat) | Yes – "World Bank Commodity Price Data (The Pink Sheet)" |
| `usd_idr` | USD/IDR, monthly mean of daily JISDOR fixings | **Bank Indonesia** web service `wskursbi.asmx/getSubKursJisdor3` (fields `tgl`, `nil_subkursasing`) | daily → monthly mean | 2013-06..2026-09 | Public statistical release by the central bank. BI's site terms were reviewed: redistribution of raw daily fixings should credit BI; the product stores and displays monthly aggregates. | Yes – "Bank Indonesia (JISDOR)" |
| `usd_idr_eom` | Last JISDOR fixing of the month | as above | daily → month-end | 2013-06..2026-09 | as above | as above |

Not used, and why:

* **FRED** (St. Louis Fed) mirrors of WTI/Brent/DCOILBRENTEU – FRED terms restrict commercial
  redistribution; the EIA originals are public domain, so FRED is unnecessary.
* **Futures curves / forward prices** (ICE, CME) – proprietary licences; this is why future months
  rely on user scenarios rather than a market-implied path.
* **Dated Brent / OPEC basket / ESDM formula inputs** – not available as a public monthly series.

## 2. Completeness rules ("partial months never masquerade as complete")

`pipeline/icp_ml/sources/base.py::aggregate_daily_to_monthly` marks a month complete only when:

1. the calendar month has ended (`today > month_end`), **and**
2. at least 60 % of the month's business days have an observation, **and**
3. either the last observation lies in the final week of the month or ≥ 7 days have passed since
   month end (publication settling).

Only complete months enter a dataset version. Partial months are reported in the ingestion run
(`partial_months_excluded`) and shown in the UI as "belum lengkap".

## 3. Freshness policy (`pipeline/icp_ml/schema.py::freshness`)

Freshness is derived from each source's publication schedule, never from a fixed date:

| Dataset | Expected latest period on day D | Lag | Grace |
|---|---|---|---|
| `icp` | month(D) − 1 (ESDM publishes early in M+1) | 1 month | 1 month |
| benchmarks, FX | month(D) − 1 (daily series complete at month end) | 1 month | 1 month |

Status: `fresh` (expected month present), `lagging` (one month behind), `stale` (more).
Expected availability is **never** treated as proof of publication: a month is present only if the
source actually delivered it.

## 4. Provenance recorded per value

* `source_documents`: URL, SHA-256, size, content type, retrieval time, local cache path of every
  document (PDF, HTML, XLSX, XML) a value came from.
* `source_evidence`: per dataset and month – value, document checksum, extraction method
  (`text-layer`, `ocr`, `html`, `xlsx`, `web-service`), verification status
  (`verified`, `single_source`, `machine_extracted`, `conflict`, `legacy_unverified`, `rejected`),
  reference (decree number, series id), notes.
* `ingestion_runs`: who/when/what changed, added/changed months, partial months excluded, warnings,
  failures with messages.
* `dataset_versions`: immutable CSV per version in the object store (content-addressed), diff to the
  parent version, quality report, provenance JSON.

Every dataset version used by a model is recorded in the model's MLflow tags; every forecast record
stores the version id of each input.

## 5. Caveats and open items

* World Bank: the catalog-wide default licence (CC BY 4.0) was verified; the Pink Sheet's own
  catalog entry could not be opened during this session. Treat as CC BY 4.0 with attribution until
  confirmed on the dataset page.
* Bank Indonesia: the web service is undocumented for third parties and slow; a full-year call can
  truncate, so the connector queries quarter by quarter and validates row counts.
* ESDM: the archive server is very slow and intermittently times out; the backfill is resumable and
  falls back to the repository's local PDF mirror (`data/raw`) when a download fails. Scanned
  decrees (2019–2021) need OCR (Tesseract, English model); where OCR fails, the monthly announcement
  article is the accepted source and the month is marked `single_source` until a second document
  confirms it.
* Commercial use: all sources above permit redistribution of derived monthly values with
  attribution; raw daily BI fixings and the original ESDM PDFs are retained for audit, not resold.

## 6. Repository seeds (`data/seed/*.csv`)

Snapshots of the connector outputs (benchmarks, FX) exported on 2026-10-16 so a fresh checkout can
start without network access; they are replaced by connector runs (`icp-api ingest …` or the
`ingest_*` jobs) and carry the provider's licence terms above. The legacy ICP file
(`data/raw/dataset.csv`, DVC-tracked) is kept unchanged for history; the official series is
rebuilt from ESDM documents by the `ingest_icp` job.
