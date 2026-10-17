# Data dictionary

*Technical reference (English). Indonesian labels are what the product shows; they come from
`DATASET_SPECS` in `pipeline/icp_ml/schema.py`.*

## 1. Monthly series (datasets)

All datasets are monthly, keyed by `period` = `YYYY-MM` (calendar month). A dataset version is an
immutable CSV; the accepted layouts are `period,<value column>` or, for ICP, `month,year,icp_price`.

| Dataset id | Value column | Label (id) | Unit | Definition | Valid range | Publication lag |
|---|---|---|---|---|---|---|
| `icp` | `icp_price` | Harga Minyak Mentah Indonesia (ICP) | USD/bbl | Official monthly ICP set by ESDM decree for the month | 5 – 300 | 1 month |
| `wti` | `wti_price` | WTI (Cushing) | USD/bbl | Mean of EIA daily spot prices in the month (complete months only) | 0 – 300 (negative April 2020 handled as warning) | 1 month |
| `brent` | `brent_price` | Brent (Eropa) | USD/bbl | Mean of EIA daily Brent spot FOB prices in the month | 0 – 300 | 1 month |
| `dubai` | `dubai_price` | Dubai (Fateh) | USD/bbl | World Bank Pink Sheet monthly average | 0 – 300 | 1 month |
| `usd_idr` | `usd_idr` | Kurs USD/IDR (JISDOR, rata-rata) | IDR per USD | Arithmetic mean of daily JISDOR fixings in the month | 5 000 – 50 000 | 1 month |
| `usd_idr_eom` | `usd_idr_eom` | Kurs USD/IDR (JISDOR, akhir bulan) | IDR per USD | Last JISDOR fixing published in the month | 5 000 – 50 000 | 1 month |

Derived (never stored as a dataset): `icp_idr = icp_price × usd_idr` (rupiah per barrel, labelled
"turunan"), spreads `icp_price − <benchmark>`, month-over-month and year-over-year changes.

## 2. Value kinds shown in the product

| Kind | Meaning |
|---|---|
| observed (teramati) | Published by the source for a completed month |
| derived (turunan) | Arithmetic on observed values (e.g. rupiah equivalent, spread, % change) |
| estimate awaiting publication (estimasi – menunggu publikasi) | Month ended, ICP not yet published; benchmarks observed |
| future forecast (proyeksi) | Month not ended; conditional on scenario or held-last benchmarks |
| scenario (asumsi skenario) | User-supplied input |
| assumed_hold_last (nilai terakhir dipertahankan) | System assumption when no scenario is given |
| legacy_record (rekam lama) | Forecast record from before the 2026-10 redesign |

## 3. Verification status of an ICP month (`source_evidence.verification_status`)

| Status | Meaning |
|---|---|
| `verified` | Two independent official documents (decree + announcement, or two announcements) agree |
| `single_source` | One official document (typically an announcement); accepted and flagged |
| `machine_extracted` | OCR of a scanned decree only; **not** accepted into the dataset until confirmed |
| `conflict` | Official documents disagree; month excluded until resolved and documented |
| `legacy_unverified` | Value inherited from the repository CSV without a document (being replaced) |
| `rejected` | Superseded legacy value (kept for audit) |

## 4. Features (`pipeline/icp_ml/features.py`, `FEATURE_COLUMNS`)

| Feature | Definition (target month *t*) |
|---|---|
| `wti_price`, `brent_price`, `dubai_price` | Benchmark monthly average for *t* (observed/scenario/held) |
| `icp_lag_1`, `icp_lag_2`, `icp_lag_3` | ICP for *t−1*, *t−2*, *t−3* (official up to the origin, predicted after it) |
| `icp_mean_3` | Mean of `icp_lag_1..3` |
| `wti_lag_1`, `brent_lag_1`, `dubai_lag_1` | Benchmark average for *t−1* |
| `spread_<b>_lag_1` | `icp − <b>` for *t−1* |
| `spread_<b>_mean_3` | Mean spread over *t−3..t−1* |
| `usd_idr` | JISDOR monthly average for *t* |

Rows are only complete when every required feature exists; gaps are never bridged.

## 5. Application tables (metadata store, migrations 0001–0004)

| Table | Purpose |
|---|---|
| `dataset_versions` | Immutable dataset versions (object key, sha256, period range, quality, diff, provenance, active flag) |
| `source_documents`, `source_evidence`, `ingestion_runs` | Provenance of every value and every ingestion attempt |
| `forecast_runs` | Every forecast (params, result, model/dataset versions, origin, issue date, scenario, workflow, org) |
| `model_promotions` | Production alias changes (promote/rollback, actor, reason, validation report) |
| `jobs` | Background jobs (status, attempts, heartbeat, result, errors) |
| `audit_events` | Security-relevant actions incl. denied attempts (no secrets, no raw IPs) |
| `organizations`, `users`, `sessions`, `api_keys` | Tenants, members (scrypt password hashes), sessions (token hashes), API keys (hashes) |
| `saved_scenarios` | Named scenario inputs per organisation |
| `alert_rules`, `alert_events` | Alert configuration and de-duplicated in-app notifications |
| `report_runs` | Generated reports (object keys per format, parameters, summary) |
| `usage_events` | Usage accounting per organisation/user/API key |

## 6. Forecast record (`forecast_runs.result_json`, API `ForecastResult`)

```
forecast_origin  YYYY-MM   last official ICP month used
issue_date       YYYY-MM-DD
horizon, method, model{version, method, method_family, rule_based, description}
points[]: period, step, kind, prediction (USD/bbl), intervals{"0.5"|"0.8"|"0.95": [low, high]},
          interval_basis (observed|hold_last|null), conditional_on_scenario,
          benchmarks{brent,dubai,wti: {value, source, used_by_model}}, recursive,
          prediction_idr, fx_rate, fx_source
warnings[], interval_levels[], interval_note
```
