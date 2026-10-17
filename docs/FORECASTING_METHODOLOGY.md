# Forecasting methodology

*Technical reference (English). The business-facing explanation lives in the application
("Proyeksi ICP → Tentang metode ini") and in generated reports.*

## 1. Target and timing

| Term | Definition |
|---|---|
| **Target** | Official monthly Indonesian Crude Price (ICP) in USD/bbl, as set by the Ministry of Energy and Mineral Resources (ESDM) in a ministerial decree for calendar month *t*. |
| **Publication** | Early in month *t+1* (typically the first or second week). |
| **Forecast origin** | The latest month with an *official, verified* ICP value available to the system. |
| **Issue date** | The calendar day a forecast was produced. The same target month forecast on different days can differ; old records are never rewritten. |
| **Horizon h** | Months after the origin (1–6). |

Each output month carries one of these kinds:

| Kind | When | Benchmarks for that month | Error reference |
|---|---|---|---|
| `estimate_awaiting_publication` | month has ended, ESDM has not published | **observed** monthly averages | `observed` backtest mode |
| `future_forecast` | month has not ended / not started | **scenario** (user) or **assumed_hold_last** (last known value) | `hold_last` backtest mode (or conditional on scenario) |
| `legacy_record` | forecast records created before 2026-10 | WTI only | n/a |

Historical backtest predictions are a separate concept (`evaluation/backtest_predictions.csv` per
model run) and are never shown as forecasts.

## 2. Inputs and feature construction (`pipeline/icp_ml/features.py`)

The panel is a monthly calendar with `icp_price`, `wti_price`, `brent_price`, `dubai_price`,
`usd_idr`. Features for target month *t*:

* same-month benchmark averages: `wti_price`, `brent_price`, `dubai_price` (observed or assumed);
* ICP lags `icp_lag_1..3`, `icp_mean_3` – official values up to the origin, the method's own
  earlier predictions after it (recursive multi-step);
* benchmark lags `*_lag_1` and spreads `spread_*_lag_1`, `spread_*_mean_3` (ICP − benchmark);
* `usd_idr` (monthly JISDOR average).

Rules: lags are computed on a complete calendar and **never bridge a missing month**; a target
whose required inputs are missing is excluded from training and refused at inference
(`MissingFeatureData`). `feature_row()` (inference) is property-tested to equal the corresponding
row of `build_feature_frame()` (training) – training/serving parity.

No feature uses information published after the origin except the target month's benchmark
averages in *observed* mode, which are available by construction (the month has ended).

## 3. Candidate methods (`pipeline/icp_ml/methods.py`)

All methods implement the same `fit/predict` interface over the named feature frame and declare
the inputs they require, so they are backtested, registered, validated and served identically.

| Method | Family | Rule |
|---|---|---|
| `naive_last` | baseline | ICP(t) = ICP(t−1) |
| `spread_wti_all` | spread rule | WTI(t) + mean spread over the whole training history (the former "WTI + average spread" baseline) |
| `spread_{wti,brent,dubai}_{6m,12m}` | spread rule | benchmark(t) + mean spread over the last 6/12 months |
| `blend_benchmarks` | blend | non-negative least squares on Brent/Dubai/WTI(t) + intercept |
| `ridge_benchmarks`, `ridge_full` | ridge regression | benchmarks(t), ICP lags, spreads (+ FX for `ridge_full`) |
| `gbm_benchmarks` | gradient boosting | benchmarks(t), ICP lag 1, Brent spread lag 1 |

## 4. Evaluation (`pipeline/icp_ml/backtest.py`)

**Rolling-origin backtest.** For every origin *L* (each official month with ≥ 24 training rows)
each method is refitted on history ≤ L and asked for h = 1..6 in two input modes:

* `observed` – benchmark averages of the target months are the realised values (relevant for
  estimates awaiting publication);
* `hold_last` – benchmarks beyond L are held at their last value known at L (relevant for genuine
  future months without a scenario).

Metrics per (method, mode, horizon): MAE, RMSE, bias, sMAPE, **MASE** (scaled by the in-sample
one-step naive MAE at each origin), directional accuracy vs ICP(L), error quantiles.

**Fair comparison.** The leaderboard uses only (origin, horizon) pairs that *every* method produced
(`common_subset`), so methods needing more history are not compared on easier windows.

**Selection** (`configs/training.yaml`): MAE averaged over h = 1..3 in `observed` mode. A rule-based
method is preferred over an ML method when it is within 2 % of the best score ("no model is
promoted solely because it is more complex").

**Uncertainty.** Prediction intervals are empirical quantiles (50/80/95 %) of the backtest errors
per mode and horizon, stored with each model (`evaluation/intervals.json`). An out-of-sample
coverage check (intervals fitted on the first 60 % of origins, evaluated on the rest) is logged as
`evaluation/interval_coverage.json`. Scenario-based points use the `observed` table and are flagged
`conditional_on_scenario`.

## 5. Results on the reconciled official series (2026-10-17, local store, model v5)

Official ICP 2019-01..2026-09 (93 months, no gaps after reconciliation), benchmarks to 2026-09;
69 rolling origins (2020-12..2026-08), MAE in USD/bbl on the common comparison set
(`evaluation/backtest_metrics_common.csv` of MLflow parent run `f8785e73…`):

| Method | observed h1 | observed h3 | observed h6 | hold_last h1 | hold_last h3 | hold_last h6 |
|---|---|---|---|---|---|---|
| naive_last | 6.09 | 10.66 | 13.40 | 6.09 | 10.66 | 13.40 |
| spread_wti_all (old baseline) | 2.00 | 2.01 | 1.97 | 6.17 | 10.25 | 13.44 |
| spread_wti_12m | 1.76 | 1.86 | 2.05 | 6.17 | 10.59 | 13.73 |
| **spread_brent_6m (promoted v5)** | **0.85** | **1.00** | **1.07** | 6.12 | 10.60 | 13.54 |
| spread_brent_12m (previous v4) | 0.87 | 1.00 | 1.22 | 6.23 | 10.73 | 13.64 |
| blend_benchmarks | 1.09 | 1.24 | 1.33 | 6.10 | 10.59 | 13.46 |
| ridge_benchmarks | 1.51 | 2.47 | 3.11 | 6.09 | 10.22 | 13.84 |
| gbm_benchmarks (old production family) | 2.35 | 3.66 | 5.73 | 6.16 | 10.64 | 13.23 |

Reading: with the target month's benchmarks known (an elapsed month awaiting publication) the
Brent + 6-month-spread rule explains ICP to within ≈ USD 0.85/bbl one month ahead (MASE ≈ 0.14,
selection score 0.93 over h1–h3); the previously deployed gradient-boosting family is 2–5× worse
and degrades with horizon. For genuinely future months with no benchmark view, **no method beats
persistence** (all ≈ 6.1 at h1, within the 10 % gate); the product therefore presents future months
as conditional on the benchmark assumption and shows the `hold_last` error band.

Out-of-sample 80 % interval coverage for v5 (intervals fitted on the first 60 % of origins, tested on
the rest): observed h1 0.96, h2–h6 1.00 (conservative); hold_last h1 0.86, h2 0.82, h3 0.65, h4 0.76,
h5 0.88, h6 0.87 – the hold_last band under-covers at h3, which the model card reports.

Earlier result on the legacy (partly wrong) series, kept for traceability: 55 origins, best
spread_brent_12m with h1 MAE 1.15 (v4); the corrections 2021-10, 2022-01, 2022-03, 2023-07 and the
14 added months moved the leaderboard but not its shape.

## 6. Model registration, validation and promotion

* Every training run logs config (+ SHA-256), dataset version ids and checksums, the full
  backtest tables, a leaderboard, and per method: metrics, predictions, intervals, coverage,
  **model card** (`model_card.md`), and the model serialized with **skops** (no pickle; trusted
  types are an explicit allow-list).
* Only the winner is registered. Promotion to the `production` alias is a separate step with
  gates: version ready; method spec present; model loads and predicts finite positive values;
  signature matches the feature schema; **beats naive** (observed h1 MAE); **no-scenario h1 MAE
  not worse than naive by > 10 %**; not worse than the current production score by > 5 %.
* Promotion/rollback history is stored in the application database with actor, reason and the
  validation report; rollback re-points the alias to the previous recorded version.

## 7. Serving semantics (`pipeline/icp_ml/forecasting.py`, `backend/icp_api/services/forecasts.py`)

`make_forecast(model, spec, panel, horizon, scenario, intervals, today)` returns, per month:
kind, prediction, intervals by level, interval basis, benchmark inputs with source
(`observed | scenario | assumed_hold_last`) and whether the model uses them, recursion flag,
rupiah equivalent (prediction × USD/IDR with its own source label). The forecast record stores the
origin, issue date, method, model version, scenario, dataset versions of every input, and the
workflow (`api`, `scenario`, `report`, `system`). The *system* forecast (shown publicly) is
republished after every data or model change.

## 8. Known limitations

* Short monthly history (2019–); errors are dominated by regime changes (2020, 2022, 2026).
* Benchmarks are monthly averages from public sources (EIA via mirror, World Bank); ESDM's own
  formula inputs (Dated Brent, OPEC basket, etc.) are not public as a monthly series.
* No forward curve or futures-based benchmark expectations are used (licensing); future months
  therefore rely on user scenarios.
* Intervals are empirical and unconditional on volatility regimes.
