# Migration from the Streamlit / LK-12 version

## What moved where

| Before | After |
|---|---|
| `app.py`, `pages/`, `components/`, `services/`, `utils/`, `config/`, `archived_pages/`, `.streamlit/`, `Dockerfile` (Streamlit UI) | `legacy/streamlit/`, not built or tested. Replaced by `apps/web` + `apps/gateway` + `backend/`. Moved rather than deleted because several files had uncommitted edits. |
| `serving/app.py`, `Dockerfile.serving`, `requirements.serving.txt`, `Dockerfile.mlflow` | `legacy/serving/`. Replaced by `backend/icp_api` (`/api/v1/forecasts` instead of the raw-feature `/predict`). |
| `src/data_ingestion/fetch_wti.py` | `pipeline/icp_ml/ingestion/wti.py` |
| `src/data_ingestion/run_ingestion.py`, `src/utils/text_parsing.py` | `pipeline/icp_ml/ingestion/icp_pdf.py`, `pipeline/icp_ml/ingestion/text_parsing.py` (CLI: `icp-ml ingest-icp-pdf`) |
| `src/data_processing/prepare_data.py`, `src/training/*.py`, `audit_and_promote.py` | Re-implemented in `pipeline/icp_ml/{features,training,registry,forecasting}.py`. Originals are in `legacy/pipeline-v1/` (and in Git history). |
| `src/data_processing/data_loader.py`, `preprocessing.py` and their tests | `legacy/pipeline-v1/`. They were unused by training and contained a silent dummy-data fallback. |
| `scripts/simulate_drift_trigger.py` | `legacy/pipeline-v1/`. Replaced by real PSI (`/api/v1/models/drift`). |
| `configs/config.yaml` | `legacy/pipeline-v1/configs/`. Replaced by `pipeline/configs/training.yaml`. |
| `monitoring/` | `infra/monitoring/` (Prometheus config and Grafana dashboard rewritten for the new metrics); the traffic scripts are in `legacy/evidence/monitoring-scripts/` |
| `metrics.json`, `ct_trigger_event.json`, `scaling-proof.txt`, `model_registry.yaml`, `reports/`, `models/` | `legacy/evidence/` (course evidence) |
| local `mlflow.db` (empty: Default experiment only) | removed 2026-10-17 (no runs or models; MLflow recreates it; the real history is in `legacy/mlflow-history/`) |
| MLflow history in Docker volumes `mlops-icp_prediction_old_pgdata` / `_mlflow-artifacts` | Exported read-only to `legacy/mlflow-history/`. The volumes themselves are untouched. |
| `.github/workflows/*.yml` (previous versions, including uncommitted edits) | `legacy/ci/`. New `ci.yml` and `continuous-training.yml` are in `.github/workflows/`. |
| `README.md` (course milestones LK-02 … LK-12) | `docs/history/course-milestones-README.md` |
| `requirements*.txt`, `pytest.ini`, `pyrightconfig.json`, `conftest.py`, `docker-compose.yaml` | `pyproject.toml` (deps, ruff, mypy, pytest), `requirements/constraints.txt`, `docker-compose.yml` |

`data/` (raw PDFs, `dataset.csv`, `wti.csv`, DVC metadata) and `notebooks/` are unchanged.

## Defects corrected

1. **Lags bridged calendar gaps.**
   - *Before:* `prepare_data.py` computed lags with row `shift()`. The dataset is missing 8 months, so
     for example `lag_1` for 2024-12 was actually June 2024's price, and the model trained on wrong
     features.
   - *Now:* lags are computed on a full monthly calendar and incomplete rows are dropped. A test
     asserts that the inference-time feature row equals the training feature row for every month.
2. **Wrong WTI feature at inference.**
   - *Before:* `utils/data_loader.get_latest_context` passed WTI of month N as `wti_price` for month
     N+1.
   - *Now:* the target month's WTI is used when observed. Otherwise it is an explicit, labelled
     assumption or an operator scenario.
3. **Partial-month WTI averages.**
   - *Before:* `fetch_wti` aggregated the current, incomplete month. The repository's
     `data/raw/wti.csv` (fetched on 2026-06-08) stores **2026-06 = 95.96**, while the full-month
     average fetched now is **84.81**.
   - *Now:* the incomplete current month is excluded, and `ingest_benchmarks` (formerly `ingest_wti`) records upstream revisions in
     the version diff. The DVC-tracked raw file was **not** modified; refresh WTI after seeding.
4. **Fabricated ICP values.**
   - *Before:* `run_ingestion.build_csv_dataset` linearly interpolated prices it failed to parse from
     PDFs, then stored them as actual observations.
   - *Now:* unparsed months are dropped and reported for manual entry. Values already in `dataset.csv`
     cannot be retroactively distinguished, which is a known data-provenance risk.
5. **Silent dummy data.**
   - *Before:* `data_loader.load_icp_dataset` returned synthetic data when the CSV was missing.
   - *Now:* missing or invalid data fails loudly (`503 DATASET_UNAVAILABLE` / `DATASET_INVALID`), and
     there are no dummy predictions anywhere.
6. **Simulated drift.**
   - *Before:* `simulate_drift_trigger.py` reported a random PSI (`rng.uniform(0.22, 0.45)`).
   - *Now:* PSI is computed from real feature distributions, with smoothing for small samples,
     disjoint windows and sample sizes reported.
7. **Throwaway CT training.**
   - *Before:* the GitHub CT workflow trained into an ephemeral SQLite MLflow on the runner, so its
     "promotions" never reached serving.
   - *Now:* CT calls the deployed backend's job API, and promotion is validated server-side.
8. **Promotion by stage with weak checks.**
   - *Before:* `audit_and_promote.py` compared only RMSE, and used deprecated MLflow stages.
   - *Now:* the `production` alias, a validated gate set (load, signature, smoke prediction, naive
     baseline, regression tolerance), recorded history and rollback.
9. **Unsafe defaults in the old stack.**
   - Hard-coded Postgres and Grafana `admin/admin` credentials in compose.
   - Streamlit with CORS and XSRF protection disabled.
   - Prediction errors echoed exception text to clients.
   - Model-serving startup `sys.exit(1)` on registry outage.

   All of these are removed: secrets come from the environment, errors use a safe envelope, and the
   readiness probe reports a degraded state instead of crashing.
10. **Uncontrolled overwrites.**
    - *Before:* ingestion rewrote `dataset.csv` in place.
    - *Now:* dataset changes create immutable versions, and changing or removing months needs
      explicit confirmation. Both are audited.

## Behavioural differences worth knowing

- The old API accepted raw engineered features (`POST /predict`). The new API derives features from
  the active datasets, so clients only choose the horizon and an optional WTI scenario.
- Model metrics are not comparable with the old README's numbers (RMSE 3.62 for LinearRegression):
  those were computed with gap-bridging features and a different split.

## October 2026: official data, forecasting semantics, business product

What changed on top of the monorepo refactor above (all local; nothing deployed):

1. **Official ICP from source documents.** `pipeline/icp_ml/sources/esdm_icp.py` + `esdm_backfill.py`
   download every ESDM decree (text layer or OCR, up to 6 pages) and monthly announcement, reconcile
   them (`verified` / `single_source` / `conflict`) and keep the documents (SHA-256) as evidence.
   The legacy CSV values stay in `data/raw/dataset.csv` (DVC) and are superseded in the metadata
   store with `rejected` evidence, never deleted. See `DATA_RECONCILIATION.md`.
2. **New datasets** `brent`, `dubai`, `usd_idr`, `usd_idr_eom` with connectors (EIA/mirror, World
   Bank, Bank Indonesia), completeness rules for partial months, provenance tables (migration 0002)
   and seeds in `data/seed/`. `ingest_wti` became `ingest_benchmarks`; `ingest_fx` and `ingest_icp`
   were added.
3. **Forecasting redesign.** Named methods behind one interface (persistence, benchmark + spread
   rules, blend, ridge, GBM), rolling-origin backtests per horizon in `observed` and `hold_last`
   input modes, fair comparison on common origins, evidence-based selection with a preference for
   simpler methods, empirical intervals with coverage checks, model cards. The old WTI-only
   gradient-boosting model (local v3) is kept in the registry but marked `legacy` and is not
   servable; v4 (`spread_brent_12m`) is the first version under the new methodology.
4. **Forecast semantics.** Every record stores the forecast origin (last official month), issue
   date, method, dataset versions, scenario and workflow; months are typed as
   `estimate_awaiting_publication` or `future_forecast`; old records are presented as
   `legacy_record` without rewriting (migration 0003).
5. **Accounts and product surface** (migration 0004): organisations, users, sessions, API keys,
   plans/entitlements, saved scenarios, alerts, reports, usage. Operator tokens keep working for
   Platform Operations.
6. **Frontend** rebuilt Indonesian-first around the business information architecture (Ringkasan
   Pasar, Eksplorasi Harga, Proyeksi ICP, Simulasi Skenario, Insight Pasar, Laporan & Pemberitahuan)
   with a separate Platform Operations area and a public landing page.

Behavioural differences: anonymous forecasts are limited to 3 months without scenario; the
production model must carry a method spec (pre-redesign versions return `503 MODEL_UNAVAILABLE`
until a new version is promoted); `GET /forecasts` is tenant-scoped.
