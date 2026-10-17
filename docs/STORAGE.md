# Storage design

| Store | Holds | Never holds |
|---|---|---|
| **D1** (`icp-metadata-staging`) / local SQLite | dataset version records, accounts, jobs, forecast runs, promotion history, audit events, schema version | secrets, binary files, model artifacts |
| **KV** (`icp-objects-staging`, zero-cost topology) or **R2** (one bucket) / `var/objects` | dataset CSV versions, forecast/report exports, exported model bundles; (R2 only) MLflow artifacts | secrets, mutable state |
| **MLflow store** (operator machine `var/mlflow/mlflow.db`, or PostgreSQL in the paid topology) | experiments, runs, metrics, params, registered model versions and aliases | application records |

Secrets live only in environment variables: `.env` locally, the container env file (`var/secrets/`, gitignored) and
`wrangler secret` in deployment.

### Transports

| Backend | Metadata | Objects | Credential on the backend |
|---|---|---|---|
| local / test | `SqliteExecutor` | `LocalObjectStore` | none |
| zero-cost (default) | `GatewayD1Executor` → Worker `/api/internal/d1/query` → D1 binding | `GatewayObjectStore` → Worker `/api/internal/objects/<key>` → KV binding | `GATEWAY_SHARED_SECRET` only |
| paid-tier | `D1HttpExecutor` → D1 REST API | `S3ObjectStore` → R2 S3 API | Cloudflare API token, R2 keys |

The Worker's internal routes accept only requests carrying the shared secret (constant-time check),
bypass the edge rate limits and never touch `BACKEND_URL`; the browser can never reach them.

## Schema (D1 / SQLite)

Source of truth: `infra/cloudflare/d1/migrations/*.sql`. Tables:

- **`dataset_versions`** (0001, rebuilt in 0002)
  - Columns: `dataset` (`icp`, `wti`, `brent`, `dubai`, `usd_idr`, `usd_idr_eom`), `version`, `object_key`,
    `sha256`, row count and period range, `source`, `source_detail`, `quality_json`, `diff_json`,
    `provenance_json`, `parent_version_id`, `is_active`, creator, timestamps.
  - Constraints: `UNIQUE(dataset, version)` and `UNIQUE(dataset, sha256)`. Identical content is never
    stored twice.
- **`source_documents`, `source_evidence`, `ingestion_runs`** (0002): every retrieved document
  (URL, SHA-256, size, content type, retrieval time), the evidence behind each monthly value
  (extraction method, verification status, reference, notes) and each ingestion attempt (added /
  changed months, partial months excluded, warnings, failures).
- **`jobs`**
  - Status lifecycle `queued → running → succeeded | failed | cancelled`.
  - Partial unique index on `job_type` where status is `queued` or `running`.
  - Also stores `idempotency_key`, attempts, timeout, heartbeat and the cancel flag.
- **`forecast_runs`** (0001, extended in 0003/0004): every forecast with parameters and result
  (JSON), model name/version/run, dataset version IDs (`dataset_versions_json`), `forecast_origin`,
  `issue_date`, `method`, `scenario_json`, `workflow` (`api`, `scenario`, `report`, `system`),
  `org_id`, `scenario_id`. Failures carry an error code. Records are never rewritten.
- **`model_promotions`**: `promote` and `rollback` records with from/to version, reason, forced flag,
  validation report and actor. This history drives rollback.
- **`audit_events`**: actor, action, target, outcome (`success`|`failure`|`denied`), request ID,
  details. No tokens, no IP addresses.
- **`organizations`, `users`, `sessions`, `api_keys`** (0004): tenants with plan/status, members with
  scrypt password hashes and roles, sessions and API keys stored as SHA-256 hashes with expiry /
  revocation timestamps.
- **`saved_scenarios`, `alert_rules`, `alert_events`, `report_runs`, `usage_events`** (0004):
  tenant-scoped product data; `alert_events` is unique per `(rule_id, dedupe_key)`; `report_runs`
  keeps one object key per generated format.
- **`app_schema`**: one row per applied migration.

Rules for new migrations:
- Name them `NNNN_description.sql`, numbered contiguously, using SQLite syntax that D1 supports.
- No `BEGIN`/`COMMIT`.
- End each file with an `INSERT INTO app_schema`.
- Never edit an applied migration; add a new one.

## Object naming

| Prefix | Example | Mutability | Retention |
|---|---|---|---|
| `datasets/{dataset}/` | `datasets/icp/<sha256>.csv` | write-once (content-addressed) | keep forever (small; the provenance of every model) |
| `exports/reports/{YYYY}/{MM}/` | `exports/reports/2026/10/rpt_….pdf\|.xlsx\|.csv` | write-once | R2 lifecycle rule: expire after **365 days** (the report record stays in D1) |
| `exports/forecasts/{YYYY}/{MM}/` | `exports/forecasts/2026/10/fc_….csv` | write-once | R2 lifecycle rule: expire after **180 days** (the forecast record stays in D1; the export link then returns 404) |
| `models/` | `models/production.json` (pointer), `models/bundles/index.json`, `models/bundles/<version>/{model.skops, meta.json, intervals.json, interval_coverage.json, model_card.md, backtest_metrics.csv, backtest_predictions.csv}` | bundle files write-once; the two pointer files are overwritten by `icp-api export-model` | keep (the operator's MLflow store is the primary copy) |
| `mlflow/` | `mlflow/<experiment>/<run>/artifacts/...`, `mlflow/<experiment>/models/m-…/` | written by MLflow (paid topology only) | keep artifacts of registered versions; unregistered candidate runs may be pruned after 90 days via `mlflow gc` (manual) |

- **Write-once enforcement.** Locally: an atomic hard-link create. On R2: `PutObject` with
  `If-None-Match: *`. Through the Worker/KV: `PUT` with `If-None-Match: *`, checked by the Worker
  before writing; KV is eventually consistent, so this is best-effort, which is safe because every
  write-once key is content-addressed or unique. A duplicate gets `412` and is treated as
  "already exists".
- **Quotas (KV free plan).** 1k writes/day. Dataset versions, reports and forecast exports each cost
  one write; a forecast whose export write fails is still recorded (`export_available=false`).
- **Integrity.** Dataset reads verify SHA-256 against the metadata row and fail closed with
  `DATASET_INTEGRITY_ERROR`.
- **Deletion.** The application never deletes objects. Retention runs only through R2 lifecycle
  rules or an explicit operator action.
- **Downloads.**
  - Exports and dataset versions are streamed by the backend through the gateway. No R2 credentials
    or presigned URLs reach the browser.
  - The Worker caches anonymous export downloads at the edge, because they are immutable.

## Credentials

| Credential | Used by | Scope |
|---|---|---|
| `GATEWAY_SHARED_SECRET` | Worker + backend (+ operator CLI for seed/export) | random ≥ 32 chars, identical on all sides; in the zero-cost topology it is the **only** shared credential |
| `D1_API_TOKEN` (paid topology) | backend | Cloudflare API token with **D1 Edit** on the account (scope to the account; rotate on exposure) |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` (paid topology; and the same as `AWS_*` for MLflow) | backend | R2 API token, **Object Read & Write**, limited to the one bucket |
| `MLFLOW_TRACKING_URI` (paid topology, Postgres URL with password) | backend | Database user limited to the MLflow database |
| `ADMIN_API_TOKENS` | backend | one named token per operator or automation |
