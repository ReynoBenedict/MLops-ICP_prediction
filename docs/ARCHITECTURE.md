# Architecture

## Components and runtimes

| Component | Code | Runtime (target) | Runtime (local) | Why there |
|---|---|---|---|---|
| Web app | `apps/web` (React, Vite, TS, Recharts) | Cloudflare Workers Static Assets | Vite dev server or `wrangler dev` | Static SPA; global edge delivery |
| API gateway | `apps/gateway` (Worker, TS) | Cloudflare Workers | `wrangler dev` (workerd) | Request limits, rate limiting, request IDs, shared-secret injection, edge caching of immutable exports. **No business logic.** |
| Backend | `backend/icp_api` (FastAPI) | Docker image `infra/docker/api.Dockerfile`; staging: operator machine behind a Cloudflare Quick Tunnel (`docs/DEPLOYMENT.md`) | `uvicorn` / Docker | Needs pandas, scikit-learn and MLflow, which do not run in the Workers runtime; long-running jobs need a persistent process |
| ML pipeline | `pipeline/icp_ml` | Same Python process / job worker subprocesses | Same | One implementation of features, training and forecasting, shared by training and serving |
| App metadata | migrations in `infra/cloudflare/d1` | **Cloudflare D1** (via the Worker's internal data plane; REST API also supported) | SQLite file `var/metadata.db` | Relational, low volume, SQLite dialect on both sides |
| Objects | `backend/icp_api/storage/objects.py` | **Cloudflare KV** via the Worker (zero-cost staging); R2/S3 supported | `var/objects/` | Immutable dataset files, forecast exports, MLflow artifacts |
| Experiment tracking & registry | MLflow | Operator machine only; serving loads an exported model bundle (`MODEL_SOURCE=bundle`). PostgreSQL + S3 artifacts supported for an online MLflow | SQLite `var/mlflow/mlflow.db` + `var/mlflow/artifacts` | MLflow supports only SQL dialects through SQLAlchemy (not D1) |

### Why Python stays off Cloudflare
The model stack (scikit-learn, pandas, numpy, MLflow) depends on native extensions and on a
SQLAlchemy-backed registry. The Workers runtime (V8 isolates, CPU-time limits, no native wheels)
is not a fit for training or for loading serialized sklearn pipelines. Workers instead carry what they do
well: static delivery, request policy at the edge and caching.

### Why the backend talks to D1 through the Worker (or over REST)
D1 is reachable from outside Workers either through the Cloudflare REST API (needs an API token on
the backend) or through a Worker that holds the binding. The zero-cost topology (docs/DEPLOYMENT.md)
uses the second form: the gateway Worker exposes `POST /api/internal/d1/query` and
`GET/HEAD/PUT /api/internal/objects/<key>` (KV) to callers that present the gateway shared secret,
so the backend holds no Cloudflare credential at all. The backend owns all business logic, so it
uses the same `MetadataRepository` with three transports:

- `SqliteExecutor` (local/test), `GatewayD1Executor` (through the Worker; default when deployed) and
  `D1HttpExecutor` (REST API) execute **identical SQL**; the two HTTP transports share the same
  D1 response envelope and retry rules.
- All writes are single statements. D1 has no interactive transactions, so atomicity comes from
  the statements themselves:
  - `INSERT ... SELECT MAX(version)+1` for numbering;
  - `UPDATE ... CASE` for activation;
  - a partial unique index for the one-active-job-per-type guard.
- Schema migrations are plain SQLite files applied by wrangler (`d1 migrations apply`) in the cloud
  and by `icp-api migrate` locally. Each migration records itself in `app_schema`, and readiness
  checks compare that version with the files.

**Material differences (no production parity is claimed):**
- **Latency.** D1 REST adds roughly one round trip to Cloudflare per query. Metadata traffic is low
  (a few queries per request), but expect tens to hundreds of milliseconds more than local SQLite.
- **Limits.** Cloudflare API rate limits apply, and Cloudflare describes the D1 REST API as primarily
  for management. This is acceptable for this application's volume (forecast records, jobs, audit
  events). If traffic grows, move the metadata store behind a Worker with a D1 binding, or to Postgres.
- **Retries.** The executor retries only when a request provably did not execute (connection errors,
  429/5xx). A timed-out write is never replayed.
- **Model source.** With `MODEL_SOURCE=bundle` the backend never imports MLflow: the promoted model
  is an exported bundle in the object store (`icp_ml.bundle`), written from the operator machine with
  `icp-api export-model`; training/promotion endpoints answer `409 OPERATION_UNAVAILABLE`.
- **Tests.** The D1 path is tested against simulators of the D1 REST protocol and of the Worker's
  internal data plane, both backed by real SQLite; the Worker routes are unit-tested with fake bindings.
  The migrations were also applied to wrangler's local D1 engine, which checked the partial unique
  index and the single-statement writes. The live D1 API has not been exercised.

## Request flow (production)

1. The browser loads `https://<gateway>/…`. Static files come from Workers assets, with security
   headers from `apps/web/public/_headers`.
2. The browser calls same-origin `/api/v1/...`. The Worker:
   - validates the method and path, and enforces the body limit (2 MB);
   - applies the edge rate limiter (forecast and general buckets);
   - assigns or propagates `X-Request-ID`;
   - drops client headers it does not forward (cookies, any forged `X-Gateway-Secret`);
   - adds `X-Gateway-Secret` and `CF-Connecting-IP`, then forwards with a 30 s timeout. Idempotent
     requests are retried once on connection failure;
   - refuses upstream redirects.
3. The backend middleware:
   - verifies the gateway secret, constant-time (`REQUIRE_GATEWAY_SECRET=true` in deployment);
   - enforces body size and request timeout;
   - binds the request ID to every log line, emits Prometheus metrics, and adds security headers.
4. Routers validate input with Pydantic. Responses are validated against the same models.
   Authorization is decided server-side (`require_admin`). Errors use one envelope:
   `{"error": {"code", "message", "request_id", "details"}}`.

## Model semantics (read this before trusting a forecast)

Full description: [FORECASTING_METHODOLOGY.md](FORECASTING_METHODOLOGY.md). In short:

- **Target.** Official ICP for month *t* (ESDM decree, published early in *t+1*).
- **Forecast origin / issue date.** The origin is the latest *official, verified* ICP month; the
  issue date is when the forecast was produced. Both are stored with every record; old records are
  never rewritten when new data arrives.
- **Two kinds of output month.** `estimate_awaiting_publication` (month ended, benchmarks observed,
  ICP not yet published) and `future_forecast` (month not ended; benchmarks from a user scenario or
  held at the last known value, always labelled). Backtest predictions are a third, separate concept.
- **Inputs.** Same-month Brent/Dubai/WTI averages, ICP lags (official up to the origin, recursive
  after it), spreads, USD/IDR. Lags are computed on a full monthly calendar; missing months are never
  bridged. `feature_row()` (serving) is tested to equal `build_feature_frame()` (training).
- **Methods.** Persistence, benchmark + spread rules, non-negative benchmark blend, ridge, gradient
  boosting - all behind one interface (`icp_ml.methods`), all backtested, registered and served the
  same way.
- **Evaluation.** Rolling-origin backtest (refit at every origin, horizons 1-6) in two input modes
  (`observed` / `hold_last`), MAE/RMSE/bias/sMAPE/MASE/direction per horizon, compared on the rows
  every method produced. Selection: MAE over h1-h3 (observed), with a 2 % preference for rule-based
  methods. **Result (2026-10, reconciled official series):** a Brent + 6-month-spread rule wins (h1 MAE ≈ 0.85
  USD/bbl vs naive 6.1); for future months without a scenario no method beats persistence - the product says so.
- **Intervals.** Empirical quantiles of backtest errors per mode and horizon, stored with the model;
  out-of-sample coverage is logged.
- **Promotion gates.** Method spec present, loads, signature, smoke prediction, beats naive (observed
  h1), hold-last h1 not worse than naive by > 10 %, not worse than production by > 5 %.

## Jobs

- A job row is inserted first, as `queued`. The database's partial unique index guarantees one
  queued or running job per type across instances.
- A runner thread starts `python -m icp_api.worker <job_id>` as a subprocess. While it runs, the
  runner heartbeats, honours cancellation (it terminates the process) and enforces the timeout.
- Exit code 75 marks a transient failure, which is retried with exponential backoff up to
  `max_attempts`.
- A reaper fails jobs whose heartbeat goes stale, for example after an instance restart.
- Jobs run on the instance that accepted them. Deploy a single always-on backend instance.
  Scaling to several backend instances would need an external queue.

## Security model

Details: [SECURITY.md](SECURITY.md).

- **Product areas.** Public website (`/`, `/metodologi`, `/masuk`, `/daftar`, `/syarat`,
  `/privasi`; no app shell), customer workspace (`/ringkasan`, `/harga`, `/proyeksi`, `/skenario`,
  `/insight`, `/laporan`, `/pemberitahuan`, `/akun`; guests are redirected to `/masuk?next=…`) and the
  operations console (`/operasi/*`, operator tokens only, with a read-only product preview). The UI
  guards mirror the server policy; they are not the security boundary.
- **Anonymous.** Only `/plans`, `/public/market-snapshot` (latest official ICP, benchmark and FX
  headline, freshness; the latest forecast appears only as "available since <date>", without values)
  and the sign-in/registration endpoints. Every other `/api/v1` endpoint answers `401 AUTH_REQUIRED`.
  The full matrix is in [API.md](API.md) and [SECURITY.md](SECURITY.md).
- **Organisation members.** Session tokens (`icp_sess_…`, hashed at rest, 30-day expiry) or
  organisation API keys (`icp_live_…`); roles viewer < analyst < admin < owner; plan entitlements
  (horizon, scenarios, reports, alerts, API access, seats) enforced in `security.require_feature`
  and the services, including plan horizon on every forecast-producing path and plan history
  length. Tenant data is always scoped by `org_id`; object ids from another organisation answer
  `404`. Members with an unchanged temporary password get `403 PASSWORD_CHANGE_REQUIRED`.
- **Platform operators.** Bearer tokens from `ADMIN_API_TOKENS` (constant-time comparison, audited
  failures) for training, ingestion, promotion/rollback, jobs, audit, system status and tenant
  administration. Operators cannot act inside customer organisations (scenarios, alerts, members,
  API keys answer `403`); tenant administration goes through `/admin/organizations/*`.
- **Gateway → backend.** A shared secret in `X-Gateway-Secret`; proxy IP headers are trusted only
  when it matches.
- **Model serialization.** skops with an explicit trusted-type allow-list (scikit-learn tree
  internals + the project's rule-based estimators); no pickle. Keep R2 write credentials limited to
  the backend.

## Frontend conventions

- **Design system.** One stylesheet (`apps/web/src/styles.css`) holds tokens for surface, ink, brand, series and status colours, plus spacing, radii, elevation and type scale. Shared primitives live in `apps/web/src/components/ui.tsx`: PageHeader, Panel, Kpi, Delta, Badge/StatusBadge, Button, Callout, ErrorState, EmptyState, Skeleton, Segmented, Tabs, Pager and Stat. Charts live in `components/charts.tsx`: TimeSeriesChart, MetricBars and CoverageGrid. Pages only compose these.
- **Theme.** Light theme first: warm off-white canvas, white panels, navy ink, a deep-teal brand accent for UI chrome, and a navy sidebar. Icons are Lucide (`lucide-react`, the only UI dependency added).
- **Charts.**
  - One y-axis (USD/bbl), recessive grid, 2 px lines, hover tooltips, and a legend for two or more series.
  - A Chart/Table toggle where a table adds value, so values are never available only through colour or hover.
  - Series colours were validated for colour-vision deficiency and contrast on the white surface: ICP `#1d6fb8`, WTI/oil `#e8692f`, comparison `#6250d6`. The brand teal is never used as a data series because it reads as grey for colour-blind users.
- **Honest gaps.** Missing months render as line breaks plus shaded bands. They are never interpolated. The holdout chart shades months that have no complete feature rows.
- **Derived context.** Overview month-over-month and year-over-year changes, 12-month highs and lows, and the latest common spread month are computed client-side from `/data/history` (`src/lib/market.ts`). A change is only reported when both months were actually observed.
- **Status.** Status badges always pair an icon and a text label with the colour. Up/down deltas carry an explicit sign.
- **Responsive.**
  - Above 1100px: two-column workspaces.
  - Below 900px: the sidebar becomes a drawer behind a top bar.
  - Below 560px: single column.
  - Wide tables scroll inside their panel and drop low-priority columns (`hide-md`, `hide-sm`). The page itself never scrolls horizontally.
