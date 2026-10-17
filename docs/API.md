# API overview

The contract is `packages/contracts/openapi.json`, generated from the backend (`npm run contracts`).
Interactive docs are at `/docs` on non-production backends. TypeScript types are generated into
`packages/contracts/src/schema.d.ts`, and `npm run contracts:check` fails CI if either drifts.

## Conventions

- **Base path.** `/api/v1`, always same-origin from the browser through the gateway.
- **Errors.** Every non-2xx response has this envelope:
  ```json
  {"error": {"code": "MODEL_UNAVAILABLE", "message": "…", "request_id": "gw_…", "details": {}}}
  ```
  Validation failures are `422 VALIDATION_ERROR` with `details.issues`. Entitlement refusals are
  `403 FEATURE_NOT_IN_PLAN | PLAN_LIMIT | SEAT_LIMIT` with `details.{feature|limit, plan}`.
  Internal errors never include exception text.
- **Request IDs.** `X-Request-ID` is accepted (8–128 characters of `[A-Za-z0-9._:-]`) or generated,
  and is always returned.
- **Pagination.** List endpoints take `limit` (1–100, default 20) and an opaque `cursor`, and return
  `{items, next_cursor}` (keyset, `created_at DESC, id DESC`).
- **Auth.** `Authorization: Bearer <token>` where the token is a session token (`icp_sess_…`, from
  login/register), an organisation API key (`icp_live_…`) or a platform-operator token
  (`ADMIN_API_TOKENS`). `GET /auth/whoami` describes the caller (kind, role, plan, `horizon_max`).
  Column legend below: **–** anonymous allowed · **user** signed-in member (minimum role in
  brackets) · **feature** member whose plan includes the feature · **op** platform operator.
- **Limits.** Bodies ≤ 2 MB (`413`), CSV imports ≤ 1 MB, rate limits per client (`429` with
  `Retry-After`), request timeout 30 s (`504`).

## Endpoints

### Health, system, auth
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /health/live`, `GET /health/ready` | – | Liveness / readiness (outside `/api`; not exposed through the gateway) |
| `GET /api/v1/system/status` | – | Dependency checks and configuration summary |
| `GET /api/v1/auth/whoami` | – | Who am I (works for every principal) |
| `POST /api/v1/auth/register` | – | `{organization_name, email, name, password, accept_terms}` → trial organisation + owner session (`201`); disabled when `PUBLIC_REGISTRATION_ENABLED=false` |
| `POST /api/v1/auth/login` / `POST /auth/logout` | – / user | Session token shown once / revoke |
| `GET /api/v1/auth/me`, `POST /auth/me` (`{name}`), `POST /auth/password` | user | Profile, rename, change password (revokes other sessions) |
| `GET /api/v1/plans` | – | Plan catalogue with entitlements (no prices; `price_note_id`) |

### Organisation (tenant) and operator tenant administration
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/org`, `POST /org` (`{name}`) | user / user(admin) | Organisation, entitlements, counts, usage this month |
| `GET /api/v1/org/members` | user (session only) | Members |
| `POST /api/v1/org/members` | user(admin) | Invite `{email, name, role}` → one-time temporary password (seat limit enforced) |
| `POST /api/v1/org/members/{id}`, `…/reset-password` | user(admin) | Role/status change, temporary password |
| `GET/POST /api/v1/org/api-keys`, `POST …/{id}/revoke` | user(admin), plan with API access | Keys shown once; stored hashed |
| `GET /api/v1/admin/organizations` | op | Tenant list |
| `POST /api/v1/admin/organizations/{id}/plan` / `/status` | op | `{plan, reason}` / `{status, reason}` (billing integration point) |

### Data
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/public/market-snapshot` | – | Landing-page snapshot: latest official ICP, previous, MoM, Brent, USD/IDR, rupiah equivalent, freshness, published forecast preview |
| `GET /api/v1/data/summary` | – | Per-dataset coverage/latest/freshness (`datasets`), ICP–benchmark `relationships`, `icp_idr` |
| `GET /api/v1/data/history?start=&end=` | – | Monthly table: ICP, WTI, Brent, Dubai, USD/IDR (avg, EOM), `icp_idr`; gaps are `null` |
| `GET /api/v1/data/quality?dataset=` | – | Quality report of the active version |
| `GET /api/v1/data/evidence?dataset=&period=` | – | Source evidence per month (document checksum, method, verification status, reference) |
| `GET /api/v1/datasets/active` | – | Active versions + provenance + freshness for every dataset |
| `GET /api/v1/datasets/versions[?dataset=]`, `GET …/{id}[/download]` | – | Version history, record, CSV |
| `GET /api/v1/datasets/ingestions[?dataset=]` | – | Ingestion runs (added/changed months, partial months excluded, failures) |
| `POST /api/v1/datasets/{dataset}/imports?mode=merge\|replace&commit=&confirm_overwrite=` | op | Validated import (body `text/csv`); dry run unless `commit=true` |
| `POST /api/v1/datasets/versions/{id}/activate` | op | Re-activate a validated version |

### Forecasts, scenarios, insights
| Method & path | Auth | Purpose |
|---|---|---|
| `POST /api/v1/forecasts` | – (≤ 3 months, no scenario) / user (plan `forecast_horizon_max`) | `{horizon, scenario?: {brent, dubai, wti, usd_idr: {"YYYY-MM": value}}}` → forecast record with per-month `kind`, intervals, benchmark sources, rupiah equivalent (`wti_overrides` is a deprecated alias) |
| `GET /api/v1/forecasts/latest` | – | Latest published **system** forecast |
| `GET /api/v1/forecasts[?status=]` | – / user / op | Anonymous: published system forecasts; member: own organisation's runs; operator: all |
| `GET /api/v1/forecasts/{id}`, `GET …/{id}/export.csv` | – | Record / immutable CSV (origin, issue date, kinds, sources, intervals) |
| `GET/POST /api/v1/scenarios`, `GET/POST …/{id}`, `POST …/{id}/archive` | feature `scenarios` (analyst+ to write) | Saved scenarios per organisation |
| `POST /api/v1/scenarios/{id}/run` | user(analyst) | Runs the scenario → forecast record (`workflow=scenario`) |
| `POST /api/v1/scenarios/compare` | user(analyst) | `{scenario_ids (≤4), horizon?}` → baseline vs scenarios table |
| `GET /api/v1/insights` | – (3 items) / user | Rule-based Indonesian insights with metrics and dataset versions |

### Alerts and reports
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/alerts/kinds` | – | Rule kinds and default parameters |
| `GET/POST /api/v1/alerts/rules`, `POST …/{id}`, `POST …/{id}/delete` | feature `alerts` (analyst+ to write) | Rules (limit per plan) |
| `GET /api/v1/alerts/events[?unread_only=]`, `POST …/events/read[?event_id=]`, `POST /alerts/evaluate` | feature `alerts` | In-app notifications (de-duplicated), mark read, evaluate now |
| `GET/POST /api/v1/reports` | user | `{kind: market_monthly\|forecast\|scenario_comparison\|data_provenance, params, formats}` (PDF/XLSX per plan; CSV always) |
| `GET /api/v1/reports/{id}`, `GET …/{id}/download?format=pdf\|xlsx\|csv` | user | Report record / file |

### Models and operations
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/models` | – | Registered versions: method, family, rule-based flag, horizon metrics (observed / hold_last), datasets, `legacy` flag |
| `GET /api/v1/models/versions/{v}` | – | + one-step backtest pairs, intervals, coverage, backtest table, model card (Markdown) |
| `GET /api/v1/models/compare?versions=a,b` | – | Side-by-side (2–5 versions) |
| `GET /api/v1/models/experiments/runs` | – | MLflow candidate runs (paginated) |
| `GET /api/v1/models/promotions` | – | Promotion/rollback history |
| `GET /api/v1/models/drift?recent_months=` | – | PSI per feature used by the production method |
| `POST /api/v1/models/versions/{v}/validate` / `/promote` (`{reason, force?}`) / `POST /models/rollback` (`{reason}`) | op | Gates (method spec, loads, signature, smoke, beats naive, hold-last vs naive, vs production); `force` overrides only metric gates |
| `GET /api/v1/jobs`, `GET /jobs/{id}`, `POST /jobs`, `POST /jobs/{id}/cancel`, `/retry` | op | `{job_type: train \| ingest_benchmarks \| ingest_fx \| ingest_icp \| refresh_forecast \| evaluate_alerts, params, idempotency_key?}` → `202` |
| `GET /api/v1/audit[?action=]` | op | Audit trail |
| `GET /api/gateway/health` | – | Gateway Worker health (Worker only) |

Ingestion/training jobs end with a post-hook that republishes the system forecast and evaluates
alert rules; results are included in the job result (`system_forecast`, `alerts`).
