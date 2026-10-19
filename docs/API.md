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
  Column legend below: **–** guest allowed · **user** signed-in member or operator (minimum member
  role in brackets) · **member** organisation member only (operators get `403`) · **feature**
  member whose plan includes the feature · **op** platform operator only.
- **Limits.** Bodies ≤ 2 MB (`413`), CSV imports ≤ 1 MB, rate limits per client (`429` with
  `Retry-After`), request timeout 30 s (`504`).

## Access control (policy v4)

Everything under `/api/v1` requires sign-in except `GET /plans`, `GET /public/market-snapshot`,
`GET /auth/whoami`, `POST /auth/login` and `POST /auth/register`. Status codes:

| Situation | Status and code |
|---|---|
| No or invalid credential | `401 AUTH_REQUIRED` (`AUTH_INVALID` for a bad/revoked token) + `WWW-Authenticate: Bearer` |
| Signed in, but wrong kind or role (member on an operator endpoint, operator on a member endpoint, viewer on an analyst action, API key on organisation administration) | `403 FORBIDDEN`, no `WWW-Authenticate` |
| Plan does not include the feature / exceeds a limit | `403 FEATURE_NOT_IN_PLAN` / `PLAN_LIMIT` / `SEAT_LIMIT` |
| Object id belongs to another organisation (or is otherwise not visible) | `404 NOT_FOUND` (existence is never revealed) |
| Member still holds an unchanged temporary password | `403 PASSWORD_CHANGE_REQUIRED` on everything except `GET /auth/whoami`, `GET /auth/me`, `POST /auth/password`, `POST /auth/logout` (and the guest endpoints) |

Matrix summary (✓ allowed, `own` = scoped to the caller's organisation):

| Endpoint group | Guest | Viewer | Analyst+ | API key | Operator |
|---|---|---|---|---|---|
| `/plans`, `/public/market-snapshot`, `/auth/whoami`, login, register | ✓ | ✓ | ✓ | ✓ | ✓ |
| `/data/summary`, `/data/history` (plan `history_years`), `/data/evidence`, `/datasets/active`, `/models`, `/models/versions/{v}`, `/alerts/kinds` | 401 | ✓ | ✓ | ✓ | ✓ |
| `/datasets/versions/{id}/download` | 401 | active versions | active versions | active versions | all |
| `/system/status`, `/data/quality`, `/datasets/versions`, `/datasets/versions/{id}`, `/datasets/ingestions`, `/models/compare`, `/models/experiments/runs`, `/models/promotions`, `/models/drift`, jobs, audit, `/admin/*`, model/dataset writes | 401 | 403 | 403 | 403 | ✓ |
| `POST /forecasts` | 401 | ✓ (no scenario) | ✓ + scenario (plan feature) | ✓ + scenario | ✓ |
| `GET /forecasts`, `/forecasts/latest`, `/forecasts/{id}`, `…/export.csv` | 401 | own + system | own + system | own + system | all |
| `/insights` | 401 | feature `insights` | same | same | ✓ |
| `/scenarios*`, `/alerts/rules*`, `/alerts/events*`, `/alerts/evaluate` | 401 | own (read) | own (write) | own | 403 |
| `GET /reports*` | 401 | own | own | own | all (incl. platform reports) |
| `POST /reports` | 401 | `market_monthly`, `data_provenance`, `forecast` (by id or horizon only) | + `forecast` with scenario, `scenario_comparison` | analyst | ✓ (platform report) |
| `/auth/me`, `/auth/password`, `/auth/logout` | 401 | own | own | 403 | 403 |
| `/org`, `/org/members` (read) | 401 | own | own | 403 | 403 |
| `/org` rename, members admin, `/org/api-keys*` | 401 | 403 | admin+ | 403 | 403 (use `/admin/organizations/*`) |

Forecast visibility: a member may read a forecast when it belongs to their organisation or is a
published system forecast (`workflow = system`); legacy anonymous runs (no organisation, not
`system`) are visible to operators only. Plan horizon (`forecast_horizon_max`) is enforced with
`403 PLAN_LIMIT` on every forecast-producing path: `POST /forecasts`, scenario create/update/run/
compare and reports of kind `forecast` / `scenario_comparison`.

## Endpoints

### Health, system, auth
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /health/live`, `GET /health/ready` | – | Liveness / readiness (outside `/api`; not exposed through the gateway) |
| `GET /api/v1/system/status` | op | Dependency checks and configuration summary |
| `GET /api/v1/auth/whoami` | – | Who am I (works for every principal, including `must_change_password`) |
| `POST /api/v1/auth/register` | – | `{organization_name, email, name, password, accept_terms}` → trial organisation + owner session (`201`); disabled when `PUBLIC_REGISTRATION_ENABLED=false` |
| `POST /api/v1/auth/login` / `POST /auth/logout` | – / member | Session token shown once / revoke |
| `GET /api/v1/auth/me`, `POST /auth/me` (`{name}`), `POST /auth/password` | member (session) | Profile, rename, change password (revokes other sessions) |
| `GET /api/v1/plans` | – | Plan catalogue with entitlements (no prices; `price_note_id`) |
| `GET /metrics` (outside `/api`) | Prometheus bearer token | Required (`METRICS_TOKEN` ≥ 32 chars) in staging/production; open locally when unset |

### Organisation (tenant) and operator tenant administration
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/org`, `POST /org` (`{name}`) | member (session) / member(admin) | Organisation, entitlements, counts, usage this month |
| `GET /api/v1/org/members` | member (session only) | Members |
| `POST /api/v1/org/members` | member(admin) | Invite `{email, name, role}` → one-time temporary password (seat limit enforced) |
| `POST /api/v1/org/members/{id}`, `…/reset-password` | member(admin) | Role/status change, temporary password |
| `GET/POST /api/v1/org/api-keys`, `POST …/{id}/revoke` | member(admin), plan with API access | Keys shown once; stored hashed |
| `GET /api/v1/admin/organizations` | op | Tenant list |
| `POST /api/v1/admin/organizations/{id}/plan` / `/status` | op | `{plan, reason}` / `{status, reason}` (billing integration point) |

### Data
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/public/market-snapshot` | – | Landing-page snapshot: latest official ICP, previous, MoM, Brent, USD/IDR, rupiah equivalent, freshness, source, `updated_at`; `latest_forecast` is a teaser `{available, issue_date, forecast_origin, horizon}` without predicted values |
| `GET /api/v1/data/summary` | user | Per-dataset coverage/latest/freshness (`datasets`), ICP–benchmark `relationships`, `icp_idr` |
| `GET /api/v1/data/history?start=&end=` | user | Monthly table: ICP, WTI, Brent, Dubai, USD/IDR (avg, EOM), `icp_idr`; gaps are `null`. Clipped to the plan's `history_years` (ending at the latest official ICP month); the response carries `history_years` (null = unlimited) and `earliest_period` |
| `GET /api/v1/data/quality?dataset=` | op | Quality report of the active version |
| `GET /api/v1/data/evidence?dataset=&period=` | user | Source evidence per month (document checksum, method, verification status, reference) |
| `GET /api/v1/datasets/active` | user | Active versions + provenance + freshness for every dataset |
| `GET /api/v1/datasets/versions[?dataset=]`, `GET …/{id}` | op | Version history, record |
| `GET /api/v1/datasets/versions/{id}/download` | user (active versions) / op (any) | CSV; inactive versions are `404` for members |
| `GET /api/v1/datasets/ingestions[?dataset=]` | op | Ingestion runs (added/changed months, partial months excluded, failures) |
| `POST /api/v1/datasets/{dataset}/imports?mode=merge\|replace&commit=&confirm_overwrite=` | op | Validated import (body `text/csv`); dry run unless `commit=true` |
| `POST /api/v1/datasets/versions/{id}/activate` | op | Re-activate a validated version |

### Forecasts, scenarios, insights
| Method & path | Auth | Purpose |
|---|---|---|
| `POST /api/v1/forecasts` | user (plan `forecast_horizon_max`); scenario: analyst+ and feature `scenarios` | `{horizon, scenario?: {brent, dubai, wti, usd_idr: {"YYYY-MM": value}}}` → forecast record with per-month `kind`, intervals, benchmark sources, rupiah equivalent (`wti_overrides` is a deprecated alias) |
| `GET /api/v1/forecasts/latest` | user | Latest published **system** forecast |
| `GET /api/v1/forecasts[?status=]` | user / op | Member: own organisation's runs plus published system forecasts; operator: all |
| `GET /api/v1/forecasts/{id}`, `GET …/{id}/export.csv` | user | Record / CSV (origin, issue date, kinds, sources, intervals); own organisation or system forecast, else `404`. Export is `Cache-Control: private, no-store` |
| `GET/POST /api/v1/scenarios`, `GET/POST …/{id}`, `POST …/{id}/archive` | feature `scenarios` (analyst+ to write), members only | Saved scenarios per organisation (plan horizon on create/update) |
| `POST /api/v1/scenarios/{id}/run` | member(analyst) | Runs the scenario → forecast record (`workflow=scenario`); plan horizon enforced |
| `POST /api/v1/scenarios/compare` | member(analyst) | `{scenario_ids (≤4), horizon?}` → baseline vs scenarios table; plan horizon enforced |
| `GET /api/v1/insights` | feature `insights` | Rule-based Indonesian insights with metrics and dataset versions |

### Alerts and reports
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/alerts/kinds` | user | Rule kinds and default parameters |
| `GET/POST /api/v1/alerts/rules`, `POST …/{id}`, `POST …/{id}/delete` | feature `alerts` (analyst+ to write), members only | Rules (limit per plan) |
| `GET /api/v1/alerts/events[?unread_only=]`, `POST …/events/read[?event_id=]`, `POST /alerts/evaluate` | feature `alerts`, members only | In-app notifications (de-duplicated), mark read, evaluate now |
| `GET/POST /api/v1/reports` | user | `{kind: market_monthly\|forecast\|scenario_comparison\|data_provenance, params, formats}` (PDF/XLSX per plan; CSV always). `forecast` params are `{forecast_id}` or `{horizon, scenario}` validated like `POST /forecasts`; `scenario_comparison` params `{scenario_ids, horizon?}` like `/scenarios/compare` and needs analyst+. Members list only their organisation's reports |
| `GET /api/v1/reports/{id}`, `GET …/{id}/download?format=pdf\|xlsx\|csv` | user | Report record / file; other organisations' and platform (operator) reports are `404` for members |

### Models and operations
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/models` | user | Registered versions: method, family, rule-based flag, horizon metrics (observed / hold_last), datasets, `legacy` flag |
| `GET /api/v1/models/versions/{v}` | user | + one-step backtest pairs, intervals, coverage, backtest table, model card (Markdown) |
| `GET /api/v1/models/compare?versions=a,b` | op | Side-by-side (2–5 versions) |
| `GET /api/v1/models/experiments/runs` | op | MLflow candidate runs (paginated) |
| `GET /api/v1/models/promotions` | op | Promotion/rollback history |
| `GET /api/v1/models/drift?recent_months=` | op | PSI per feature used by the production method |
| `POST /api/v1/models/versions/{v}/validate` / `/promote` (`{reason, force?}`) / `POST /models/rollback` (`{reason}`) | op | Gates (method spec, loads, signature, smoke, beats naive, hold-last vs naive, vs production); `force` overrides only metric gates |
| `GET /api/v1/jobs`, `GET /jobs/{id}`, `POST /jobs`, `POST /jobs/{id}/cancel`, `/retry` | op | `{job_type: train \| ingest_benchmarks \| ingest_fx \| ingest_icp \| refresh_forecast \| evaluate_alerts, params, idempotency_key?}` → `202` |
| `GET /api/v1/audit[?action=]` | op | Audit trail |
| `GET /api/gateway/health` | – | Gateway Worker health (Worker only) |

Ingestion/training jobs end with a post-hook that republishes the system forecast and evaluates
alert rules; results are included in the job result (`system_forecast`, `alerts`).
