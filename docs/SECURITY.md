# Security

*Technical reference (English).*

## Principals and authorization (all server-side)

| Principal | How authenticated | What it can do |
|---|---|---|
| Guest | – | Plans, public market snapshot (no forecast values: only a `{available, issue_date, forecast_origin, horizon}` teaser), `whoami`, login, register. Everything else under `/api/v1` is `401` |
| Organisation member (`viewer` < `analyst` < `admin` < `owner`) | Session token `icp_sess_…` (Bearer), 30-day expiry, revocable | Customer workspace per **plan entitlements** (history depth, horizon, insights, scenarios, alerts, reports); `analyst`+ runs scenarios/alerts and scenario forecasts/reports; `admin`+ manages members and API keys |
| Organisation API key (`icp_live_…`) | Bearer; plan must include API access | Acts as `analyst`; never organisation administration or profile |
| Platform operator | Admin token from `ADMIN_API_TOKENS` (`name:token`) | Internal reads (system status, data quality, dataset versions/ingestions, experiments, promotions, drift), training, ingestion, promotion/rollback, jobs, audit, tenant plan/status; read access to every forecast and report. **Refused (`403`) on customer-organisation endpoints** (scenarios, alerts, `/org`, members, API keys, `/auth/me`) - tenants are administered through `/admin/organizations/*` |

Rules live in `backend/icp_api/security.py` (`authenticate`, `require_user`, `require_feature`,
`require_admin`, `check_horizon`) and `entitlements.py`; the frontend only mirrors them. The full
matrix and status codes are in [API.md](API.md#access-control-policy-v4) and are covered by
`backend/tests/test_access_control.py`.

* **Status codes.** No/invalid credential: `401 AUTH_REQUIRED|AUTH_INVALID` + `WWW-Authenticate:
  Bearer`. Authenticated but wrong kind/role: `403 FORBIDDEN` (no challenge header). Plan limits:
  `403 FEATURE_NOT_IN_PLAN|PLAN_LIMIT|SEAT_LIMIT`. Unchanged temporary password:
  `403 PASSWORD_CHANGE_REQUIRED` on everything except `whoami`, `GET /auth/me`, `/auth/password`,
  `/auth/logout`.
* **Tenant isolation.** Tenant data is always queried with the caller's `org_id`; an id that belongs
  to another organisation returns `404` (scenarios, alert rules/events, reports and downloads,
  forecasts and forecast exports). Members may read their own forecasts and published system
  forecasts; legacy anonymous runs are operator-only. Operator-generated platform reports are not
  listed or served to members.
* **Plan limits on every forecast path.** The plan horizon is checked on `POST /forecasts`,
  scenario create/update/run/compare and forecast/scenario-comparison reports; report parameters
  are validated with the same schemas as the direct endpoints and need the same role.
* **Caching.** Forecast exports are `Cache-Control: private, no-store`; the gateway does not
  edge-cache API responses.

## Credentials and secrets

* Passwords: scrypt (N=2¹⁵, r=8, p=1, 16-byte salt), constant-time verification, minimum 10
  characters; temporary passwords force a change at first login and revoke other sessions.
* Session tokens and API keys are random (`secrets.token_urlsafe(32)`), stored only as SHA-256;
  shown once. Logout revokes the session; disabling a member revokes all their sessions.
* Admin tokens, the gateway shared secret, D1/R2/EIA credentials come from the environment
  (`.env` locally, platform secrets in deployment); `.env.example` contains no real values and
  `npm run secret-scan` fails the build on token-like strings in the repository.
* The browser keeps the session/operator token in `sessionStorage` only; nothing is bundled.
* MLflow is configured not to record environment variable names in model artifacts.

## Transport, edge and request hygiene

* Cloudflare Worker gateway (`apps/gateway`): forwards `/api/v1/*` only, adds `X-Gateway-Secret`,
  rate limits per IP/token hash, security headers, 405 for non-GET/POST, bounded upstream timeout
  with retries for idempotent GETs.
* Backend: request ids on every response, body size limit, request timeout, CORS allow-list,
  per-process rate limits (defence in depth), structured JSON logs without secrets, Prometheus
  metrics behind a bearer token (`METRICS_TOKEN`, mandatory in staging/production because `/metrics`
  bypasses the gateway secret; optional locally).
* Uploads (`/datasets/{dataset}/imports`): CSV parsed with strict schema/range/duplicate
  validation, previewed before commit, destructive changes require `confirm_overwrite`; downloaded
  source documents are cached in `var/cache/sources` and never executed; XML from Bank Indonesia is
  parsed with `defusedxml`; models are serialized with skops (no pickle) and loaded with an explicit
  trusted-type allow-list.

## Audit and privacy

* `audit_events` records sensitive actions and denied attempts with actor, action, outcome, target,
  request id and non-secret details (no passwords, tokens, raw IPs).
* Personal data stored: member e-mail, display name, last login time, user-agent of sessions.
  Retention: sessions expire after 30 days; audit events are kept (operator export/purge is a
  documented operator task, not automated yet). See the in-app *Kebijakan Privasi*.
* No payment data is processed (no billing provider is integrated).

## Dependency and configuration checks

* `requirements/constraints.txt` pins the audited set (pip-audit clean on 2026-10-16 before the
  reportlab/defusedxml additions; re-run `pip-audit -r requirements/constraints.txt`).
* `load_settings()` validates deployment configuration (`APP_ENV=staging|production` requires
  D1/R2/Postgres, gateway secret ≥ 32 chars, `METRICS_TOKEN` ≥ 32 chars, no SQLite/local paths).

## Known gaps

* No e-mail delivery: password resets and invitations are handed over out of band by an admin.
* No MFA; no account lockout beyond rate limiting (login endpoints share the default limiter).
* Per-process rate limiting is not shared across backend instances (the Worker limit is).
