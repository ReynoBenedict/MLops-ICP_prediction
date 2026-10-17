# Security

*Technical reference (English).*

## Principals and authorization (all server-side)

| Principal | How authenticated | What it can do |
|---|---|---|
| Anonymous | – | Public market snapshot, 3 insights, published system forecasts, forecasts ≤ 3 months without scenario (rate limited) |
| Organisation member (`viewer` < `analyst` < `admin` < `owner`) | Session token `icp_sess_…` (Bearer), 30-day expiry, revocable | Product features per **plan entitlements**; `analyst`+ runs scenarios/alerts/reports; `admin`+ manages members and API keys |
| Organisation API key (`icp_live_…`) | Bearer; plan must include API access | Acts as `analyst`; never organisation administration |
| Platform operator | Admin token from `ADMIN_API_TOKENS` (`name:token`) | Training, ingestion, promotion/rollback, jobs, audit, tenant plan/status |

Rules live in `backend/icp_api/security.py` (`authenticate`, `require_user`, `require_feature`,
`require_admin`) and `entitlements.py`; the frontend only mirrors them. Tenant data is always
queried with the caller's `org_id`; cross-tenant access returns 404 (tested in
`backend/tests/test_product.py`).

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
  metrics behind an optional token.
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
  D1/R2/Postgres, gateway secret ≥ 32 chars, no SQLite/local paths).

## Known gaps

* No e-mail delivery: password resets and invitations are handed over out of band by an admin.
* No MFA; no account lockout beyond rate limiting (login endpoints share the default limiter).
* Per-process rate limiting is not shared across backend instances (the Worker limit is).
