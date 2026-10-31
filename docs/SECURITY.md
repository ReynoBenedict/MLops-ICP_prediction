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
* **Caching.** Every API response defaults to `Cache-Control: no-store`; forecast exports, report
  files and dataset downloads are `private, no-store`. The gateway caches nothing that depends on a
  credential: its only stored answers are last-known-good copies of the two anonymous public endpoints
  (see "Last-known-good copies" below), served only while the backend is unreachable and marked stale.
* **Plan history depth** (`history_years`) applies to every per-month data path for members:
  `/data/history`, `/data/verification`, dataset version downloads (clipped CSV, file name
  `…_from-YYYY-MM.csv`) and the provenance report. Customers see system forecasts clipped to their
  plan horizon (JSON and CSV export); stored records are unchanged.
* **Model card.** Members get the model card without build internals (code commit, config hash,
  training job id); operators see everything.

## Credentials and secrets

* Passwords: scrypt (N=2¹⁵, r=8, p=1, 16-byte salt), constant-time verification, minimum 10
  characters. **Hash versioning:** new hashes are stored as `scrypt$ln=15,r=8,p=1$<salt>$<digest>`; the
  legacy `scrypt$<salt>$<digest>` still verifies, and a successful sign-in rehashes any legacy or
  differently-parameterised hash in the current format (one extra scrypt, once). Absurd stored parameters
  and malformed or anonymised hashes are refused.
* **Temporary passwords** (invitation, admin reset) force a change at first login and expire after
  `TEMP_PASSWORD_TTL_HOURS` (default 72 h; `users.temp_password_expires_at`). After expiry the right
  temporary password gets `403 TEMP_PASSWORD_EXPIRED` (a wrong one still `401`, so expiry is not revealed)
  and sessions opened with it stop working.
* **Sessions:** absolute 30-day expiry plus an **idle timeout** (`SESSION_IDLE_TIMEOUT_HOURS`, default
  168 h, measured from `last_seen_at`, ≈ 5 min precision); an idle session is revoked server-side. Members
  list their active sessions (`GET /auth/sessions`: device label, created/last seen/expiry, `current`;
  no token material or raw user agent), revoke one (`POST /auth/sessions/{id}/revoke`, `404` for other
  users' sessions) or sign out everywhere else (`POST /auth/sessions/revoke-others`). A password change
  revokes **all** sessions of that user, including the current one (the UI signs out); a password reset by
  an admin and disabling a member revoke them too.
* Session tokens and API keys are random (`secrets.token_urlsafe(32)`), stored only as SHA-256;
  shown once. Logout revokes the session; disabling a member revokes all their sessions.
* **E-mail tokens (designed, disabled).** Password reset, e-mail verification and invitation e-mail are
  implemented behind `EMAIL_ENABLED=false`, which is the only accepted value until a provider exists
  (`true` is refused at start-up; the sender is a no-op that makes no network calls). Every such endpoint
  answers `503 EMAIL_DISABLED` before reading or writing anything. Design: `icp_mail_` + 256-bit random
  token, only its SHA-256 stored (`email_tokens`, migration 0007), TTL 1 h (reset), 48 h (verify), 7 days
  (invite), single use by one conditional update, superseded by a newer token of the same purpose, voided
  on anonymisation, never copied by `clone-state`; the reset request answers the same whether or not the
  address exists; all endpoints are in the per-IP `auth` bucket. When a provider is added, send
  asynchronously so the reset-request timing does not differ for known addresses.
* **Billing webhook (sandbox only, disabled by default).** `POST /api/v1/billing/webhooks/midtrans` has no
  bearer authentication; it is accepted only when billing is enabled and the notification passes, in
  order: `signature_key = SHA512(order_id + status_code + gross_amount + ServerKey)` compared in constant
  time, a known order, the order's environment equal to the configured one, `gross_amount` and currency
  equal to the stored order, then an idempotent event log (unique per provider event) and a
  compare-and-set state change. Replays answer `200 duplicate`, illegal transitions are recorded as
  ignored, and the stored payload excludes `signature_key`. Details: [BILLING.md](BILLING.md).
* Admin tokens, the gateway shared secret, D1/R2/EIA credentials come from the environment
  (`.env` locally, platform secrets in deployment); `.env.example` contains no real values and
  `npm run secret-scan` fails the build on token-like strings in the repository.
* The browser keeps the member session token in `localStorage` (`icp.sessionToken`, until sign-out,
  idle timeout or the 30-day expiry; anyone using the same browser profile can use it, which the privacy
  policy states) and the operator token in `sessionStorage` (`icp.operatorToken`, cleared with the tab).
  Nothing is bundled. An XSS flaw would expose the session token; the CSP and React escaping are the
  mitigations, and sign-out revokes the token server-side. A migration to `Secure`/`HttpOnly` cookies is
  designed but not implemented (see [Cookie-session migration design](#cookie-session-migration-design-not-implemented)).
* MLflow is configured not to record environment variable names in model artifacts.

## Transport, edge and request hygiene

* Cloudflare Worker gateway (`apps/gateway`): forwards `/api/v1/*` only (GET, HEAD, POST, OPTIONS; 405
  otherwise), adds `X-Gateway-Secret`, security headers, a body limit and an upstream deadline; idempotent
  GET/HEAD requests get one retry after a connection failure, inside the same deadline (timeouts are never
  retried). `BACKEND_URL` must be https without credentials in staging and production (the secret travels
  with every upstream request), redirects from the backend are refused (`502 UPSTREAM_REDIRECT`), and the
  target host always comes from `BACKEND_URL`, never from the request. Malformed numeric variables fall back
  to the defaults instead of disabling the body limit or aborting every request (fixed 2026-10-25). The
  script entry point (`src/worker.ts`) exports only the handler: the workers runtime refuses a main module
  with non-handler exports, which the v7 source had (found 2026-10-25, see DEPLOYMENT.md).
* **Last-known-good copies of public data (contract C2, 2026-10-25).** For exactly `GET
  /api/v1/public/market-snapshot` and `GET /api/v1/plans` with **no** `Authorization` header and no query
  string, the Worker stores the backend's `200` JSON answer in KV under a fixed key
  (`edge-cache/public/market-snapshot`, `edge-cache/public/plans`; the key never contains request input) and
  serves it, marked `X-ICP-Cache: stale` with `X-ICP-Cached-At`, when the backend fails (network error, 8 s
  timeout, status ≥ 500, or a non-JSON 4xx page from in front of the backend). KV use is bounded: at most one
  read per key per isolate per 5 minutes (a missing or corrupt copy is remembered too) and one write per key per
  5 minutes (exact per isolate, best effort across isolates), and only when the content changed or the stored
  copy is a day old: the metadata carries `contentSha256` (the JSON without the volatile `generated_at`) next
  to the byte `sha256`, so an unchanged snapshot is rewritten once a day, not on every answer; a failed write
  is not retried for 5 minutes (2026-10-25/26, wave B). Controls against cache poisoning and misuse: only anonymous answers are stored (they
  are the guest view, without forecast values, by policy v4), only a JSON object or array of at most 256 KiB,
  at most one write per key per 5 minutes and only when the content hash changed or the copy is a day old; a stored value is served only if
  its bytes match the stored byte SHA-256 (the content hash is used for change detection only) and its timestamp parses (re-serialised before it reaches a header); the
  internal data plane refuses the `edge-cache/` prefix, so neither the backend nor a holder of the gateway
  secret can read or plant a copy through it; a request with any `Authorization` header (even empty or
  malformed) never reads or writes a copy. KV failures fall back to the normal error. Tests:
  `apps/gateway/test/edge-cache.test.ts`; procedure and budget: [OPERATIONS.md](OPERATIONS.md#stale-public-data-last-known-good-copies-at-the-gateway).
* **Upstream error pages (2026-10-25).** Any upstream error answer (4xx or 5xx) that is not JSON (Cloudflare
  52x/530 pages of an offline tunnel, Cloudflare 1015/1020 pages, the Render router's 404, proxy pages) is
  discarded and replaced by the standard envelope `UPSTREAM_TIMEOUT_OR_UNAVAILABLE` (`504` for 504/522/524,
  otherwise `502`). Those pages could name the backend's host. The backend answers every error as JSON
  (`backend/icp_api/errors.py`, `middleware.py`; the gateway forwards no `Origin` header, so CORS preflight
  replies cannot occur), so only pages produced in front of it are affected; JSON errors keep their status and
  body. The browser therefore never receives an HTML error page from `/api/`; successful answers pass through
  as sent (downloads are CSV, PDF or Excel). The Worker logs the upstream status with the request id only.
* **Client IP (SEC-01).** Cloudflare overwrites `CF-Connecting-IP` with the Worker's egress address on
  subrequests to another Cloudflare-proxied host (Quick Tunnel, Render), so the Worker sends the visitor's
  address in its own header `X-ICP-Client-IP` (taken from the Worker's inbound `CF-Connecting-IP`; a
  client-supplied copy is never forwarded because only allow-listed request headers are copied).
  `security.client_key` uses `X-ICP-Client-IP`, then `CF-Connecting-IP`, then `X-Forwarded-For` **only when
  the gateway secret verifies**, and ignores values that are not IP addresses. Hosted behaviour still needs
  one staging confirmation (log a hash of the received value).
* **Edge limiter (SEC-02).** Every `/api/v1/*` request is counted in a **client-IP bucket**; a second,
  per-credential bucket is added only for well-formed `icp_sess_…`/`icp_live_…` tokens, and the request is
  refused when either is exhausted, so rotating junk `Authorization` headers no longer yields fresh buckets.
  `/api/v1/auth/*` is keyed on the client IP only. Users behind one NAT share the IP bucket.
* **Invalid credentials (SEC-02).** Invalid or unknown tokens are limited per client on every route (bucket
  `auth_failed`, `RATE_LIMIT_DEFAULT_PER_MINUTE`) before anything is written, and `auth.failed` audit rows are
  aggregated: at most one row per (client, minute), recording the aggregation. Account, organisation,
  operator (`/admin/*`), job and audit routes also carry the per-client `default` rate limit.
* Sign-in abuse: `/auth/login`, `/auth/register` and `/auth/password` share an `auth` limit of
  `RATE_LIMIT_AUTH_PER_MINUTE` (default 10) per client IP. Failed sign-ins are counted per
  **(SHA-256 of the e-mail, client IP)**: after `LOGIN_FAILURES_MAX` (5) failures within
  `LOGIN_FAILURE_WINDOW_SECONDS` (900 s) that address is paused **from that IP only**
  (`429 RATE_LIMITED`, `details.scope=client`), so a third party can no longer lock the owner out. An
  account-wide ceiling `LOGIN_ACCOUNT_FAILURES_MAX` (100 failures from all IPs in the window,
  `scope=account`) limits distributed guessing; it must be ≥ the per-pair limit in deployed
  environments. A successful sign-in clears only the (e-mail, IP) pair it came from (fixed 2026-10-20: it used to reset every IP and the account ceiling); unknown e-mails behave identically and cost
  one scrypt like known ones, so sign-in 429s do not reveal accounts. The client IP comes from the
  gateway headers described above (only when the gateway secret verifies).
* **Registration and invitation enumeration (SEC-04).** Registering a taken address still answers
  `409 EMAIL_TAKEN` (self-service registration was kept and there is no e-mail flow), but the request now
  costs the same scrypt work as a successful registration, so timing no longer distinguishes it. Inviting
  an address that already has an account, in this **or any other** organisation, answers one generic
  `409 EMAIL_UNAVAILABLE` with the same message, so tenants cannot probe each other's members; the invite
  endpoint is in the per-IP `auth` bucket. Residual: the 409 itself still reveals that an address is
  registered (see Known gaps). E-mail addresses are normalised with NFKC + casefold (AUTH-02).
* **Password change** (`/auth/password`): wrong current passwords are throttled per user
  (`LOGIN_FAILURES_MAX` within `LOGIN_FAILURE_WINDOW_SECONDS`, then `429` even with the right password until
  the window passes).
* **Admin boundaries (SEC-03).** Resetting the password of, changing the role of, or disabling an `admin`
  requires the owner; admins manage analysts and viewers.
* **Plan features (SEC-11)** are re-checked on every scenario and alert write path and on
  scenario-comparison reports (`403 FEATURE_NOT_IN_PLAN`), not only on create.
* Generated files: every dynamic string placed in a PDF is XML-escaped before ReportLab parses its
  paragraph markup. As a second layer, ReportLab's loaders (`open_for_read` in `reportlab.lib.utils` and
  `reportlab.pdfbase.pdfdoc`) are replaced for every PDF build with one that accepts only in-memory
  file objects, and `trustedSchemes` is empty: markup can make the server neither fetch a URL (http,
  https, `//host`, UNC) nor read a local file (absolute, relative, `file:`). Before 2026-10-20 the
  trusted-scheme setting alone did not stop local paths, because ReportLab tries a plain `open()` first.
  Spreadsheet cells (CSV, Excel and the data-export CSVs) are neutralised by `cell_safe`: a leading TAB,
  CR or LF is always prefixed with `'`; otherwise the first non-whitespace character is NFKC-folded and
  checked against `= + - @` and U+2212, so leading spaces, NBSP and full-width look-alikes no longer
  bypass it; leading invisible characters (categories Cc, Cf such as U+200B..U+200D, U+2060 and the BOM,
  Zs, Zl, Zp) are skipped before that check and a leading control character is always prefixed (SEC-14);
  plain numbers stay unchanged. Excel cells are always stored as text (formula injection). Failed reports store a generic `REPORT_FAILED` message with the request id; exception
  text stays in the server log.
* Tenant-facing alerts and insights never include raw ingestion error text (upstream responses,
  paths); they name the run id, and operators read details on the Operations pages.
* Backend: request ids on every response (including unhandled `500 INTERNAL_ERROR`, which carries the
  real `request_id` and never exception text), body size limit, request timeout, CORS allow-list,
  per-process rate limits (defence in depth; operator reads 240/min and writes 30/min in separate
  buckets), structured JSON logs without secrets or full e-mail addresses (tested), Prometheus metrics
  behind a bearer token (`METRICS_TOKEN`, mandatory in staging/production because `/metrics` bypasses
  the gateway secret; optional locally).
* **Static site headers** (`apps/web/public/_headers`, served by Workers Static Assets): CSP `default-src
  'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self';
  connect-src 'self'; manifest-src 'self'; media-src 'self'; frame-src 'none'; worker-src 'none'; object-src
  'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'` (no third-party origin: fonts are self-hosted under `/fonts`, images under
  `/img` and `/brand`), `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
  `Referrer-Policy: strict-origin-when-cross-origin`, `Cross-Origin-Opener-Policy: same-origin`,
  `Cross-Origin-Resource-Policy: same-origin`, `X-Permitted-Cross-Domain-Policies: none` and a
  `Permissions-Policy` that disables camera, microphone, geolocation, payment, USB/serial/HID/MIDI, sensors,
  display capture and topics. Hashed `/assets/*` and versioned `/fonts/*` are `immutable` for a year. Checked
  on 2026-10-25 with local `wrangler dev` and headless Chrome on the baseline build: no CSP or
  Permissions-Policy console errors, the self-hosted font loads, an injected inline script is blocked.
* Response headers on every API response (backend and Worker): `X-Content-Type-Options: nosniff`,
  `X-Frame-Options: DENY`, `Content-Security-Policy: default-src 'none'; frame-ancestors 'none'` (not
  on `/docs`), `Cross-Origin-Resource-Policy: same-origin`, Referrer-Policy, `Cache-Control: no-store`;
  `Strict-Transport-Security` when deployed. Early gateway/body-limit/timeout replies get the same headers
  (Worker error replies too, since SEC-09: 404/405/413/429/502/503/504 are tested).
* **Internal data plane (SEC-10).** `/api/internal/*` (D1 and KV for the backend) requires the gateway
  secret and accepts exactly **one** SQL statement per request that starts with `SELECT`, `INSERT`,
  `UPDATE`, `DELETE` or `WITH`; after blanking literals, quoted identifiers and comments, the keywords
  `ATTACH`, `DETACH`, `PRAGMA`, `VACUUM`, `REINDEX`, `ANALYZE`, `CREATE`, `DROP`, `ALTER`, `BEGIN`, `COMMIT`,
  `ROLLBACK`, `SAVEPOINT` and `RELEASE` are refused anywhere, with one exception: `PRAGMA table_info(<identifier>)`
  (used by `clone-state`). Refused statements get `403` and never reach D1; bound parameters are capped at
  100 (the D1 limit). Schema changes go only through `wrangler d1 migrations apply`. Residual: one secret
  is still used in both directions (Worker → backend and backend → data plane).
* **Bind address.** The backend binds to `API_HOST`, or to loopback when it is unset; deployed environments
  refuse to start on a loopback bind, so the container image and `docker-compose.yml` set `API_HOST=0.0.0.0`
  explicitly (no wildcard default in code).
* **Secret rotation (OPS-04).** Both sides accept the previous gateway secret(s) listed in
  `GATEWAY_ACCEPTED_SECRETS` (comma-separated, each ≥ 32 characters when deployed, compared in constant time)
  next to `GATEWAY_SHARED_SECRET`; outbound calls always use `GATEWAY_SHARED_SECRET`. Procedure:
  [OPERATIONS.md](OPERATIONS.md#rotating-the-gateway-shared-secret).
* Public health: `/`, `/health/live`, `/health/ready` and the Worker's `/api/gateway/health` return only
  `{"status"}` to non-operators; operators get the readiness detail.
* API-key quota: requests with an organisation API key are counted per UTC month (`usage_events`,
  `api.request`) and refused at the plan's `api_requests_per_month` with `429 QUOTA_EXCEEDED`; quota
  headers on every successful API-key response. Sessions and operators are not quota-limited. A unit is
  reserved atomically (one conditional insert, so concurrent requests cannot overshoot) and refunded when
  the final status is 401, 403 or 429 (SEC-07). A gateway GET retry after a dropped connection can still
  count twice.
* Session and API-key "last used" timestamps are written at most every 5 minutes per credential (D1 write
  budget).
* Single instance: rate limits and sign-in counters live in process memory, so the backend must run as one
  process. `icp-api serve` always starts one uvicorn worker (ignoring a platform `WEB_CONCURRENCY`) and
  `API_INSTANCES` other than 1 refuses to start. D1-backed counters were rejected because every request
  would cost a D1 write on the free tier.
* Uploads (`/datasets/{dataset}/imports`): CSV parsed with strict schema/range/duplicate
  validation, previewed before commit, destructive changes require `confirm_overwrite`; downloaded
  source documents are cached in `var/cache/sources` and never executed; XML from Bank Indonesia is
  parsed with `defusedxml`; models are serialized with skops (no pickle) and loaded with an explicit
  trusted-type allow-list.

## Audit and privacy

* `audit_events` records sensitive actions and denied attempts with actor, action, outcome, target,
  request id and non-secret details (no passwords, tokens, raw IPs).
* Personal data stored: member e-mail, display name, last login time, user-agent of sessions.
  Retention: sessions expire after 30 days; audit events are kept until an operator prunes them with
  `icp-api audit-prune` (dry run by default, `AUDIT_RETENTION_DAYS=0` keeps everything, never
  automatic, the prune itself is audited). See the in-app *Kebijakan Privasi* and
  [LEGAL_AND_PRIVACY.md](LEGAL_AND_PRIVACY.md).
* Data export: organisation owners download a ZIP of their own organisation's data (`GET /org/export`,
  session only, audited). It is built in memory and never stored, excludes password, session-token and
  API-key hashes and other tenants' rows, and neutralises CSV formulas. Field list (SEC-12):
  `sessions.json` holds **only the requesting owner's own** sessions (other members' devices and sign-in
  times are their personal data); in `audit_events.json` rows whose actor is a platform operator show
  `actor = "platform-operator"` and drop the operator-only `reason`/`note` details; in `plan_requests.json` and
  `deletion_requests.json` `resolved_by` reads `platform-operator` while the customer-facing
  `resolution_note` is kept (the customer APIs `GET /org/plan-requests` and `/org/deletion-requests` apply
  the same redaction). Members (no secrets), API-key metadata (no hash), scenarios, alerts, reports,
  forecasts, usage and the organisation row are exported unchanged.
* Withdrawing a deletion request is possible only for the member who made it (SEC-05); the owner sees
  every request, operators execute or decline.
* Deletion requests: members request deletion of their own account, owners of the organisation; an
  operator executes or declines. Execution anonymises in place (e-mail `deleted+<id>@invalid.local`,
  name, unusable password, disabled status, sessions and API keys revoked; organisation renamed and
  suspended, scenarios archived and renamed, alert rules disabled). Audit, usage counters,
  forecast/report records and official data are kept; nothing is hard-deleted. Every step is audited.
* No payment data is processed: the Midtrans sandbox adapter is disabled by default (`BILLING_ENABLED=false`),
  refuses the production environment, and never handles card data (payment happens on the provider's
  Snap page); see [BILLING.md](BILLING.md).

## Dependency and configuration checks

* `requirements/constraints.txt` pins the shipped Python set (the API image installs
  `.[api,cloud,postgres,ingest] -c requirements/constraints.txt` on a base image pinned by digest).
* Scans run in the CI `security` job (pinned pip-audit 2.10.1 and bandit 1.9.4, plus
  `npm audit --omit=dev --audit-level=moderate` and the secret scan) and can be run by hand from a
  throwaway virtual environment so the project venv stays untouched:
  ```bash
  pip-audit -r requirements/constraints.txt --no-deps --disable-pip
  bandit -r backend/icp_api pipeline/icp_ml -ll --ignore-nosec
  npm audit --omit=dev
  node scripts/secret-scan.mjs --self-test && node scripts/secret-scan.mjs
  ```
  Results of the latest run are in [audits/REMEDIATION_LOG.md](audits/REMEDIATION_LOG.md). Run of 2026-10-25
  (local): `npm audit --omit=dev --audit-level=moderate` 0 vulnerabilities; `pip-audit -r
  requirements/constraints.txt --no-deps --disable-pip` (pip-audit 2.10.1 from the project venv) **39 known
  vulnerabilities in 3 pinned packages**: `pillow` 12.3.0 (37 advisories, fixed in 13.0.0), `mlflow` 3.17.1
  (1, fixed in 3.17.2), `certifi` 2026.7.22 (1, fixed in 2026.10.19), all published after the clean run of
  2026-10-20; bandit was not run that day. **Raised the same day (wave B):** `certifi` 2026.10.19,
  `mlflow`/`mlflow-skinny`/`mlflow-tracing` 3.17.2, and `pillow` 13.0.0 for Python ≥ 3.11 (environment marker in
  `requirements/constraints.txt`). Pillow 13 requires Python 3.11, which CI and the API image use; a Python 3.10
  development venv keeps `pillow` 12.3.0 (accepted residual risk DEP-01 in
  [audits/RESIDUAL_RISKS.md](audits/RESIDUAL_RISKS.md): the code never decodes an untrusted image). Verified:
  targeted backend and pipeline tests (reports and PDFs, forecasts, model registry and bundles, insights, storage)
  pass on Python 3.10 with the new mlflow and certifi (801 passed) and on Python 3.11 with Pillow 13 (815
  passed). Caveat: the first targeted run right after the mlflow upgrade (other files, backend before
  pipeline, 761 tests; the exact order was not recorded) had 8 setup errors: the module fixtures of
  `test_forecast_explain.py` and `test_d_evaluation.py`, whose `refresh_forecast` job ended with
  `MODEL_UNAVAILABLE`. They did not recur in the later runs above nor in four reruns on 2026-10-26 (two other
  orders, a cold bytecode cache, and a forced simultaneous first initialisation of the session's fresh MLflow
  store, from which the API process recovers), so their cause is not established; the default-order full
  suite is the reference. Real PDF reports rendered by a local instance open in PyMuPDF with the same page counts and
  byte-identical embedded charts under Pillow 12.3 and 13.0. Scans after the bump (pip-audit 2.10.1 and
  bandit 1.9.4 in throwaway venvs): `pip-audit -r requirements/constraints.txt --no-deps --disable-pip` under
  Python 3.11 (the CI interpreter) **no known vulnerabilities** (134 components; the CycloneDX SBOM also
  generates); under Python 3.10 the same command evaluates the `< "3.11"` marker and reports the 37 Pillow
  12.3.0 advisories; `bandit -r backend/icp_api pipeline/icp_ml -ll` (with and without `--ignore-nosec`) no
  issues, 0 `# nosec`; `npm audit --omit=dev --audit-level=moderate` 0; secret scan and its self-test clean.
  bandit runs with
  `--ignore-nosec`: suppression comments have no effect, and the code base contains no `# nosec` (since
  2026-10-20 every SQL statement is a constant string or comes from a constant map, and the bind address is
  explicit configuration). A new finding is fixed in code, never suppressed. The development venv also
  contains Jupyter/Streamlit tooling with known advisories; none of it is shipped in the API image.
* The secret scan (`scripts/secret-scan.mjs`) checks provider-specific token formats and high-entropy
  credential assignments in sources and in the built bundles (`apps/*/dist`), exempts placeholders per value,
  never opens local `.env*`/`.dev.vars*` files and reports file, line and rule only. It does not scan Git
  history.
* `load_settings()` validates deployment configuration (`APP_ENV=staging|production` requires
  remote stores, gateway secret ≥ 32 chars, `METRICS_TOKEN` ≥ 32 chars, no SQLite/local paths); see
  [ENVIRONMENTS.md](ENVIRONMENTS.md#deployed-configuration-validation-enforced-at-startup).
* Threat model and security acceptance tests: [THREAT_MODEL.md](THREAT_MODEL.md).

## Cookie-session migration design (not implemented)

Goal: move the member session token out of `localStorage`, where XSS can read it, into a cookie page
scripts cannot read. Bearer tokens stay for API keys, scripts and the operator console.

**Why it fits.** The Worker serves the SPA and `/api/*` from **one origin** (`run_worker_first: ["/api/*"]`),
and the Vite dev proxy is same-origin too. A host-only first-party cookie therefore needs no CORS
credentials; `CORS_ALLOWED_ORIGINS` stays strict and `allow_credentials` stays false.

**Cookie.** `Set-Cookie: __Host-icp_sess=<icp_sess_…>; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=2592000`.
- The `__Host-` prefix forces Secure, Path=/ and no Domain, so no subdomain can set or read it.
- `SameSite=Lax` keeps top-level navigation signed in and keeps cross-site POSTs cookie-less.
- Local `http://localhost` cannot use Secure/`__Host-`. There the name is `icp_sess` without Secure, and only
  when `APP_ENV=local`; settings validation guards this.

**Server.**
1. **Login and registration** set the session cookie and a readable CSRF cookie
   `__Host-icp_csrf=<random>` (same attributes, without HttpOnly). In `cookie_only` mode the JSON `token` is
   omitted for browser clients; in `dual` mode it is still returned.
2. **`authenticate`** uses `Authorization: Bearer` first (unchanged), otherwise `__Host-icp_sess`. A
   cookie-authenticated unsafe method needs all three of the following, or it gets `403 CSRF_FAILED`. GET and
   HEAD stay side-effect free; the only write is the throttled `last_seen_at` touch.
   - `X-CSRF-Token` equal to the CSRF cookie (constant-time double submit);
   - `Origin`/`Referer` equal to the configured public origin;
   - `Sec-Fetch-Site: same-origin` when that header is present.
3. **Logout and "sign out everywhere"** revoke server-side as today and also clear both cookies. The idle
   timeout and the session list/revoke endpoints are unchanged, because the cookie carries the same opaque
   token.
4. **Gateway** forwards only `__Host-icp_*` cookies (re-serialised) and `x-csrf-token`, passes `set-cookie`
   back, never logs either, and keys cookie sessions on IP + a hash of the session cookie.

**Frontend.**
- `client.ts` drops the `localStorage` token and uses `credentials: "same-origin"`.
- Unsafe requests send `X-CSRF-Token` read from the CSRF cookie.
- "Signed in" comes from `GET /auth/whoami`.
- The operator console keeps its `sessionStorage` bearer token. Cloudflare Access in front of the operator
  console is preferable.

**Rollout and rollback.** `SESSION_COOKIE_MODE=off|dual|cookie_only`, default `off`.
1. Ship `dual` first: it sets the cookie and also returns the JSON token, and the client prefers the cookie.
2. After 30 days, every live session was created in `dual`; switch to `cookie_only` and delete the
   `localStorage` key.
3. Rollback is `off`: cookies are ignored. No data migration is needed in either direction.

**Tests required when implemented.**
- Cookie attributes are exact.
- A missing or wrong `X-CSRF-Token` gives `403`; a foreign `Origin` gives `403`; GET works without CSRF.
- Logout clears the cookies.
- A bearer API key works without cookies or CSRF.
- `dual` and `off` modes behave as described.
- The gateway cookie and header allow-list holds.
- A jsdom check confirms that scripts cannot read the session cookie.

**Residual risk.** XSS can still *use* a session while the page is open, but can no longer exfiltrate the
token. The CSP `script-src 'self'` remains the main control.

## Known gaps

* No e-mail delivery: the reset/verification/invitation token service exists but is disabled
  (`EMAIL_ENABLED=false` is enforced), so password resets and invitations are still handed over out of
  band by an admin as expiring temporary passwords. Without e-mail, `409 EMAIL_TAKEN` on registration
  still reveals that an address has an account (timing is equalised; invitations answer a generic
  `EMAIL_UNAVAILABLE`).
* The member session token is still in `localStorage` (cookie migration designed, not implemented); the
  legacy `sessions` array of `GET /auth/me` still includes raw user agents until the UI uses
  `GET /auth/sessions`.
* Billing is sandbox-only and disabled: no automatic plan activation on payment, no refunds from the
  product, no production keys; the sandbox key prefix check relies on an unverified convention.
* The client-IP header behind the Worker (SEC-01) is unit-tested but not yet confirmed on the hosted stack.
* No MFA; no permanent account lockout (failed sign-ins are throttled per (address, IP) and per address).
* Sign-in counters are in memory (reset on restart). An attacker controlling ≥ 20 IP addresses can still
  trigger the 15-minute account-wide pause after 100 failures (deliberate ceiling against distributed
  guessing).
* Rate limiting is per process; horizontal scaling needs a shared store first (single instance enforced).
* API keys have no scopes or expiry and survive the deactivation of the member who created them
  (organisation-level credential; revoke explicitly).
* There is no owner transfer, so an owner can only request deletion of the whole organisation; deletion
  requests have no automatic grace period (the operator decides).
* The ReportLab loader replacement depends on ReportLab's internal entry points; the regression tests
  cover the known vectors, a future ReportLab version could add another loader.
* The static CSP keeps `style-src 'unsafe-inline'` (React and SVG style attributes); script execution stays
  `'self'` only. No `Strict-Transport-Security` on static assets while the site runs on `*.workers.dev`
  (SEC-15: add it with a custom domain; the `.dev` TLD is HSTS-preloaded and the Worker's `/api` answers already
  send it).
* A last-known-good public copy can be old: it is labelled with the time it was stored, it ages while the
  backend is down, and it stops being refreshed when the account's daily KV write quota is used up. The copy
  is rewritten when its content changes and otherwise once a day, so the stored time can be up to about a day
  older than the backend's last answer (residual risk GW-C2). Purging is manual (`wrangler kv key delete …
  --remote`, OPERATIONS.md).
* A Python 3.10 development environment keeps Pillow 12.3.0 with 37 published advisories (Pillow 13 needs
  Python 3.11; CI and the API image get 13.0.0). Accepted because no code path decodes an untrusted image
  (DEP-01 in audits/RESIDUAL_RISKS.md).
