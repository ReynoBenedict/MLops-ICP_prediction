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
- **Limits.** Bodies ≤ 2 MB (`413`), CSV imports ≤ 1 MB, rate limits per client (`429 RATE_LIMITED` with
  `Retry-After`), request timeout 30 s (`504 REQUEST_TIMEOUT`; [timeout budget](OPERATIONS.md#timeout-budget)). Operator reads (`GET`/`HEAD`) use their own bucket
  (`RATE_LIMIT_ADMIN_READ_PER_MINUTE`, default 240/min); operator writes keep `RATE_LIMIT_ADMIN_PER_MINUTE` (30/min).
- **Sign-in throttle.** `POST /auth/login` answers `429 RATE_LIMITED` with `Retry-After` and
  `details: {retry_after_seconds, scope}`: `scope=client` after `LOGIN_FAILURES_MAX` (5) failures for one
  e-mail from one client IP within `LOGIN_FAILURE_WINDOW_SECONDS` (900 s); `scope=account` after
  `LOGIN_ACCOUNT_FAILURES_MAX` (100) failures for that e-mail from all IPs. Unknown e-mails behave the same;
  a successful sign-in clears only the (e-mail, IP) pair it came from. Login, registration and password change also share the per-IP
  `auth` bucket (`RATE_LIMIT_AUTH_PER_MINUTE`, default 10/min).
- **API-key quota.** Requests authenticated with an organisation API key count against the plan's
  `api_requests_per_month` (UTC calendar month). Successful responses carry `X-Quota-Limit`,
  `X-Quota-Remaining`, `X-Quota-Reset`; at the limit the API answers `429 QUOTA_EXCEEDED` with `Retry-After`
  and `details: {limit, used, period, reset_at, plan}` (refused requests are not counted). Sessions and
  operators are not quota-limited.
- **Idempotency.** `POST /forecasts` and `POST /reports` accept an optional `Idempotency-Key` header
  (`^[A-Za-z0-9._:-]{8,128}$`). Within 24 h, the same caller and organisation repeating the key with the same
  body gets the original succeeded result with `Idempotent-Replayed: true` and no new usage; a different body
  gets `409 IDEMPOTENCY_KEY_REUSED`. On `POST /reports`, a repeat while the first request with that key is
  still being generated (for example after its `504 REQUEST_TIMEOUT`: generation goes on in the worker
  thread and the report is stored only when it ends) gets `409 IDEMPOTENCY_IN_PROGRESS` with `Retry-After: 15`
  and `details.retry_after_seconds`, and no second report; once the first request has ended, the repeat
  replays its report (or, after a failure, runs again). The reservation is held in the one backend process
  (`API_INSTANCES=1`). On `POST /forecasts`, two concurrent first requests with one key can both run (no
  unique index). The gateway forwards the header and CORS exposes `Idempotent-Replayed`.
- **Response headers.** Every API response carries `Cache-Control: no-store` (files: `private, no-store`),
  `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Content-Security-Policy: default-src 'none';
  frame-ancestors 'none'` (not on `/docs`), `Cross-Origin-Resource-Policy: same-origin`, and
  `Strict-Transport-Security` in staging/production. Unhandled errors return `500 INTERNAL_ERROR` with the
  real `request_id` and `X-Request-ID`, never exception text.

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
| `/data/summary`, `/data/history` and `/data/verification` (plan `history_years`), `/data/evidence`, `/datasets/active`, `/models`, `/models/versions/{v}` (no build internals for members), `/alerts/kinds` | 401 | ✓ | ✓ | ✓ | ✓ |
| `/datasets/versions/{id}/download` | 401 | active versions, plan `history_years` | same | same | all, unclipped |
| `POST /org/plan-requests`, `…/{id}/withdraw` | 401 | 403 | admin+ (session) | 403 | 403 |
| `GET /org/plan-requests` | 401 | own | own | 403 | 403 |
| `GET /admin/plan-requests`, `POST /admin/plan-requests/{id}/resolve` | 401 | 403 | 403 | 403 | ✓ |
| `GET /org/export` | 401 | 403 | owner only (admin/analyst 403) | 403 | 403 |
| `POST /org/deletion-requests` (`account` / `organization`) | 401 | own account / 403 | own account; `organization` owner only | 403 | 403 |
| `GET /org/deletion-requests`, `…/{id}/withdraw` | 401 | own | own (owner: organisation) | 403 | 403 |
| `GET /admin/deletion-requests`, `…/{id}/resolve` | 401 | 403 | 403 | 403 | ✓ |
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
| `GET /`, `GET /health/live`, `GET /health/ready` | – | `{"status"}` only for non-operators (readiness `503` when not ready); with an operator bearer `/health/ready` adds environment, version and per-check detail. Checks run concurrently, each bounded at 5 s, so a probe answers within about 5 s; a check still running from an earlier probe is shared by later probes, never started again. The production model is loaded in the background at start-up and, until it is loaded, the model checks wait at most 0.5 s and report `degraded` ("still loading after start-up") instead of blocking, so a probe during the warm-up answers in well under a second. Outside `/api`; not exposed through the gateway |
| `GET /api/v1/system/status` | op | Dependency checks, configuration summary and the call's `request_id` |
| `GET /api/v1/auth/whoami` | – | Who am I (works for every principal, including `must_change_password`) |
| `POST /api/v1/auth/register` | – | `{organization_name, email, name, password, accept_terms}` → trial organisation + owner session (`201`); disabled when `PUBLIC_REGISTRATION_ENABLED=false` |
| `POST /api/v1/auth/login` / `POST /auth/logout` | – / member | Session token shown once / revoke. A correct but expired temporary password gets `403 TEMP_PASSWORD_EXPIRED`; sessions idle longer than `SESSION_IDLE_TIMEOUT_HOURS` (default 168 h) are revoked |
| `GET /api/v1/auth/me`, `POST /auth/me` (`{name}`), `POST /auth/password` | member (session) | Profile, rename, change password (revokes **all** of the user's sessions, including the current one; the UI signs out). Wrong current passwords are throttled per user (`429 RATE_LIMITED`). `UserPublic` carries `email_verified` and, while a temporary password is pending, `temporary_login_expires_at`. The legacy `sessions` array of `/auth/me` is superseded by `/auth/sessions` |
| `GET /api/v1/auth/sessions` | member (session) | Active sessions: `id`, `device` label, `created_at`, `last_seen_at`, `expires_at`, `idle_expires_at`, `current` (no token material, no raw user agent). Allowed while a password change is pending |
| `POST /api/v1/auth/sessions/{id}/revoke`, `POST /auth/sessions/revoke-others` | member (session) | End one own session (`204`; `404` for another user's or an ended session) / sign out everywhere else (`{revoked}`; the current session stays) |
| `POST /api/v1/auth/password-reset/request` (`{email}`), `POST …/password-reset/confirm` (`{token, new_password}`) | – | E-mail password reset. **Disabled: `503 EMAIL_DISABLED`** while `EMAIL_ENABLED=false` (enforced until a provider exists). When enabled: `202` with the same answer whether or not the address exists / `204`, every session ends; invalid, used or expired token `400 TOKEN_INVALID` |
| `POST /api/v1/auth/email/verify/request`, `POST …/email/verify/confirm` (`{token}`) | member (session) / – | E-mail verification (`409 EMAIL_ALREADY_VERIFIED`); disabled like the reset (`503 EMAIL_DISABLED`) |
| `POST /api/v1/auth/invite/accept` (`{token, new_password}`) | – | Accept an e-mailed invitation (sets the password, marks the address verified); disabled (`503 EMAIL_DISABLED`) |
| `GET /api/v1/plans` | – | Plan catalogue with entitlements (no prices; `price_note_id`) |
| `GET /metrics` (outside `/api`) | Prometheus bearer token | Required (`METRICS_TOKEN` ≥ 32 chars) in staging/production; open locally when unset |

### Organisation (tenant) and operator tenant administration
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/org`, `POST /org` (`{name}`) | member (session) / member(admin) | Organisation, entitlements, counts, usage this month |
| `GET /api/v1/org/members` | member (session only) | Members |
| `POST /api/v1/org/members` | member(admin) | Invite `{email, name, role}` → one-time temporary password with `expires_at` (`TEMP_PASSWORD_TTL_HOURS`, default 72 h; seat limit enforced). An address that already has an account in any organisation gets the generic `409 EMAIL_UNAVAILABLE` |
| `POST /api/v1/org/members/{id}/invitation-email` | member(admin) | Send the invitation by e-mail (`202`; `409 MEMBER_ALREADY_ACTIVE`; an admin target needs the owner). Disabled: `503 EMAIL_DISABLED` |
| `POST /api/v1/org/members/{id}`, `…/reset-password` | member(admin) | Role/status change, temporary password (with `expires_at`). Acting on another **admin** requires the owner (`403 FORBIDDEN`) |
| `GET/POST /api/v1/org/api-keys`, `POST …/{id}/revoke` | member(admin), plan with API access | Keys shown once; stored hashed |
| `GET /api/v1/admin/organizations` | op | Tenant list |
| `POST /api/v1/admin/organizations/{id}/plan` / `/status` | op | `{plan, reason}` / `{status, reason}` (billing integration point) |
| `POST /api/v1/org/plan-requests` | owner/admin (session) | `{requested_plan, note?≤500}` → `201` pending request; `409` while one is pending; `422` for the current plan. No payment is taken |
| `GET /api/v1/org/plan-requests`, `POST …/{id}/withdraw` | member / owner-admin | Own organisation's requests; withdraw a pending one (`409` otherwise) |
| `GET /api/v1/admin/plan-requests[?status=]`, `POST …/{id}/resolve` | op | `{decision: approve\|decline, note?}`; approve changes the plan through the audited operator path (migration 0005 required) |
| `GET /api/v1/org/export` | owner (session only) | ZIP of the organisation's own data (`manifest.json`, organisation, members, sessions, API keys without hashes, scenarios, alert rules/events, report and forecast records, usage, plan and deletion requests, related audit events; JSON plus formula-neutralised CSV for members, scenarios and usage). Generated on request, never stored; `private, no-store`; audited `org.export`. Admin/analyst/viewer, API keys and operators get `403` |
| `POST /api/v1/org/deletion-requests` | member (session) | `{scope: account\|organization, reason?≤500}` → `201`. `account`: own account (the owner gets `409 OWNER_ACCOUNT` and uses `organization`); `organization`: owner only. One pending request per organisation/scope/user (`409`) |
| `GET /api/v1/org/deletion-requests`, `POST …/{id}/withdraw` | member (session) | Owner sees the organisation's requests, others their own; withdraw while pending (**requester only**; `404` for anyone else, `409` when no longer pending) |
| `GET /api/v1/admin/deletion-requests[?status=]`, `POST …/{id}/resolve` | op | `{decision: execute\|decline, note?}`. Execute anonymises, never hard-deletes: account → e-mail `deleted+<id>@invalid.local`, name "Pengguna dihapus", unusable password, disabled, sessions revoked; organisation → every member anonymised, API keys revoked, organisation renamed and suspended, scenarios archived and renamed, alert rules disabled. Audit, usage, forecast/report records and official data are kept. Migration 0006 required |

### Data
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/public/market-snapshot` | – | Landing-page snapshot: latest official ICP, previous, MoM, Brent, USD/IDR, rupiah equivalent, freshness, source, `updated_at`; `latest_forecast` is a teaser `{available, issue_date, forecast_origin, horizon}` without predicted values. Optional (contract C1, 2026-10-25): `recent`, `coverage`, `icp_evidence`, `generated_at` - see [Public market snapshot](#public-market-snapshot) |
| `GET /api/v1/data/summary` | user | Per-dataset coverage/latest/freshness (`datasets`), ICP–benchmark `relationships`, `icp_idr` |
| `GET /api/v1/data/history?start=&end=` | user | Monthly table: ICP, WTI, Brent, Dubai, USD/IDR (avg, EOM), `icp_idr`; gaps are `null`. Clipped to the plan's `history_years` (ending at the latest official ICP month); the response carries `history_years` (null = unlimited) and `earliest_period` |
| `GET /api/v1/data/quality?dataset=` | op | Quality report of the active version |
| `GET /api/v1/data/evidence?dataset=&period=` | user | Source evidence per month (document checksum, method, verification status, reference). Benchmark/FX rows report `provider_published`, never `verified` |
| `GET /api/v1/data/verification?dataset=` | user (plan `history_years`) | Per-month `status`, `basis` (`two_documents`, `decree_text`, `document_plus_legacy`, `single_document`, `ocr_single`, `conflict`, `no_evidence`, `provider_published`), number of agreeing ESDM documents, references, bilingual `label`/`explanation`, `counts`, `legend`; per month `dissent[]` (other source readings that differ from the value used, with kind, reference and a neutral reason; source URL for operators only) and `dissent_label`; top-level `dissent_months` |
| `GET /api/v1/datasets/active` | user | Active versions + provenance + freshness for every dataset |
| `GET /api/v1/datasets/versions[?dataset=]`, `GET …/{id}` | op | Version history, record |
| `GET /api/v1/datasets/versions/{id}/download` | user (active versions) / op (any) | CSV, `private, no-store`; inactive versions are `404` for members; members' files are clipped to the plan's `history_years` (`…_from-YYYY-MM.csv`) |
| `GET /api/v1/datasets/ingestions[?dataset=]` | op | Ingestion runs (added/changed months, partial months excluded, failures) |
| `POST /api/v1/datasets/{dataset}/imports?mode=merge\|replace&commit=&confirm_overwrite=` | op | Validated import (body `text/csv`); dry run unless `commit=true` |
| `POST /api/v1/datasets/versions/{id}/activate` | op | Re-activate a validated version |

#### Public market snapshot

`GET /api/v1/public/market-snapshot` needs no account and is the only data endpoint guests can call (policy v4).
Besides the original fields it returns (all optional for older clients; contract C1):

| Field | Content |
|---|---|
| `recent` | `[{period: "YYYY-MM", icp_usd, brent_usd}]`, ascending: the last `PUBLIC_HISTORY_MONTHS` months (default 24, allowed 0-36, `0` → `[]`) ending at the latest official ICP month and never earlier than the first one. `icp_usd` is the official value of the active ICP dataset; a month without one is `null` (never `0`, never an estimate), and so is a month whose official documents disagree (verification basis `conflict`, "not used until the difference is resolved"). `brent_usd` is the stored Brent monthly average or `null`. No forecast values |
| `coverage` | `{first_period, last_period, months, confirmed_months}` of the official ICP series (`months` = months with an official value). `confirmed_months` counts months whose verification basis is `two_documents` or `decree_text`, computed by the same service as `GET /data/verification` (unclipped). `null` without ICP data |
| `icp_evidence` | For the latest ICP month: `{period, basis, label: {id, en}, documents: [{kind: "decree"\|"announcement", reference, published_on, url}]}`, or `null` when no official document is stored (a `conflict` month gets its basis and label without documents, see below). One entry per stored document (repeated retrievals of the same file count once); only documents whose reading agrees with the official value. `reference` is the decree's own number, shown only when it is verifiable (a well-formed decree number, attached to no other month's decree, not contradicted by the month's announcement); announcements have no number of their own, so their `reference` is always `null`. `published_on` (`YYYY-MM-DD`) only when stored (announcement post date); `url` only when the stored source is a public `https://….go.id` page |
| `generated_at` | ISO-8601 UTC time the response was generated. In a stored copy served by the gateway (below) it is the time that copy was generated |

A month whose official documents disagree (basis `conflict`) is withheld from guests everywhere, not only in
`recent`: when it is the latest month, `icp_usd`, `mom_change_pct` and `icp_idr` are `null` while `icp_period`
still names the month and `icp_evidence` carries the `conflict` basis and label with an empty `documents` list
(listing one side's documents would present it as the month's source, and an announcement link can name the
value); as the previous month it nulls `previous_period`, `previous_usd` and `mom_change_pct`. No month has
that basis in the current data.

The answer is the same for every caller, so the backend keeps it in memory (SEC-C-01): it is computed again
after at most 60 s, and at once after a dataset activation, an evidence write or a new system forecast in the
backend process or after any background job (ingestion, forecast refresh) ends; a repeat inside that time
reads nothing from the database. A change written by another process (the operator CLI) therefore appears
within 60 s. `generated_at` is always the time of the response.

Through the gateway, an anonymous request for this endpoint or `GET /api/v1/plans` (no `Authorization` header, no query string) is answered
with `X-ICP-Cache: live`, or, while the backend is unreachable, with the last stored copy: `200`, `X-ICP-Cache: stale`,
`X-ICP-Cached-At` (when the copy was last written: when its content last changed or, for unchanged content, its daily refresh, so at most about a day before the backend's last answer)
and `Cache-Control: no-store` (contract C2, [OPERATIONS.md](OPERATIONS.md#stale-public-data-last-known-good-copies-at-the-gateway)).
A stale copy's `freshness` describes the data at that time, so clients must not show it as current.

### Forecasts, scenarios, insights
| Method & path | Auth | Purpose |
|---|---|---|
| `POST /api/v1/forecasts` | user (plan `forecast_horizon_max`); scenario: analyst+ and feature `scenarios` | `{horizon, scenario?: {brent, dubai, wti, usd_idr: {"YYYY-MM": value}}}` → forecast record with per-month `kind`, intervals, benchmark sources, rupiah equivalent (`wti_overrides` is a deprecated alias). Optional `Idempotency-Key` header (see Conventions) |
| `GET /api/v1/forecasts/latest` | user | Latest published **system** forecast |
| (all forecast reads) | user | Read-time additions: per point `evaluation` (backtest n, MAE, MASE, direction hit rate and interval coverage for the same step and input situation, plus the evaluation period `origin_start`/`origin_end`, the naive baseline on the same cases with `beats_baseline`, `coverage_period`, `under_coverage`, `coverage_warning` and a localized `summary`), `actual` + `kind_now=official_published` once ESDM has published the month; per run `result.notes` (bilingual method/interval/evaluation), `result.input_usage` with `effect: used|ignored|rupiah_only`, `result.ignored_inputs`, `notes.ignored_inputs` and `notes.coverage_warning` and, for members, `plan_clip` (points beyond the plan horizon removed; also in the CSV export). Stored records are unchanged |
| `GET /api/v1/forecasts[?status=]` | user / op | Member: own organisation's runs plus published system forecasts; operator: all |
| `GET /api/v1/forecasts/{id}`, `GET …/{id}/export.csv` | user | Record / CSV (origin, issue date, kinds, sources, intervals); own organisation or system forecast, else `404`. Export is `Cache-Control: private, no-store` and appends read-time columns (`kind_now`, `official_icp_usd`, `official_basis`, `*_used_by_method`, evaluation and baseline MAE, coverage, `coverage_warning`) |
| `GET/POST /api/v1/scenarios`, `GET/POST …/{id}`, `POST …/{id}/archive` | feature `scenarios` (analyst+ to write), members only | Saved scenarios per organisation (plan horizon on create/update) |
| `POST /api/v1/scenarios/{id}/run` | member(analyst) | Runs the scenario → forecast record (`workflow=scenario`); plan horizon enforced |
| `POST /api/v1/scenarios/compare` | member(analyst) | `{scenario_ids (≤4), horizon?}` → baseline vs scenarios table with per-scenario `input_usage` and `ignored_inputs`; plan horizon enforced |
| `GET /api/v1/insights?lang=id\|en` | feature `insights` | Deterministic insight engine (rules in `backend/icp_api/insights`, see [INSIGHT_ENGINE.md](INSIGHT_ENGINE.md); findings without complete evidence are dropped): localized title/body/next step, `importance`, `facts`, `evidence` (datasets + versions, period window, inputs → outputs, plain-language method, model version, forecast ids). Operator-only items (ingestion failures) are returned to operators only |

### Alerts and reports
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/alerts/kinds` | user | Rule kinds (`label_id`, `label_en`) and default parameters. Events carry `title_en`/`body_en`, the subject `period` and a deep-link `link`; `created_at` is the detection time |
| `GET/POST /api/v1/alerts/rules`, `POST …/{id}`, `POST …/{id}/delete` | feature `alerts` (analyst+ to write), members only | Rules (limit per plan) |
| `GET /api/v1/alerts/events[?unread_only=]`, `POST …/events/read[?event_id=]`, `POST /alerts/evaluate` | feature `alerts`, members only | In-app notifications (de-duplicated), mark read, evaluate now |
| `GET/POST /api/v1/reports` | user | `{kind: market_monthly\|forecast\|scenario_comparison\|data_provenance, params, formats}` (at least one format, `[]` → `422`; PDF per plan, else `403 FEATURE_NOT_IN_PLAN`; XLSX is dropped on plans without Excel and an XLSX-only request then produces CSV; CSV always; `market_monthly`/`data_provenance` take no params, non-empty `params` → `422`; optional `Idempotency-Key` header). `forecast` params are `{forecast_id}` or `{horizon, scenario}` validated like `POST /forecasts`; `scenario_comparison` params `{scenario_ids, horizon?}` like `/scenarios/compare` and needs analyst+. Members list only their organisation's reports |
| `GET /api/v1/reports/{id}`, `GET …/{id}/download?format=pdf\|xlsx\|csv` | user | Report record / file; other organisations' and platform (operator) reports are `404` for members. Only a `succeeded` run lists `formats` and can be downloaded: a `failed` run has `formats: []` and its download is `404` (a file written before the failure is never offered); a failure message that is not a coded message is shown as `REPORT_FAILED: …`. A succeeded run whose file is missing from storage answers `503 REPORT_OBJECT_MISSING` |

### Models and operations
| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/models` | user | Registered versions: method, family, rule-based flag, horizon metrics (observed / hold_last), datasets, `legacy` flag |
| `GET /api/v1/models/versions/{v}` | user | + one-step backtest pairs, intervals, coverage, backtest table, model card (Markdown), `evaluation_disclosure` (period, same-case naive baseline per mode and horizon, coverage with n and warnings, `calibrated: false`) |
| `GET /api/v1/models/compare?versions=a,b` | op | Side-by-side (2–5 versions) |
| `GET /api/v1/models/experiments/runs` | op | MLflow candidate runs (paginated) |
| `GET /api/v1/models/promotions` | op | Promotion/rollback history |
| `GET /api/v1/models/drift?recent_months=` | op | PSI per feature used by the production method |
| `POST /api/v1/models/versions/{v}/validate` / `/promote` (`{reason, force?}`) / `POST /models/rollback` (`{reason}`) | op | Gates (method spec, loads, signature, smoke, beats naive, hold-last vs naive, vs production); `force` overrides only metric gates |
| `GET /api/v1/jobs`, `GET /jobs/{id}`, `POST /jobs`, `POST /jobs/{id}/cancel`, `/retry` | op | `{job_type: train \| ingest_benchmarks \| ingest_fx \| ingest_icp \| refresh_forecast \| evaluate_alerts, params, idempotency_key?}` → `202` |
| `GET /api/v1/audit[?action=]` | op | Audit trail |
| `GET /api/gateway/health` | – | Gateway Worker health, `{"status": "ok"}` only (Worker only) |

### Billing (sandbox only, disabled by default)
Every endpoint answers `503 BILLING_DISABLED` while `BILLING_ENABLED=false` (the default), the webhook included.
Design, checks and activation gates: [BILLING.md](BILLING.md).

| Method & path | Auth | Purpose |
|---|---|---|
| `GET /api/v1/billing/status` | user | `{enabled, environment, provider}` |
| `POST /api/v1/billing/orders` | op | `{org_id, amount (integer IDR), currency: "IDR", description, plan_code?}` → `201` order (`created`); the amount is an owner decision, nothing is priced in code |
| `GET /api/v1/billing/orders`, `GET …/orders/{id}` | member(admin; owner/admin) | Own organisation's orders; other organisations' ids `404 BILLING_ORDER_NOT_FOUND` |
| `POST /api/v1/billing/orders/{id}/checkout` | member(admin; owner/admin) | Creates the Midtrans Snap (sandbox) transaction → `pending` with `checkout_url`; idempotent; `409 BILLING_ORDER_STATE`, `502 BILLING_PROVIDER_ERROR` |
| `POST /api/v1/billing/webhooks/midtrans` | – (provider signature) | Midtrans HTTP notification: signature `SHA512(order_id+status_code+gross_amount+ServerKey)` (`401 BILLING_SIGNATURE_INVALID`), known order, environment (`409 BILLING_ENVIRONMENT_MISMATCH`), amount/currency (`409 BILLING_AMOUNT_MISMATCH`), idempotent event log (`200`, `duplicate: true` on replay), state machine (illegal moves recorded as ignored, `200`) |

Ingestion/training jobs end with a post-hook that republishes the system forecast and evaluates
alert rules; results are included in the job result (`system_forecast`, `alerts`).
