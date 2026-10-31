# Threat model

Lightweight STRIDE model of ICP Intelligence. Controls are described in [SECURITY.md](SECURITY.md);
the endpoint-level access matrix is in [API.md](API.md#access-control-policy-v4). Last revised
2026-10-20 after the v6 remediation (items marked **v6** were changed then; evidence in
[REMEDIATION_REPORT_2026-10.md](REMEDIATION_REPORT_2026-10.md)); gateway and static-site rows revised
2026-10-25 (last-known-good public copies, upstream error normalisation, static headers; section
"2026-10-25 gateway changes"); public snapshot read cost, report retries, conflict months and the decree
repair revised 2026-10-26 (section "2026-10-26 platform fixes").

Assumptions: a single backend instance (see [ARCHITECTURE.md](ARCHITECTURE.md#jobs)); no payment
and no e-mail; no formal penetration test has been carried out.

## Assets

| Asset | Why it matters |
|---|---|
| Customer organisation data: members and e-mails, settings, scenarios, alert rules/events, forecasts, reports and files, usage events, plan requests | Confidentiality between tenants; personal data |
| Credentials: session tokens, API keys, operator tokens, gateway shared secret | Full access to a tenant, the operator console or the data plane |
| Official datasets, dataset versions and source evidence | Integrity of every number shown and of the model inputs |
| Model artifacts, registry, promotion history | Integrity of forecasts |
| Audit log | Accountability; must not leak secrets |

## Trust boundaries

- **B1** Internet → Cloudflare Worker gateway (static assets, `/api/v1/*`).
- **B2** Worker → FastAPI backend (shared `X-Gateway-Secret`; the backend refuses `/api/v1` without it
  in deployed environments).
- **B3** Backend → Worker internal data plane `/api/internal/*` (D1 SQL and KV objects, same secret).
- **B4** Backend → outbound sources (ESDM, Bank Indonesia, EIA, World Bank) and MLflow/object store.
- **B5** Tenant ↔ tenant inside one database (`org_id` scoping).
- **B6** Member roles viewer < analyst < admin < owner, and organisation API keys.
- **B7** Customer ↔ platform operator.
- **B8** Generated files (PDF, XLSX, CSV, ZIP) → the user's desktop applications.

## Entry points, threats and controls

| Entry point | Threats (STRIDE) | Controls | Residual risk |
|---|---|---|---|
| Public site and `/public/market-snapshot`, `/plans` | I: forecast values leaking to guests; T: XSS, clickjacking, cross-window attacks; D: spending the D1 daily read allowance through the anonymous snapshot (2026-10-26) | Snapshot returns only a forecast teaser (date, origin, horizon); CSP `script-src 'self'` with only same-origin sources (self-hosted fonts and images), `frame-src`/`worker-src 'none'`, `X-Frame-Options: DENY`, `nosniff`, Referrer-Policy, `Cross-Origin-Opener-Policy`/`Cross-Origin-Resource-Policy: same-origin`, restrictive Permissions-Policy (`apps/web/public/_headers`); React escaping; no `dangerouslySetInnerHTML`; the snapshot is kept in memory for at most 60 s, so repeats read no database rows (section "2026-10-26 platform fixes") | Member session token in `localStorage` is exposed by any XSS (CSP is the mitigation); `style-src 'unsafe-inline'`; no HSTS on static assets until a custom domain exists (SEC-15) |
| Gateway last-known-good copies of the public answers (KV `edge-cache/public/*`, 2026-10-25) | T: cache poisoning (planting a copy, keying on client input, storing a member's view); I: serving data as current when it is not; D: exhausting the KV quota; I: tunnel hostnames in upstream error pages | Fixed keys for two exact paths; only `GET` without `Authorization` and without query string; only `200` JSON objects/arrays ≤ 256 KiB; the data plane refuses the `edge-cache/` prefix; served only if the bytes match the stored SHA-256; timestamp re-serialised before it becomes a header; stale answers carry `X-ICP-Cache: stale`, `X-ICP-Cached-At`, `no-store`; ≤ 1 write per key per 5 min (exact per isolate, best effort across isolates: KV propagation up to ~60 s); ≤ 1 KV read per key per isolate per 5 min, also while no usable copy exists ("no copy" and corrupt copies are remembered); non-JSON 4xx/5xx pages are replaced by the JSON envelope | The page must present a stale copy as stored data of a stated time (UI responsibility); a copy ages without bound while the backend is down; anyone with the Cloudflare account (wrangler) can still write the key with matching metadata |
| `/auth/*` (login, register, password, logout, me) | S: credential stuffing; I: account enumeration; D: brute force, targeted lock-out | scrypt, constant-time verification, same cost for unknown e-mails, 10-char minimum, per-IP auth bucket at gateway and backend, per-address failure throttle, temporary passwords force a change | No MFA. **v6:** failures are counted per (e-mail, IP), so a third party can only pause its own IP; an account-wide ceiling (100 failures from all IPs) remains as a deliberate trade-off; a success clears only its own pair. Counters are in memory, per process (single instance enforced) |
| `/data/*`, `/datasets/*` | I: plan history bypass; T: poisoned imports | `history_years` clipping on every per-month path incl. downloads and provenance report; operator-only versions/imports with preview and `confirm_overwrite`; SHA-256 integrity checks; `private, no-store` on downloads | – |
| `/data/verification` (v5) | I: plan history bypass; misleading status | Member-only, plan-clipped; statuses derived from stored evidence; benchmarks labelled `provider_published`, never "verified" | – |
| `/forecasts`, `/models`, `/insights` | I: cross-tenant reads; E: horizon above plan; D: expensive POST | `org_id` scoping → `404`; `check_horizon` on every forecast path; system forecasts clipped to plan; separate forecast rate bucket; members get model cards without build internals | Insight plan gate exists in code but every plan currently includes insights (tested with a patched plan; the `market_monthly` report now honours it too, **v6**). **v6:** accuracy is always shown with period, n and same-case baseline; intervals never called calibrated |
| Insight engine (v5) | I: raw ingestion errors or operator findings reaching tenants; T: misleading or causal claims | Audience filter (operator-only findings); no raw error text; templates without causal words or jargon (test-enforced); **v6:** evidence contract drops findings without complete evidence | Rule text quality depends on templates; reviewed by tests, not by a domain expert |
| `/scenarios`, `/alerts`, `/reports` | I/T: cross-tenant ids; E: viewer writes; T: report injection; SSRF from the PDF renderer | Org scoping, role checks, feature gates; all dynamic PDF text XML-escaped and ReportLab resource loading disabled; spreadsheet cells starting with `= + - @ TAB CR` prefixed and stored as text; generic `REPORT_FAILED` messages | **v6:** ReportLab loaders accept only in-memory files (local paths, `file:`, UNC and URLs refused); formula neutralisation covers leading whitespace, NBSP and full-width look-alikes; a report from a stored forecast is clipped to the plan horizon. A foreign forecast id is refused with `404` and stores no row (verified live) | Loader guard depends on ReportLab internals |
| `/org/*`, members, API keys, plan requests (v5) | E: role escalation, seat abuse; S: API-key misuse; T: approving one's own plan change | Admin+ only for administration; seat limits; keys hashed, act as analyst, require a plan with API access; plan requests: one pending per org (DB partial unique index), only operators resolve, resolution audited through `set_plan` | API keys have no scopes or expiry beyond revocation. **v6:** monthly API-key quota enforced (`429 QUOTA_EXCEEDED`, quota headers) |
| Operator console, `/admin/*`, `/jobs`, `/audit`, `/system/status`, model promotion | S: stolen operator token; E: customer reaching operator operations; R: unlogged actions | `require_admin`, constant-time comparison, operator token in `sessionStorage`, operators refused on customer-organisation endpoints, audit events for sensitive and denied actions | Static long-lived tokens from the environment. **v6:** operator reads have their own 240/min bucket, so console reloads no longer exhaust the 30/min write budget |
| Gateway internal data plane `/api/internal/*` | E/T: holder of the secret has full D1 SQL and KV access | Constant-time secret check, secret never sent to the browser, client-supplied secret headers replaced; since 2026-10-20 (SEC-10) one statement per request, only SELECT/INSERT/UPDATE/DELETE/WITH plus `PRAGMA table_info(<ident>)`, DDL/PRAGMA/ATTACH/transaction keywords refused with 403 before D1, ≤ 100 bound parameters; rotation via `GATEWAY_ACCEPTED_SECRETS` | One shared secret, used in both directions, still allows any DML on any table (including reading password hashes); per-direction secrets remain a recommendation |
| Backend origin (`/health/*`, `/metrics`) | I: information disclosure; S: bypassing the gateway | Gateway secret required on `/api/v1` when deployed; `METRICS_TOKEN` mandatory in staging/production | **v6:** `/`, `/health/ready` and the Worker health return only `{"status"}` to non-operators |
| Ingestion (ESDM PDFs, BI XML, EIA, World Bank) | T: poisoned source data; D: parser bombs | `defusedxml`, cached never-executed documents, schema/range/completeness validation, reconciliation of two ESDM sources | OCR-only months are flagged, not excluded. **v6:** differing readings are stored as `dissent` and shown, never used to classify a month; `verify-provenance` checks value, label and evidence per month |
| Model loading | T/E: deserialisation | skops with an explicit trusted-type allow-list; no pickle | – |

## v6 changes and the threats they introduce

Status after verification on 2026-10-20 (live HTTP matrix against the dev API plus regression tests):

| Change | New threats | Required controls (acceptance criteria) | Status |
|---|---|---|---|
| Self-service **data export** for organisation owners (ZIP of JSON/CSV) | I: exporting another tenant's data or secrets; D: large exports; T: formula injection in CSV; I: cached files | Owner-only; strictly `org_id`-scoped queries; never include password hashes, token/key hashes or other tenants' rows; formula neutralisation in CSV; size/rate limits; `Content-Disposition: attachment`, `private, no-store`; audited | **Met**: owner-only (admin, analyst, viewer, API key, operator 403; guest 401), 17 files, no hashes or other tenants' rows, hostile names prefixed in CSV, `private, no-store`, rate bucket `forecast` |
| Account/organisation **deletion request** (request + operator-executed soft delete/anonymisation) | E: non-owner requesting deletion; R: unaudited deletion; T: deleting official/historical data; I: residual personal data | Owner-only request, operator-only execution, both audited; sessions and API keys revoked on execution; personal fields anonymised; official datasets and evidence untouched; idempotent; no hard deletes | **Met** on throwaway orgs: role rules, `409` duplicates/`OWNER_ACCOUNT`, cross-tenant `404`, double-resolve `409`, sessions revoked, sign-in refused, e-mail and names gone, official data intact |
| **API quota** enforcement | D: quota exhaustion by a tenant's own keys; bypass via sessions | Counted from `usage_events` for API keys only; `429 QUOTA_EXCEEDED` with reset time; sessions not quota-limited (by decision) | **Met**: quota headers live; exhaustion and `429` verified by `test_api_key_monthly_quota` (the 20,000/month professional quota was not exhausted live) |
| **Throttle redesign** (sign-in) | D: targeted lock-out; bypass via IP rotation | Throttle must not let a third party block a known address indefinitely; single-instance assumption documented or enforced in config validation | **Met** after a Q-found fix (success elsewhere no longer resets an attacker's counters); forged `CF-Connecting-IP` without the gateway secret is ignored; `API_INSTANCES=1` enforced. v7 (SEC-01): behind the Worker the client IP now travels in `X-ICP-Client-IP` because Cloudflare rewrites `CF-Connecting-IP` to the Worker egress on cross-zone subrequests; unit-tested, hosted confirmation pending |
| **Audit-log retention** CLI | R: loss of evidence; T: pruning other tables | Default keep everything; dry-run first; prunes only `audit_events` older than the configured period; never automatic | **Met** (`test_audit_prune_is_dry_run_by_default_and_never_automatic`; not run on the shared store) |

## Security acceptance tests

Result of the 2026-10-20 verification in brackets.


1. `npm run check` exits 0 (lint, types, contract, all tests, build, secret scan). [**Pass**]
2. `pip-audit -r requirements/constraints.txt` clean; `npm audit --omit=dev` 0; bandit
   `-ll --ignore-nosec` shows no findings, and the code contains no `# nosec` (suppressions are not
   accepted; see [SECURITY.md](SECURITY.md#dependency-and-configuration-checks)). [**Pass** (2026-10-20, after
   wave A of the v7 audit): 0 / 0 / no issues, 0 `# nosec`. Before that date bandit passed only because of 13
   `# nosec` markers, which `--ignore-nosec` reported as 13 Medium findings. 2026-10-25: pip-audit found 39
   advisories published after that run (pillow, mlflow, certifi); after the wave B pin bump pip-audit is clean
   under Python 3.11 (the CI and image interpreter), bandit and npm audit clean; a Python 3.10 dev venv keeps
   Pillow 12.3.0 (accepted, RESIDUAL_RISKS DEP-01).]
3. HTTP matrix: every operation as guest → `401` with `WWW-Authenticate` except the public set;
   cross-tenant ids → `404`; role matrix unchanged; no stack traces in 4xx/5xx; downloads and
   exports `private, no-store`. [**Pass**: 80/80 guest operations, cross-tenant 404s, reports and exports `private, no-store`]
4. Report injection: markup and formula payloads in scenario names cause no outbound request, render
   as literal text, and spreadsheet cells are stored as text. [**Pass**: 0 listener hits, no local image embedded, 0 formula cells in PDF/XLSX/CSV]
5. New endpoints (export, deletion request, quota) appear in OpenAPI, are covered by access-control
   tests and pass guest-401 / cross-tenant-404. [**Pass**]
6. No secrets in the web bundle (`icp_sess_`, `icp_live_`, gateway secret). [**Pass**: no token-like strings or the gateway secret in `apps/web/dist`; no `dangerouslySetInnerHTML`, `innerHTML` or `eval` in `apps/web/src`]

## 2026-10-25 gateway changes

| Change | New threats | Controls (acceptance) | Status |
|---|---|---|---|
| Last-known-good copies of `GET /public/market-snapshot` and `GET /plans` in KV (contract C2) | Cache poisoning; a member's or operator's view stored and shown to guests; stale data presented as current; KV quota exhaustion | Unit tests with fake KV/fetch (`apps/gateway/test/edge-cache.test.ts`): live 200 stored; unchanged content not rewritten until the stored copy is a day old, then refreshed once (the snapshot's `generated_at` is ignored by the content hash; integrity still rests on the byte SHA-256; wave B); ≤ 1 write per 5 min per key; a failed write (quota) not retried for 5 min and at most one write check per key in flight; 530 HTML → stale with headers when cached, JSON envelope when not; timeout → stale; any `Authorization` header → no KV read or write; other paths, query strings, HEAD/POST never cached; no KV binding → inert; tampered bytes or malformed metadata never served; failing requests with no usable copy cost one KV read per key per interval, not one per request (review finding GWR-02); data plane refuses `edge-cache/`. Local end-to-end with `wrangler dev` (miniflare KV): live → stored; tunnel-style 530 and an 8 s hang → stale, byte-identical | **Met** locally; not deployed |
| Non-JSON upstream error answers (4xx and 5xx) normalised to `UPSTREAM_TIMEOUT_OR_UNAVAILABLE` | Information disclosure through Cloudflare/tunnel/Render error pages; HTML in an API client | HTML discarded, envelope with request id, upstream status only in the Worker log; JSON errors and successful answers (downloads) unchanged. 4xx pages were passed through until the review (GWR-03) | **Met** (unit tests; 5xx also local e2e) |
| Worker entry point exports only the handler (`src/worker.ts`) | Availability: workerd refused to start the v7 main module (`Incorrect type for map entry 'CLIENT_IP_HEADER'`) | `test/entrypoint.test.ts`: `src/worker.ts` exports only `default`, and `wrangler.jsonc` `main` (top level and every environment) names that file (review finding GWR-04); local `wrangler dev` starts and serves | **Met** locally |
| Config hardening: https-only `BACKEND_URL` in staging/production, no URL credentials, strict numeric variables, one deadline for upstream retries | Secret sent in clear; limits silently disabled by a typo; retries outliving the Cloudflare 100 s edge limit | Unit tests in `edge-cache.test.ts` ("configuration hardening") | **Met** |
| Static headers: COOP, CORP, `frame-src`/`worker-src 'none'`, Permissions-Policy, immutable `/fonts/*` | Breaking the site (blocked fonts, scripts, images) | Local `wrangler dev` + headless Chrome on the baseline build: no CSP/Permissions-Policy console errors, font loads, inline script blocked | **Met** for the baseline build; re-check with the new public site build |

## 2026-10-26 platform fixes

Findings of the wave C security review (SEC-C-01, -03, -04, -07); evidence in the backend tests named below.

| Change | Threat addressed | Controls (acceptance) | Status |
|---|---|---|---|
| Anonymous `GET /public/market-snapshot` kept in memory per backend process (SEC-C-01) | D: one client exhausting the D1 free plan's 5M rows read per day, which stops every tenant's workspace until the UTC day ends. Since contract C1 the answer reads every ICP evidence row: 3 statements and about 280 rows read per call on the local store (up to about 420 when many non-system forecasts are newer than the latest system one), so roughly 12k-18k calls, about 40-60 minutes for one IP at the 300/min limits. Through the gateway data plane each statement was also one Worker request | The answer is computed at most once per 60 s, and again at once after a dataset activation, an evidence write or a system forecast in the process or after any job attempt (jobs write from a worker process); a repeat runs no SQL (`test_guest_snapshot_is_memoised_and_a_repeat_reads_nothing`, `test_guest_snapshot_memo_follows_every_write_it_depends_on`, `test_snapshot_memo_expires_and_computes_concurrent_misses_once`). D1 cost of this endpoint is bounded by about 1,440 computations a day (≤ about 0.6M rows) whatever the request volume; a cached answer costs one Worker request (the caller's) | **Met** (unit tests; statement counts measured on a copy of the local store, not on D1) |
| `Idempotency-Key` on `POST /reports` reserved while the first request runs (SEC-C-03) | T/R: a retry after the backend's `504` (generation continues in its worker thread) generating and storing a second report with the same key (duplicate files and usage event) | In-process reservation per (organisation, caller, key): a repeat during the first attempt gets `409 IDEMPOTENCY_IN_PROGRESS` with `Retry-After`; released when the attempt ends, also on failure (`test_report_retry_while_the_first_attempt_runs_is_refused_not_duplicated`, `test_report_reservation_is_per_caller_and_key_and_always_released`) | **Met** for one backend process (`API_INSTANCES=1`); a second process would need a database reservation |
| Conflict months withheld from the public headline too (SEC-C-04) | I/T: guests shown, as the headline ICP, a value whose official documents disagree while the chart leaves the month out | `icp_usd`, `mom_change_pct`, `icp_idr` (latest) and `previous_*` (previous month) are `null` for a `conflict` month; `icp_evidence` names the basis and lists no document, since an announcement link can name the value (`test_guest_snapshot_withholds_a_conflict_month_in_the_headline_too`, `test_evidence_of_a_conflict_month_lists_no_document`) | **Met**; no conflict month in the current data |
| `repair-decree-references --apply` takes and verifies a backup and always audits (SEC-C-07) | T: evidence and state files changed without a restorable copy; R: a failure part-way leaving the database change unaudited, after which a re-run audits `evidence_rows: 0` | Local store: `icp-api backup` plus copies of the two state files, verified with `restore-check` (and the evidence row count) before anything is written; deployed store: refused unless `--backup-taken`; the audit row is written in `finally` with the counts actually applied (`failure` after a partial run) (`test_repair_cli_apply_takes_and_verifies_a_backup_first`, `test_repair_cli_refuses_on_a_deployed_store_without_a_confirmed_backup`, `test_repair_writes_nothing_when_the_backup_fails`, `test_repair_audits_what_was_applied_when_the_state_write_fails`) | **Met** |
