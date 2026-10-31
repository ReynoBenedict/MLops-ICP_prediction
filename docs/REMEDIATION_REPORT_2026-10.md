# Remediation report v6 (October 2026)

**Status: implemented and independently verified locally on 2026-10-20.** Nothing was committed, pushed or
deployed; staging still runs the 2026-10-17 build (see [DEPLOYMENT.md](DEPLOYMENT.md#pending-for-the-next-deployment-changes-made-locally-on-2026-10-1920)).
Implementation by three agents (security/operations backend "S", data/forecasting/insights "D",
frontend "F"); verification and documentation by an independent agent ("Q") that re-ran every check below
instead of relying on the implementers' notes. Severity scale: High, Medium, Low (P3).

Contents: [1 Baseline](#1-baseline-before-remediation-2026-10-20) · [2 Findings and fixes](#2-findings-and-fixes) ·
[3 Independent verification](#3-independent-verification) · [4 Remaining risks](#4-remaining-risks-and-open-decisions) ·
[5 Data created](#5-data-created-during-verification)

## 1. Baseline before remediation (2026-10-20)

Measured on HEAD `09d672c` plus the uncommitted v5 changes, while the v6 work was starting.

| Step | Result |
|---|---|
| lint | pass (ruff 97 files formatted; ESLint clean) |
| typecheck | pass (mypy 76 files) |
| contracts:check | up to date: 70 paths, 81 operations |
| test | **fail**: pytest 223 passed; web Vitest 93/94 (`insights.test.tsx` "deep links straight into an insight…" expected `/insight`, got `/insight?insight=ins_spread`); gateway 21 passed |
| build, secret scan | pass |
| pip-audit (constraints) / bandit `-ll` / `npm audit --omit=dev` | clean / 11 Medium (reviewed false positives) / 0 |

## 2. Findings and fixes

Each entry: severity · component · original behaviour → root cause → fix · code · regression tests (all
re-run by Q, §3.2) · remaining risk.

### Security and operations (S)

**S-1 PDF renderer could read local files and reach UNC shares (High).** `services/reports.py` (ReportLab).
Original: `harden_reportlab()` only emptied `rl_config.trustedSchemes`; ReportLab's `open_for_read` tries a
plain `open()` first, so raw `<img src>` markup with an absolute, POSIX-style or relative path was read and
embedded, and `file:///…`, `//host/x` and UNC paths reached `open()` (http/https/data were already refused).
The primary layer (escaping of every user string) was intact, so this was reachable only if escaping failed.
Fix: `_in_memory_only` replaces `open_for_read` in `reportlab.lib.utils` and `reportlab.pdfbase.pdfdoc`,
re-applied at every PDF build. Tests: `test_security_hardening.py::test_pdf_markup_cannot_read_files_or_urls`
(9 vectors; 6 failed before), `::test_pdf_links_and_fonts_do_not_fetch`, `::test_reports_own_chart_image_still_renders`.
Remaining: depends on ReportLab internals.

**S-2 Spreadsheet formula injection bypasses (Medium).** `cell_safe` (CSV, XLSX). Original: only a value
starting exactly with `= + - @ TAB CR` was neutralised; `" =1+2"`, `"\n=1+2"`, NBSP, full-width `＝ ＋ － ＠` and
U+2212 passed. Fix: leading TAB/CR/LF always prefixed; otherwise the first non-whitespace character is
NFKC-folded and checked; plain numbers unchanged; XLSX cells typed as strings. Tests:
`::test_cell_safe_adversarial` (18 cases; 10 failed before), `::test_csv_and_xlsx_outputs_are_inert`.

**S-3 Sign-in throttle allowed trivial lock-out (Medium).** `security.FailureThrottle`, `routers/accounts.py`.
Original: 5 failures for an e-mail from any address paused that e-mail everywhere for 15 minutes, including
the owner with the right password. Fix: counted per (SHA-256 of e-mail, client IP), limit 5; account-wide
ceiling `LOGIN_ACCOUNT_FAILURES_MAX` = 100; `429 RATE_LIMITED` with `details.scope` and `Retry-After`; client IP
trusted from `CF-Connecting-IP` only with a verified gateway secret. Tests: `test_security_api.py`
`::test_attacker_ip_is_throttled_but_owner_from_another_ip_still_signs_in`,
`::test_distributed_guessing_hits_the_account_ceiling`, `::test_unknown_email_throttles_like_a_known_one`,
`::test_success_clears_the_pair_counter`; `test_auth_abuse.py` (7).

**Q-1 A successful sign-in reset every attacker's counters (Low, found by Q, fixed by the coordinator).**
`security.FailureThrottle.reset`. Original (after S-3): any successful sign-in popped the account ceiling and
every (e-mail, IP) pair, so each owner sign-in gave an attacker 5 fresh guesses and defeated the ceiling.
Repro: `ux6\q\throttle6.mjs` on a throwaway org — after 5 failures from 127.0.0.1 and an owner sign-in from a
gateway-verified `198.51.100.23`, the right password from 127.0.0.1 was accepted (200). Fix: `reset(account,
client)` clears only the succeeding pair; the login route passes the client. Test (red first):
`test_security_api.py::test_owner_sign_in_elsewhere_does_not_reset_the_attackers_counters`. Re-run after the
fix: 10/10 steps as designed, the attacker's IP stays at `429 scope=client` (§3.3).

**S-4 Public health endpoints disclosed environment, version and dependency names (Low).**
`routers/health.py`, Worker. Fix: `/`, `/health/ready` (non-operators) and `/api/gateway/health` return only
`{"status"}`; operators keep detail; `SystemStatus` adds `request_id`. Test: `::test_public_health_reveals_only_status`, gateway health test.

**S-5 Missing security headers; 500s without request id (Low/Medium).** `middleware.py`, Worker. Original:
no `X-Frame-Options`, CSP or CORP on API responses, no HSTS; unhandled exceptions answered with
`request_id: null`, no `X-Request-ID` and no headers. Fix: `X-Frame-Options: DENY`, CSP `default-src 'none';
frame-ancestors 'none'`, CORP `same-origin`, HSTS when deployed, also on early replies; `RequestContextMiddleware`
answers unhandled errors with the real request id. Tests: `::test_api_responses_carry_security_headers`, `::test_unhandled_errors_and_validation_errors_do_not_leak`.

**S-6 Operator console 429 after ~8 reloads (Low; open since ux4).** `security.require_admin`. Fix: reads use a
240/min `admin_read` bucket; writes keep 30/min. Side fix: a status change for an unknown organisation wrote a
"success" audit row before answering 404. Tests: `::test_operator_console_reads_do_not_exhaust_the_write_budget`,
`::test_operator_status_change_on_unknown_org_is_404_and_not_audited_as_success`.

**S-7 to S-9 verified without code change.** Session lifecycle (30-day absolute expiry, password change revokes
all sessions, disable/suspend/revoke immediate) `::test_session_lifecycle`; every route needs authentication
except the 9 public ones and customers get 403 on every operator route
`::test_every_route_requires_authentication_unless_public`, `::test_customers_get_403_on_every_operator_route`;
cross-tenant ids and `org_id` parameters `::test_cross_tenant_ids_on_member_key_plan_and_alert_routes`,
`::test_org_id_parameters_are_ignored`; logs and audit hold no credentials or full e-mails
`::test_logs_and_audit_hold_no_credentials_or_full_emails`.

**S-10 API-key monthly quota not enforced (Medium).** `security.enforce_api_quota`, `storage/product.count_usage`.
Original: `/plans` advertised `api_requests_per_month` but nothing checked it. Fix: count `api.request` units
since the start of the UTC month; at the limit `429 QUOTA_EXCEEDED` with `Retry-After` and
`{limit, used, period, reset_at, plan}`; quota headers on success; sessions and operators exempt; refused
requests not recorded. Test: `test_operations.py::test_api_key_monthly_quota`.

**S-11 Every plan field enforced server-side.** `::test_every_plan_field_has_an_enforcement_check`,
`::test_plan_field_is_enforced_server_side` (12 fields).

**S-12 Organisation data export (new).** `services/privacy.py`, `GET /org/export`, owner session only, audited,
in-memory ZIP (17 files), `private, no-store`, no hashes, CSV via `cell_safe`. Test: `::test_owner_exports_only_their_own_org_data`.

**S-13 Deletion requests (new, migration 0006).** Request/withdraw/list for members, execute/decline for
operators, anonymisation in place, everything audited. Tests: `::test_account_deletion_request_flow`,
`::test_organization_deletion_request_flow_keeps_official_data`, `::test_deletion_request_withdraw_and_decline`,
`::test_migrations_apply_in_order_to_an_empty_database_and_are_rerun_safe`, `::test_migration_0006_preserves_existing_rows`.

**S-14 Audit retention CLI (new).** `icp-api audit-prune` (dry run by default, `AUDIT_RETENTION_DAYS=0` keeps
everything, never scheduled). Test: `::test_audit_prune_is_dry_run_by_default_and_never_automatic`.

**S-15 Single instance enforced (Low).** `icp-api serve` honoured a platform `WEB_CONCURRENCY` and could fork
workers with separate counters. Fix: always one worker; `API_INSTANCES` must be 1. D1-backed counters rejected
(one write per request on the free tier). Tests: `::test_serve_always_runs_one_worker_process`, `::test_single_instance_is_enforced_by_configuration`.

**S-16/S-17 Reproducibility and supply chain.** Docker base pinned by digest; new CI `security` job (pinned
pip-audit/bandit, npm audit, secret scan); 13 bandit findings marked `# nosec` after review (Q re-reviewed each,
§3.2).

**S-18 `market_monthly` report embedded insights regardless of plan (Low, latent; found by D).**
Fix: the report includes insights only when the caller's plan does. Test: `::test_market_report_embeds_insights_only_when_the_plan_includes_them`.

### Data, forecasting and insights (D)

**D-01 Look-ahead leakage in the hold-last backtest and live path (Medium).** `pipeline/icp_ml/backtest.py`.
FX was never overwritten after the origin (so `ridge_full` saw realised FX), and months between non-contiguous
horizons kept realised benchmark lags. Fix: every month after the origin is filled scenario → observed
(observed mode) → held, FX included, with recursive intermediate predictions. Effect: production
`spread_brent_6m` and `naive_last` unchanged; `ridge_full` hold-last MAE h1–h6 6.25/9.35/10.76/12.02/12.63/13.51 →
6.21/9.22/10.62/11.87/12.31/13.15; winner unchanged; no retraining. Tests: `pipeline/tests/test_backtest_leakage.py`
(17; 12 failed before). Details: [FORECASTING_METHODOLOGY.md](FORECASTING_METHODOLOGY.md#4-evaluation-pipelineicp_mlbacktestpy).

**D-02 Accuracy shown without period, baseline or under-coverage warning (High, honesty).** Fix: per point,
model card, report and CSV export now state period, n, horizon, mode, the naive baseline on the same cases,
`beats_baseline`, coverage window, `under_coverage` and `coverage_warning`; intervals described as empirical, not
calibrated. Tests: `test_d_evaluation.py` (9).

**D-03 Ignored scenario inputs not flagged on all paths (Medium).** `input_usage[].effect`, `ignored_inputs`,
compare and report notes. Tests: `test_d_inputs.py::test_post_forecast_flags_ignored_inputs`, `::test_scenario_compare_flags_ignored_inputs`.

**D-04 Report from a stored forecast bypassed the plan horizon and kept stale labels (High).** A trial
organisation received all 4–6 stored steps; published months still read "awaiting publication". Fix: the report
uses the same `explain_run` as the API (clip, `kind_now`, official value column); `forecast_revision` no longer
fires for published months. Tests: `::test_forecast_report_from_a_stored_run_is_clipped_and_uses_official_values`,
`test_d_plan_matrix.py` (17).

**D-05 Dissenting source readings dropped or mislabelled (Medium).** Fix: `dissent` status, shown in
`/data/verification`, `/data/evidence` and the provenance report; `backfill-dissent` and `verify-provenance`
CLIs. Run on `var/` by the coordinator after a backup: 4 readings inserted; verify: 93 months, 0 disagreements,
0 gaps. Tests: `test_d_provenance.py` (14). Details: [DATA_RECONCILIATION.md](DATA_RECONCILIATION.md).

**D-06 Insights without complete evidence could be published (Medium).** Evidence contract
(`insights/validate.py`), engine `2026-10.2`, five rules completed, wording without accuracy headlines. Tests:
`test_d_insight_quality.py` (validator, drop-and-log, thresholds, 300 seeded contexts, 322-text overclaim sweep).
Details: [INSIGHT_ENGINE.md](INSIGHT_ENGINE.md).

**D-07 Plan limits matrix** for all four plans (17 tests). **D-08 Overclaiming texts** ("calibrated",
"Teramati (resmi)", "dari data terverifikasi") removed in backend and, by F, in the UI and legal text.
**D-09** remaining risk only (FX scenario stays rupiah-only).

### Frontend, UX and accessibility (F)

| Id | Severity | Finding → fix | Evidence |
|---|---|---|---|
| F-1 | Low | v5 browser failures: 10 of 14 were checker false positives (wrong regex, invalid Back test, Chrome's own 404 log); duplicate React key in the insight dialog (fixed, also in the lead strip); operator 429 (fixed with `sharedGet` + S-6) | `ux6\pw\j-insight-v6.mjs`, `j-misc-v6.mjs` |
| F-2 | Low | Flaky Escape in modal under load (the Q baseline failure): key handler moved to `useLayoutEffect` | `components/modal.tsx`; web suite 102/102 |
| F-3 | Medium | `/operasi/pekerjaan` overflowed by 15 px at 320 px (`minmax(300px,1fr)`) | `evidence/2026-10-20-v6/06-ops-jobs-no-overflow-320.png` |
| F-4 | Low | Touch targets < 24 px (language toggle, footer/legal/auth links) → ≥ 28 px on coarse pointers | `…/10-mobile-touch-targets-footer-390.png` |
| F-5 | Low | Operator strings (22 kB) shipped to every customer → lazy ops chunk; ~5 kB gzip less per customer page | `ux6\pw\closure.cjs` |
| F-6 | – | Renames: `api/v5.ts` → `api/product-types.ts`, `v5.test.tsx` → `ux-regressions.test.tsx`, `styles-v4/v5.css` → `styles-layout.css`; ~30 dead CSS classes removed; static pages pixel-identical | `ux6\pw\pixdiff.mjs` |
| F-7 | – | UI for D-02/D-03/D-05 (accuracy disclosure, coverage warning, ignored inputs, dissent label, model-card disclosure) | `…/01-…`, `02-…`, `03-…`, `07-…`, `11-…` |
| F-8 | – | UI for S features: Data & privasi panel (export, deletion request with confirmation), operator deletion queue, throttle-scope and quota messages; privacy text updated | `…/04-…`, `05-…`, `08-…`, `09-…` |

## 3. Independent verification

All commands run by Q from the repository root on 2026-10-20 against the final code; logs in
`%TEMP%\claude\ux6\q\p2\`.

### 3.1 Quality gates

`(time npm run check)` → **EXIT 0, 6 min 21 s**:

| Step | Result |
|---|---|
| ruff check / format | `All checks passed!` / `109 files already formatted` |
| mypy | `Success: no issues found in 79 source files` |
| contracts:check | `API contract is up to date.` (75 paths, 87 operations; +5 paths/+6 operations: export, deletion requests ×3 paths, admin deletion requests ×2) |
| pytest | **1,001 passed**, 1 warning (Starlette/httpx deprecation), 313 s |
| web Vitest | 102 passed / 12 files |
| gateway Vitest | 21 passed / 2 files |
| build | Vite OK; Wrangler dry run 13.02 KiB / gzip 4.05 KiB |
| secret scan | `Secret scan: no findings.` |

**Why 1,001 Python tests (from 223).** `pytest --collect-only`: 230 distinct test functions; 797 of the 1,001
items are parametrised cases. Two parametrised tests account for 622 items:
`test_api_texts_do_not_overclaim` (322 items, one per API-provided text in two languages) and
`test_randomized_series_never_break_the_contract` (300 items, one per seed of a deterministic random insight
context). Both assert real properties (no causal/overclaiming words; complete evidence, no NaN, unique ids, no
contradictions) and failed before the D-06 fix, so they are not padding, but they inflate the count: compare
distinct functions, not items. Largest files: `test_d_insight_quality.py` 649 items,
`test_access_control.py` 66, `test_security_hardening.py` 30.

`git diff --check` → exit 0 (only CRLF conversion warnings).

### 3.2 Scans and targeted regressions

| Command | Result |
|---|---|
| `pip-audit -r requirements/constraints.txt --no-deps --disable-pip` | `No known vulnerabilities found` |
| `bandit -r backend/icp_api pipeline/icp_ml -ll` | `No issues identified` (13 lines carry `# nosec`) |
| `bandit … -ll --ignore-nosec` | the 13 suppressed findings: 12 × B608 (`cli.py:243,252,264` table names from the local `sqlite_master`; `storage/product.py:48,94,227,280` column whitelists; `:54` constant fragment; `:260` clause from constants; `:605` export table/column map with a `KeyError` for anything else; `:628` placeholder list only; `storage/repository.py:99` table name from internal constants, all 16 `_page` callers pass literals) and 1 × B104 (`settings.py:244`, deployed hosts only). Each reviewed: every user value is a bound parameter; **no real issue is silenced** |
| `npm audit --omit=dev` | `found 0 vulnerabilities` |
| Bundle scan | no `icp_sess_…`/`icp_live_…` tokens or the gateway secret in `apps/web/dist`; no `dangerouslySetInnerHTML`, `innerHTML` or `eval` in `apps/web/src` |
| Targeted pytest (security, operations, report safety, auth abuse, D inputs/provenance/evaluation/plan matrix, backtest leakage, validator) | **164 passed** in 160 s |
| `pytest …::test_owner_sign_in_elsewhere_does_not_reset_the_attackers_counters` (Q-1) | 1 passed |

### 3.3 Adversarial HTTP matrix (live dev API)

`node ux6\q\adv6.mjs http://localhost:5173 http://127.0.0.1:8000` → **155/161 PASS**; the 6 failures were
analysed and are not product defects:

| Group | Pass | Notes |
|---|---|---|
| guest: every contract operation without credentials | 80/80 | `401 AUTH_REQUIRED` + `WWW-Authenticate` except the public set |
| health | 3/3 | `/health/ready` and `/` return only `{"status"}` to guests; operator gets checks |
| headers | 0/1 | **checker false positive**: required `Permissions-Policy`, which the final design does not include; XFO, CSP, CORP, nosniff present |
| export | 7/8 | owner 200 ZIP, attachment, `private, no-store`; admin/analyst/viewer/API key/operator 403, guest 401; 17 files, no hashes, no other-tenant data, hostile names (`=…`, `" =1+2"`, NBSP `@`, `＝`, `−`, TAB) all prefixed with `'`. The one fail is a **checker false positive**: it flagged the 12-character display prefix `icp_live_…` in `api_keys.json` (the full key is absent) |
| deletion | 23/25 | full flow on throwaway orgs (§5): role rules, `409`/`OWNER_ACCOUNT`, cross-tenant 404, operator execute, double-resolve 409, sessions revoked, e-mail removed from member list, organisation anonymised, official data intact. 2 **checker false positives**: a failed *login* returns 401 without `WWW-Authenticate` (correct for a credential form) |
| quota | 2/2 | API key responses carry `X-Quota-Limit/Remaining/Reset`; session responses do not. `429 QUOTA_EXCEEDED` itself verified by `test_api_key_monthly_quota` only (exhausting 20,000 requests live was not attempted) |
| tenant | 6/6 | scenarios, forecasts, forecast export 404 across tenants; a report with a foreign `forecast_id` → 404 and **no failed row stored** (v5 P3 #10 closed) |
| reports | 21/21 | 5 scenario-comparison reports (PDF, XLSX, CSV) with markup/URL/file/UNC/formula names, `private, no-store`, **0 hits** on the 127.0.0.1 listener |
| throttle | 2/4 | 2 **ordering artefacts** of the matrix (a successful owner sign-in reset the counters before the next checks) — this led to Q-1; re-run with `throttle6.mjs` after the fix: **10/10** (5×401, 6th `429 scope=client` `Retry-After: 895`, right password same IP 429, forged `CF-Connecting-IP` without gateway secret 429, gateway-verified other IP 200, attacker IP still 429) |
| operator | 1/1 | 40 `GET /system/status` in a burst → 0 × 429 |

Offline inspection of a second set of 4 reports with 12 hostile names (`reportcheck.mjs` + `inspect_reports.py`,
throwaway org Z on a professional plan): every PDF has the same 2 images (brand and chart), so no local file
was embedded; 0 formula cells or formula-like strings in XLSX (no `data_type='f'`) and CSV; listener 0 hits.
`500` responses with a request id were verified by test only (no live 500 could be provoked safely).

### 3.4 Browser journeys

playwright-core with system Chrome (headless) on the dev stack (Vite :5173, API :8000), test account A and the
local operator token. Agent F's scripts were re-run unchanged by Q; Q added its own spot checks.

| Script (widths) | Result | Scope |
|---|---|---|
| `ux6\pw\j-responsive.mjs` (320/390/768/1024/1440) | 566/567 | 7 guest, 9 customer and 8 operator pages: no page scroll or element outside the viewport, no nested scrollers, no clipped text, tap targets, one h1, console errors. The one failure (`/operasi/audit` at 390: 0 h1) did not reproduce: Q's re-check after a longer settle found exactly one h1 at all five widths (a lazy chunk still loading when the probe ran) |
| `ux6\pw\j-a11y-nav.mjs` | 29/29 | guest redirects with `next=`, customer/operator boundaries (ops modules never requested by customers), SPA Back/Forward and titles, mobile menu Escape and focus return, deletion confirmation dialog (focus, inert background, Escape, nothing posted), reduced motion, offline errors with retry |
| `ux6\pw\j-insight-v6.mjs` (5 widths) | 126/126 | insight feed and dialog: keyboard, focus trap and return, evidence sections, no duplicate keys or console errors |
| `ux6\pw\j-misc-v6.mjs` | 32/32 | leave guard, reports journey, operator console without 429, language toggle |
| `ux6\q\qshots.mjs` (Q; 5 widths) | 56/56 | guest `/akun#data` → `/masuk?next=/akun…`; Data & privasi panel present; no horizontal scroll on `/akun`, `/proyeksi`, `/insight`, `/laporan`; EN toggle shows no raw i18n keys; operator audit page has one h1; no console errors; README captures |

### 3.5 Documentation

`node ux6\q\linkcheck.mjs .` (relative links, image sources and `#anchors` in `README.md` and `docs/**`; it was
checked to catch a broken link and a broken anchor) → **28 files, 165 relative links, 0 broken**. `docs/` went
from 80 files / 11,490,228 bytes to **53 files / 3,762,710 bytes**; every merge, archive and deletion is listed in
[DOCUMENTATION_INVENTORY.md](DOCUMENTATION_INVENTORY.md). `docs.zip` at the repository root (a copy of the old
tree) was left untouched for the owner.

## 4. Remaining risks and open decisions

- Counters (rate limits, sign-in) are in memory: single instance only; a restart clears them. An attacker with
  ≥ 20 IPs can still trigger the 15-minute account-wide pause (deliberate ceiling).
- ReportLab loader guard relies on library internals; regression tests cover the known vectors.
- API keys have no scopes or expiry; they survive the deactivation of their creator.
- No owner transfer; no grace period for deletion requests; retention periods and response times are owner
  decisions ([LEGAL_AND_PRIVACY.md](LEGAL_AND_PRIVACY.md)).
- Coverage samples per horizon are small (≈ 27 cases); benchmark and FX values are not vintages.
- The new CI `security` job has never run on GitHub; staging lacks all v6 changes and migrations 0005/0006.
- The dev venv holds Jupyter/Streamlit packages with 45 advisories (not shipped).

## 5. Data created during verification

Throwaway organisations registered through the public API on the local dev stack (names end in "(uji lokal)",
credentials only in `ux6\q\adv6-throwaway.json`): "PT Uji Q6 X …" (professional plan set by the operator; admin,
analyst, viewer invited; the viewer's account deleted through a request), "PT Uji Q6 Y …" (organisation deletion
executed: anonymised and suspended), "PT Uji Q6 Z …" (professional plan for the report check; scenarios,
reports, one forecast). All sessions were logged out except those revoked by deletion. In-memory sign-in
counters for X's and Z's e-mails were triggered on purpose. Agent F's browser re-runs use test account A
(reports and withdrawn plan requests). No existing organisation was modified or deleted.
