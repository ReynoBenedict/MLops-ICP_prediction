# Full risk register (2026-10-20)

Status: CURRENT. Compiled 2026-10-20 from the four read-only audits of this mission. The remediation waves update
the statuses; see [REMEDIATION_LOG.md](REMEDIATION_LOG.md). Priorities follow
[PRODUCTION_READINESS_PLAN.md](PRODUCTION_READINESS_PLAN.md). Questions only the owner can answer are in
[OWNER_DECISIONS_REQUIRED.md](OWNER_DECISIONS_REQUIRED.md).

**Sources:** these are session working files outside the repository:
- audit 1: security and identity
- audit 2: backend, data and ML
- audit 3: frontend, product and accessibility
- audit 4: QA, DevSecOps and commercial

**Scan versions:** pip-audit 2.10.1, bandit 1.9.4, npm 10.9.2 (`npm audit`), Node 22.14.0, coverage 7.16.2.

**ID renames (to remove collisions between audits):**
- Audit 1 `OPS-01` (D1 write amplification) is now **OPS-07**.
- Audit 2 `CI-01` (hand-written web types) is now **CI-07**.
- Audit 3 `COPY-01` and audit 4 `LEGAL-01` describe the same gap. They are kept as one entry (LEGAL-01, alias COPY-01).

**Owners:**
- *Security*: the wave A/B agent.
- *Backend*: the wave C/E agent.
- *Frontend*: the wave D agent.
- *QA*: the CI & DevSecOps agent.
- *Owner*: the product owner (business, legal, hosting, money).

**Statuses:**
- Open
- In progress
- Fixed – verified by QA (reproduced independently in wave F) · Fixed – claimed (implementer tests; covered by the green full suite, not independently probed)
- Verified
- Accepted (residual; see [RESIDUAL_RISKS.md](RESIDUAL_RISKS.md))
- Owner decision

**Totals:** **0 P0**, 8 P1 (5 code/ops, 3 owner decisions), 27 P2 as compiled, and the rest P3. Some P3 rows group
several items. Items raised during remediation are added at P3: DATA-06 and OPS-08.

## Summary table

| ID | Title | Sev | Type | Owner | Status |
|---|---|---|---|---|---|
| SEC-01 | Client IP collapses behind the Worker → per-IP throttles become global | P1 | likely vulnerability | Security | Fixed – claimed (implementer tests; full suite green in wave F); hosted confirmation pending |
| CI-01 | CI test data: a clean checkout lacks `data/raw/wti.csv` → suite fails/skips | P1 | confirmed defect | QA | Fixed – verified by QA (0 skips; remote CI not run) |
| CI-02 | Security job passes only because of 13 `# nosec` | P1 | policy / false green | QA + Security | Fixed – verified by QA (bandit --ignore-nosec clean, 0 nosec) |
| CI-04 | Scheduled CT workflow runs `train` against bundle-mode staging | P1 | operational | QA | Fixed – verified by QA (YAML review; not triggered) |
| OPS-01 | Restore never rehearsed; backups unverified, same disk | P1 | operational | Backend + Owner | Fixed – verified by QA (local restore rehearsed; D1/KV/MLflow restore open) |
| OPS-06 | Hosting: laptop + Quick Tunnel, staging outdated, migrations 0005/0006 not on D1 | P1 | operational / owner | Owner | Owner decision |
| BILL-01 | Billing not implemented | P1 | business decision | Owner (+Backend sandbox) | Owner decision; sandbox adapter implemented and disabled (BILLING.md) |
| LEGAL-01 (COPY-01) | Terms/Privacy drafts with 14 owner placeholders | P1 | business-legal | Owner | Owner decision |
| SEC-02 | Edge limiter bypass with rotating junk tokens; audit-row amplification | P2 | confirmed vulnerability (availability) | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-03 | Admin can reset/demote peer admins | P2 | confirmed design gap | Security | Fixed – verified by QA |
| SEC-04 | Account enumeration via registration/invite (response + timing) | P2 | design gap | Security + Owner | Mitigated – claimed (implementer tests; full suite green): timing equalised, generic invite code; registration 409 remains until e-mail |
| SEC-05 | Owner can withdraw a member's own erasure request | P2 | confirmed design gap (privacy) | Security | Fixed – verified by QA |
| SEC-06 | No idle timeout / session revoke / logout-all; token in localStorage | P2 | design gap | Security | Partly fixed – verified by QA (idle revoke, list, revoke-others); cookie migration open |
| SEC-10 | One shared secret both directions + arbitrary SQL on data plane | P2 | design gap | Security | Partly fixed – verified by QA (allow-list on real Worker code); per-direction secrets open |
| OPS-07 | Synchronous D1 writes on every authenticated request | P2 | operational | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| JOB-01 | Storage error during heartbeat orphans worker subprocess | P2 | design gap | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| JOB-02 | Reaper fails queued jobs waiting for the runner slot | P2 | confirmed bug | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| COPY-02/UX-01 | Indonesian server messages masked by generic texts | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| A11Y-01 | Focus lost after async actions | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| A11Y-02 | Registration field errors not associated/announced | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| A11Y-03 | "Not included" plan features are visual-only | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| A11Y-04 | Role `<select>` unnamed, acts on every change; repeated button names | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| UX-02 | Irreversible member/key actions without confirmation | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| UX-03 | Admin sees self-edit controls (e-mail vs name comparison) | P2 | confirmed defect | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| CI-03 | Actions pinned by tag; pip unpinned; no Dependabot; no image scan | P2 | supply-chain gap | QA + Owner | Fixed locally (partly) |
| SUP-01 | Python pins without hashes; editable install in image | P2 | supply-chain gap | QA | Open |
| SUP-02 | Image built from uncommitted working tree; no provenance/scan | P2 | supply-chain gap | Owner | Open |
| SUP-07 | Secret-scan coverage gaps (blanket `<...>` exemption, missing formats, dist) | P2 | tooling gap | QA | Fixed – verified by QA (fake-key exemption now test-files only) |
| OPS-02 | No off-machine backup | P2 | operational | Owner | Owner decision |
| OPS-03 | No alerting, uptime monitor, paging, postmortem template | P2 | operational | QA + Owner | Open |
| OPS-04 | Gateway secret rotation needs downtime (single value) | P2 | design gap | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| BILL-02 | Billing integration prerequisites (webhook, idempotency, status re-check) | P2 | design gap | Backend | Fixed for sandbox – verified by QA (webhook checks, idempotency); live gates open |
| MAIL-01 | No transactional e-mail | P2 | business decision | Owner (+Security token service) | Owner decision; disabled token service implemented (EMAIL_ENABLED=false enforced) |
| LEGAL-02 | Privacy "not used" facts must change with billing/e-mail | P2 | business-legal | Owner + QA | Docs updated (LEGAL_AND_PRIVACY facts); in-app Privacy text must change when billing/e-mail go live |
| ML-01 | Interval coverage check without target embargo | P2 (plan) | methodology | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-07 | API-key quota counted before authorisation; racy | P3 | design gap | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-08 | API keys: no expiry/scope; weak prefix | P3 | design gap | Security | Open |
| SEC-09 | Worker error replies lack security headers (doc mismatch) | P3 | confirmed | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-11 | Plan-feature gates only on create | P3 | latent gap | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-12 | Data export reveals operator names/notes, all members' user-agents | P3 | privacy gap | Security + Owner | Fixed – verified by QA |
| SEC-13 | Unvalidated report params stored | P3 | confirmed | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-14 | Spreadsheet neutralisation misses Cf/Cc chars | P3 | hardening | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| SEC-15 | Hygiene: `assert` invariants, client request ids, `next=` control chars, href schemes, HSTS | P3 | hardening | Security / Frontend | Partly fixed – claimed (implementer tests): asserts, request ids; HSTS for a custom domain open |
| SQL-01 | 13 `# nosec` lines (policy) | P3 | policy | Security | Fixed – verified by QA (0 nosec) |
| AUTH-01 | Password hash has no parameter/version tag | P3 | design gap | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| AUTH-02 | E-mail normalisation lower-case only | P3 | design gap | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| AUTH-03 | Duplicate-registration race → orphan org + 500 | P3 | confirmed by code | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| AUTH-04 | Temporary password never expires | P3 | design gap | Security | Fixed – expiry returned verified by QA; refusal claimed (implementer tests) |
| AUTH-05 | Password change has no failure counter | P3 | design gap | Security | Fixed – claimed (implementer tests; full suite green in wave F) |
| DATA-01 | Writes retried on ambiguous 502/503/504 | P3 | design gap | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| DATA-02 | No idempotency for sync POST /forecasts, /reports | P3 | design gap | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| DATA-03 | STORAGE.md drift (edge caching, KV retention) | P3 | documentation | Backend | Fixed locally (docs) |
| DATA-04 | Migration data-preservation only tested for 0006; DB-ahead not reported | P3 | test gap | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| DATA-05 | Key regex `$`, MAX_SQL_PARAMS 200, `clip_export` fails open | P3 | hardening | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| ML-02 | 0.10 under-coverage threshold lets short 95% interval pass | P3 | business decision | Owner + Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| ML-03 | Accuracy reported on the selection backtest | P3 | disclosure | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| ML-04 | Month semantics use server-local date | P3 | design gap | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| ML-05 | Bundle files carry no payload hash | P3 | design gap | Backend | Fixed – claimed (implementer tests; full suite green in wave F) |
| CI-05 | CT workflow interpolated inputs into shell | P3 | hardening | QA | Fixed – verified by QA |
| CI-06 | Security-job comment claimed clean run (relied on nosec) | P3 | documentation | QA | Fixed locally |
| CI-07 | Hand-written web types duplicate contract schemas | P3 | drift risk | Frontend | Open |
| SUP-03 | Compose images by tag; Grafana fallback password (local only) | P3 | hygiene | QA | Accepted (local only) |
| SUP-04 | `legacy-peer-deps=true` hides peer mismatches | P3 | hygiene | QA | Open |
| SUP-05 | `npm sbom` failed (root package without version) | P3 | tooling | QA | Fixed – verified by QA |
| SUP-06 | Gateway `.map` files in dry-run output | P3 | false positive | — | Closed (false positive) |
| OPS-05 | Runbooks still describe Render, not the laptop topology | P3 | documentation | QA | Open |
| A11Y-05..07 | Small targets; empty `<th>`; unnamed buttons; one-time secrets not live regions | P3 | defects | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| COPY-03/04 | L10N numbers/dates/time zone; contradictory scenario/toast copy | P3 | defects | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| UX-04..06 | Timeout/403 copy, hidden request id on 4xx, empty-state CSV, /operasi title leak | P3 | defects | Frontend | Fixed – claimed (implementer tests; full suite green in wave F) |
| PERF-01/02 | `no-store` on public data + waterfall; entry payload 170 KB gz | P3 | performance | Frontend | PERF-01 fixed locally; PERF-02 accepted |
| DATA-06 | Legacy `data/raw/wti.csv` as default WTI seed (image, settings, pipeline CLI) | P3 | informational / config defect | Backend + QA + Security | Fixed – verified by QA (suite green on tracked seed) |
| OPS-08 | MLflow backup not relocatable: an experiment `artifact_location` is an absolute path into the live `var/` | P3 | operational limitation | Backend | Open |
| TEST-03 | Order-dependent backend tests (test_access_control assumes no promoted model; test_product snapshot test assumes an earlier plan upgrade) | P3 | test robustness | Backend | Open (reproduced by QA; pass in default order) |
| TEST-01 | Untested critical paths: backup CLI, clone-state, export-model CLI, ingest job orchestration; coverage 79% | P3 | test gap | Backend + QA | Open |
| TEST-02 | Web Vitest flake under load (`ops.test.tsx` wait) | P3 | test robustness | Frontend | Fixed – reviewed by QA (legitimate determinism fix; no perf budget) |

## Entries

Format per entry: **type · severity · confidence · component** — evidence — reproduction — impact — fix — tests —
acceptance criterion — owner — status.

### P1

**SEC-01 – Client IP collapses behind the Worker.** Likely vulnerability · P1 · medium (~0.7) · gateway +
`security.py`.
- Evidence: `apps/gateway/src/index.ts:302` forwards only `cf-connecting-ip`; `backend/icp_api/security.py:110-116`
  trusts it when the gateway secret verifies; Cloudflare overwrites it with the Worker egress address on cross-zone
  subrequests (the backend is behind a Quick Tunnel/Render, both proxied by Cloudflare).
- Reproduction (synthetic): every request with the secret and `CF-Connecting-IP: 2a06:98c0:3600::103`; 5 wrong
  passwords for one e-mail → 6th request `429 scope=client`; any other user from that "IP" also limited.
- Impact: targeted lock-out of any known e-mail for 15 min; `auth` bucket (10/min) and per-IP buckets become global.
- Fix: Worker sends `x-icp-client-ip` (optionally HMAC-bound); backend reads it only when the secret verifies and
  ignores `cf-connecting-ip`/`x-forwarded-for` from the gateway.
- Tests: gateway unit test for the header; backend test that `client_key` uses it only via the gateway.
- Acceptance: tests pass; staging log (hashed IP) shows distinct values for two clients — staging step is an owner
  action because deployment is out of scope.
- Owner: Security · Status: Open.

**CI-01 – CI test data.** Confirmed defect · P1 · high · `backend/tests/conftest.py`, `pipeline/tests/conftest.py`, CI.
- Evidence: Git index lists `data/raw/dataset.csv` (force-added) but **not** `data/raw/wti.csv` (DVC-only,
  `data/.gitignore: /raw`); `data/seed/*.csv` are tracked; `.dvc/config` has no remote and CI never runs `dvc pull`.
  The backend `app` fixture checked only `dataset.csv`, while the app seeds WTI from `data/raw/wti.csv`
  (`settings.py:274`) and silently skips a missing file (`main.py:36`).
- Reproduction: `SEED_WTI_PATH=<absent path> pytest backend/tests` → **2 failed, 899 passed, 39 errors**
  (KeyError 'wti', DATA_INSUFFICIENT, readiness `degraded`). So the audit's original "tests skip in CI" was wrong in
  mechanism: a clean CI run **fails** (or, for `repo_data`, skips).
- Impact: CI red (or misleadingly partial) on every push; the remote run state is unknown.
- Fix (done): test fixtures use Git-tracked inputs only (`SEED_ICP_PATH=data/raw/dataset.csv`,
  `SEED_WTI_PATH=data/seed/wti.csv`); `ICP_REQUIRE_DATA=1` (set in ci.yml) turns any data skip into a failure.
- Tests: full pytest with `ICP_REQUIRE_DATA=1` (see REMEDIATION_LOG for the run).
- Acceptance: full suite passes with tracked data only; zero data-related skips; CI sets `ICP_REQUIRE_DATA=1`.
- Owner: QA · Status: Fixed – verified by QA.

**CI-02 – bandit green only through `# nosec`.** Policy / false green · P1 · high · `.github/workflows/ci.yml`, SQL builders.
- Evidence: `bandit -ll` → 0 issues with 13 skipped; `bandit -ll --ignore-nosec` → 13 Medium, exit 1 (12×B608,
  1×B104). Lines: `cli.py:243,252,264`; `settings.py:244`; `storage/product.py:48,54,94,227,260,280,605,628`;
  `storage/repository.py:99`. All reviewed as non-exploitable today.
- Fix: Security removes the markers by refactoring (SQL-01 alternatives); CI runs bandit with `--ignore-nosec`
  (done in ci.yml).
- Tests: `bandit -r backend/icp_api pipeline/icp_ml -ll --ignore-nosec` exits 0; `grep -rn "nosec" backend pipeline` → none.
- Acceptance: both commands clean; security job unchanged otherwise.
- Owner: Security (code) + QA (CI) · Status: In progress (CI side done; code side wave A).

**CI-04 – CT workflow vs bundle mode.** Operational · P1 · high · `.github/workflows/continuous-training.yml`.
- Evidence: staging runs `MODEL_SOURCE=bundle`, which refuses `train` with `409 OPERATION_UNAVAILABLE`
  (DEPLOYMENT.md); the workflow ran `train` on a cron (5th/12th); repository variable/secret not configured.
- Fix (done): schedule removed (manual dispatch only); reads `/api/v1/system/status` and trains only when
  `model_source=mlflow` **and** input `train=true`; otherwise prints the operator-local procedure.
- Tests: YAML parses; logic reviewed (cannot trigger workflows).
- Acceptance: no `schedule:`; `train` guarded; inputs via `env:`.
- Owner: QA · Status: Fixed locally.

**OPS-01 – Restore never rehearsed.** Operational · P1 · high · `cli.py backup`, docs/OPERATIONS.md.
- Evidence: no restore command; three ~34 MB unencrypted backups in `var/backups/` on the same disk, no manifest,
  checksum, retention or test; D1 export/Time Travel and KV copy documented but never exercised.
- Fix: restore CLI with verification (wave C) and a rehearsal on a copy: copy the latest backup to a temp folder →
  `PRAGMA integrity_check` + row counts vs live DB opened read-only → throwaway API on another port pointed at the
  copy → `/health/live`, data summary, latest forecast, schema v6 → record RTO/RPO in OPERATIONS.md.
- Tests: CLI round-trip test (backup → restore into temp → integrity + counts).
- Acceptance: rehearsal executed and recorded with timings; restore command tested.
- Owner: Backend (CLI) + coordinator (rehearsal) · Status: Open.

**OPS-06 – Hosting topology.** Operational / owner · P1 · high · deployment.
- Evidence: backend container `icp-api:staging-20261017` on the operator laptop behind a Cloudflare Quick Tunnel
  ("no uptime guarantee", testing only; hostname changes on restart); D1 at schema 4 (0005/0006 not applied);
  `METRICS_TOKEN` required at start-up of the new image (presence not verified, values off-limits); single instance
  enforced (`API_INSTANCES=1`); rollback path documented for Render.
- Fix: owner decision (always-on host) — see OWNER_DECISIONS_REQUIRED.md §9; before any redeploy: back up D1,
  apply 0005/0006, set `METRICS_TOKEN`, redeploy Worker, smoke test.
- Acceptance: documented decision; deployment checklist executed by the owner.
- Owner: Owner · Status: Owner decision.

**BILL-01 – Billing not implemented.** Business decision · P1 · high · accounts/entitlements.
- Evidence: only "no payment provider" comments (`entitlements.py:4`, `routers/accounts.py:269`,
  `services/accounts.py:9,460`); plans change via operator `set_plan`; plan requests take no payment.
- Fix: owner chooses provider/prices (OWNER_DECISIONS §3–6); wave E builds a provider-neutral, disabled-by-default
  sandbox adapter (see [BILLING_PROVIDER_COMPARISON.md](BILLING_PROVIDER_COMPARISON.md)).
- Acceptance: owner decisions recorded; sandbox adapter tests with synthetic payloads; `BILLING_ENABLED=false` default.
- Owner: Owner (+Backend) · Status: Owner decision.

**LEGAL-01 (alias COPY-01) – Legal drafts.** Business-legal · P1 · high · `apps/web/src/pages/legal-content.ts`, docs/LEGAL_AND_PRIVACY.md.
- Evidence: visible "[Diisi pemilik: …]" placeholders for entity, address, contact, age, liability, law, notice,
  legal basis, production host, transfer, three retention periods, response time; not reviewed by counsel.
- Fix: owner + counsel fill them; no public or paid launch before.
- Acceptance: no placeholder left; counsel sign-off date recorded.
- Owner: Owner · Status: Owner decision.

### P2

**SEC-02 – Edge limiter bypass + audit amplification.** Confirmed vulnerability (availability/cost) · high.
Evidence `index.ts:72-80` (limiter keyed on sha256(Authorization) outside `/auth/*`), `security.py:306-310` (audit row
per unknown token); measured +5 audit rows for 5 junk tokens; unthrottled routes `/org*`, `/auth/me`, `/audit`,
`/admin/*`, `/jobs*`, `/system/status`. Impact: D1 free 100k writes/day exhaustion. Fix: always IP-keyed edge bucket
(token bucket as second), backend `default` limit on those routes, aggregate `auth.failed`. Tests: gateway rotating-junk
test; backend N invalid tokens → ≤1 aggregated row. Acceptance: tests pass. Owner Security · Open.

**SEC-03 – Admin over peer admin.** Confirmed design gap · high. Evidence `services/accounts.py:296-348`; reproduction:
admin1 resets admin2 (200 + temp password), signs in as admin2, demotes admin2. Fix: target role ≥ actor role → 403
(owner-only). Tests: access-control cases for reset/demote/disable expecting 403. Acceptance: tests pass. Owner
Security · Open.

**SEC-04 – Account enumeration.** Design gap · high. Evidence `accounts.py:111-112` (409 EMAIL_TAKEN in 4 ms vs ~146
ms login), invite path `248-249` reveals other tenants. Fix (interim, no e-mail): constant work on the 409 path,
generic messages, invite answer not revealing other tenants, list in SECURITY.md; final fix needs e-mail (MAIL-01).
Tests: timing-parity test. Acceptance: parity within noise; doc entry. Owner Security (+Owner for e-mail) · Open.

**SEC-05 – Owner withdraws a member's erasure request.** Confirmed design gap (privacy) · high. Evidence
`services/privacy.py:193-200`, `routers/accounts.py:396-409`; reproduction: member requests, owner withdraws → 200. Fix:
only the requester withdraws `account` scope. Tests: owner → 403/404. Owner Security · Open.

**SEC-06 – Session controls.** Design gap · high. Evidence `services/accounts.py:28,164-198`, `client.ts:6,38-39`. Fix:
idle timeout, revoke one / logout-all endpoints, cookie migration as design (wave B). Tests: idle-expired 401;
revoke/logout-all. Owner Security · Open.

**SEC-10 – Shared secret both directions + arbitrary SQL.** Design gap (documented residual) · high. Evidence
`index.ts:154-184,232-249`. Fix: per-direction secrets, reject DDL/PRAGMA at the Worker, optional HMAC+timestamp.
Tests: gateway rejects DDL. Owner Security · Open.

**OPS-07 (audit 1 "OPS-01") – Synchronous D1 writes per request.** Operational · high. Evidence `touch_session` per
request (`services/accounts.py:197`), API key `touch_api_key` + `record_usage`. Fix: write `last_seen_at`/`last_used_at`
only when older than N minutes. Tests: two requests within N minutes → one write. Owner Security · Open.

**JOB-01 – Heartbeat storage error orphans the worker.** Design gap · high (code). Evidence `services/jobs.py:243-289`.
Fix: terminate/kill in `except`, `finish_job(failed, RUNNER_ERROR)`, tolerate N heartbeat failures. Tests: failing
repository stub → subprocess terminated. Owner Backend · Open.

**JOB-02 – Reaper fails queued jobs.** Confirmed bug · high. Evidence `fail_stale_jobs` covers queued jobs without
heartbeat; probe reaped a queued `ingest_fx` behind a running `ingest_icp`. Fix: heartbeat/owner for queued jobs or
separate queued TTL. Tests: two job types, one slot, short stale window → second job runs. Owner Backend · Open.

**ML-01 – Coverage without embargo.** Methodology · high (mechanism) / medium (materiality). Evidence
`pipeline/icp_ml/backtest.py:278-302`; v5 hold_last h6 50%: 0.739 current vs 0.435 embargoed. Fix: embargo
`max(horizon)`; disclose for existing artefacts; no retraining. Tests: non-overlap unit test. Owner Backend · Open.

**COPY-02/UX-01, A11Y-01..04, UX-02, UX-03 – Frontend defects.** Confirmed · high · `apps/web`. Evidence and
reproductions in audit 3 (e.g. `components/ui.tsx:196-238,63`, `AuthPages.tsx:168-178`, `LandingPage.tsx:77`,
`AccountPage.tsx:98-140`). Fixes: dedicated i18n keys for EMAIL_TAKEN/AUTH_INVALID/SELF_EDIT; `aria-disabled` + focus
management; `aria-invalid`/`aria-describedby` + focus first invalid; hidden "tidak termasuk" text; named selects with
explicit confirm; ConfirmDialog for revoke/disable/reset; compare user ids. Tests: Vitest per case. Acceptance: tests
pass + keyboard walk-through. Owner Frontend · Open.

**CI-03 – Action pinning / update automation.** Supply-chain gap · high. Done: checkout, setup-python and setup-node
pinned to commit SHAs resolved from the public GitHub API (`git/ref/tags/v4|v5`, 2026-10-20) with the tag in a
comment; pip pinned (`PIP_VERSION=26.2.1`). Open (owner/infra): verify SHAs against the release pages, enable
Dependabot/Renovate (repository setting), add image build + scan. Owner QA + Owner · Fixed locally (partly).

**SUP-01 – No hashes in Python pins.** Supply-chain gap · high. Evidence pip-audit warning; `setuptools>=69`
unpinned; `pip install -e` in the image. Fix: hash-pinned lock (`pip-compile --generate-hashes` or `uv pip compile`)
— needs a tool install, so deferred; non-editable install in the image. Owner QA · Open.

**SUP-02 – Image provenance.** Supply-chain gap · high. Evidence: render.yaml/DEPLOYMENT "built locally from the working
tree". Fix: build from a committed tag in CI, attach SBOM, scan (trivy/grype), push with digest. Owner Owner (+QA) · Open.

**SUP-07 – Secret-scan coverage.** Tooling gap · high. Fixed: value-level placeholder exemptions (no blanket `<...>`
line exemption), Stripe/Midtrans/Xendit/Google/npm/Docker/Render/Resend/SendGrid/JWT/Cloudflare-assignment rules,
quoted/unquoted high-entropy credential assignments, scans `apps/*/dist`, `.sh/.ps1/.pem/.key`, never opens
`.env*`/`.dev.vars*`, built-in `--self-test` (also run before every scan). Residual: no Git-history scan. Owner QA ·
Fixed locally.

**OPS-02 – No off-machine backup.** Operational · high. Owner decision (encrypted off-site copy of `var/backups`
and MLflow). Owner Owner.

**OPS-03 – Monitoring/incident response.** Operational · high. Present: liveness/readiness, `/metrics` with
mandatory token when deployed, JSON logs with request id. Missing: alert rules, uptime monitor, paging, log retention,
SLA, postmortem template. Fix: postmortem template + alert-rule examples in docs (QA); uptime monitor/paging is an
owner choice. Owner QA + Owner · Open.

**OPS-04 – Secret rotation downtime.** Design gap · high. Evidence `middleware.py:106-108` single value. Fix: accept
current + previous secret during rotation (comma list), documented procedure. Owner Security · Open.

**BILL-02 – Billing prerequisites.** Design gap · high. Raw-body webhook route verified by provider signature (outside
the gateway-secret requirement), `payment_events` with unique provider id, status-API re-check before `set_plan`,
out-of-order handling, refunds/downgrades, PPN decisions, always-on host. Owner Backend (sandbox) · Open.

**MAIL-01 – No transactional e-mail.** Business decision · high. One-time passwords handed over out of band
(`services/accounts.py:280`); no reset/verification/alert e-mail. Owner chooses provider + domain (OWNER_DECISIONS §8);
wave B builds a disabled token service. Owner Owner · Owner decision.

**LEGAL-02 – Privacy facts vs code.** Business-legal · high. "Not used: payment processing, e-mail delivery" must change
together with any provider. Owner Owner + QA · Open.

### P3 (one line each; details in the audit sources)

- **SEC-07** quota counted before authorisation and racy (`security.py:297-300`) → record after status, conditional insert.
- **SEC-08** API keys without expiry/scope, prefix `icp_live_`+3 chars → `expires_at`, read-only default, 8-char prefix.
- **SEC-09** Worker `errorResponse` lacks security headers (`index.ts:46-52`), contrary to SECURITY.md → route through `apiSecurityHeaders` + tests.
- **SEC-11** plan gates only on create (`routers/product.py`) → `require_feature` everywhere (latent).
- **SEC-12** export includes operator names/notes and all members' user-agents → redact; LEGAL review.
- **SEC-13** unvalidated report params stored → strict schemas (`extra="forbid"`).
- **SEC-14** `cell_safe` misses `​`, `﻿`, `\x00` prefixes → strip Cf/Cc first.
- **SEC-15** `assert` as security invariant; client-chosen request ids; `next=` control chars; `href` scheme; HSTS once a custom domain exists.
- **SQL-01** 13 `# nosec` → refactor per the alternatives in audit 1 (tracks CI-02).
- **AUTH-01..05** hash format without parameters/rehash; e-mail NFKC/IDNA; registration race → 409; temp-password expiry; password-change failure counter.
- **DATA-01..05** ambiguous write retries; POST idempotency keys; STORAGE.md drift; migration preservation tests 0002–0005 + DB-ahead readiness note; key regex/param limit/`clip_export` fail-closed.
- **ML-02** under-coverage threshold (owner decision on rule; do not weaken). **ML-03** selection optimism disclosure. **ML-04** `Asia/Jakarta` date helper. **ML-05** sha256 in bundle meta/pointer.
- **CI-05** inputs interpolated into `run:` → passed via `env:` (fixed). **CI-06** misleading security-job comment (fixed). **CI-07** hand-written web types → alias contract schemas.
- **SUP-03** compose images by tag, Grafana fallback password (localhost only, accepted). **SUP-04** `legacy-peer-deps=true`. **SUP-05** root package `version` added; `npm sbom --sbom-format cyclonedx --omit dev` → CycloneDX 1.5 (fixed). **SUP-06** gateway `.map` files only in dry-run output (false positive).
- **OPS-05** runbooks mention Render dashboard/previous image tag for the laptop topology → update.
- **A11Y-05..07, COPY-03/04, UX-04..06, PERF-01/02** frontend P3 items as listed in audit 3.
- **DATA-06** (raised during remediation; informational, resolved): there are two WTI files.
  - The **active** local WTI dataset v3 (`dsv_01a144f72a6ab8fb408c8a6c`, "EIA RWTC, monthly mean of daily
    prices") and production model v5 both use the EIA monthly mean. That series is **byte-identical to the
    Git-tracked `data/seed/wti.csv`**, so every customer-facing number and the model use the EIA series.
  - The DVC-only legacy `data/raw/wti.csv` has different values and a different header (2026-06: 95.96 vs
    84.8071). It fed only the inactive seed version v1.
  - The defect was in the defaults, which still pointed at the legacy file: `settings.seed_wti_path`, the
    pipeline CLI `DEFAULT_WTI`, and the Docker image, which copied `data/raw/wti.csv`.
  - Fix:
    - pipeline CLI default → `data/seed/wti.csv` (wave C, `test_v7_data_defaults`);
    - Dockerfile copies `data/seed/` and sets `SEED_WTI_PATH=/app/data/seed/wti.csv`; it no longer copies the
      legacy file (QA);
    - test fixtures use the seed (CI-01);
    - settings default → seed (wave A, pending).
  - No dataset was rewritten.
- **OPS-08** (raised by the OPS-01 rehearsal, 2026-10-20): in backup `20261020T024313Z`, 1 of the 2 MLflow
  experiments in `mlflow.db` stores an absolute `artifact_location` inside the live `var/`.
  - Impact: restoring the MLflow store to another path or machine would point runs at missing or live
    artifacts. Bundle-mode serving (staging) is unaffected.
  - Fix: document a path-rewrite step, or use relative or `mlflow-artifacts:` URIs; rehearse an MLflow-mode
    restore on another path.
  - Owner: Backend.
- **TEST-01** untested: `icp-api backup`, `clone-state`, `export-model` CLI, ingest job orchestration; coverage 79% (cli.py 32%, legacy ingestion 0%; `tasks.py`/`worker.py` 0% because coverage does not follow the job subprocess).
- **TEST-02** web Vitest flake under CPU load (`ops.test.tsx`, BASELINE.md) → raise wait or await lazy chunk explicitly.

## Scan record (2026-10-20, before remediation)

| Tool | Result |
|---|---|
| pip-audit 2.10.1 `-r requirements/constraints.txt --no-deps --disable-pip` | no known vulnerabilities (134 components) |
| bandit 1.9.4 `-ll` | 0 issues (13 skipped by `# nosec`) |
| bandit 1.9.4 `-ll --ignore-nosec` | 13 Medium → exit 1 |
| `npm audit --omit=dev` / `npm audit` (npm 10.9.2) | 0 / 0 vulnerabilities |
| pytest + coverage | 1,002 passed, 79% line coverage (with the developer's DVC data present) |
