# Launch acceptance (wave F, 2026-10-20)

Status: CURRENT. This is the independent QA verification after waves A–E. Evidence is in
[../evidence/2026-10-20-production-readiness/](../evidence/2026-10-20-production-readiness/INDEX.md).
Statuses: **Pass** (reproduced by QA) · **Fail** · **Blocked** (needs an owner decision or an action outside
this mission) · **Open** (not done).

## Recommended status: **READY FOR STAGING VERIFICATION** (not ready for production)

Reasons:
- **Local gates are green and were reproduced by QA.**
  - `npm run check` passes: exit 0, 340 s.
  - pytest: 1169 passed, 0 skipped. Web: 144 passed. Gateway: 33 passed.
  - Lint, mypy, contracts, build and the secret scan pass.
  - bandit `-ll --ignore-nosec` is clean with 0 `# nosec`; pip-audit is clean.
  - 17/17 independent security spot checks passed, 11/11 data-plane checks passed, and the restore rehearsal
    worked.
- **The changed code has never run on the hosted stack.**
  - Staging still runs the 2026-10-17 image with D1 at schema 4. It needs migrations 0005–0008, the new image
    and a Worker redeploy.
  - The client-IP fix (SEC-01) is only proven in unit tests.
  - Staging verification is the next step, and only the owner may deploy.
- **Production is blocked by owner decisions, not code.**
  - Always-on hosting: the Quick Tunnel on a laptop is not production hosting.
  - Live billing (merchant account, prices, tax, refunds).
  - Transactional e-mail (provider, domain).
  - Legal texts (entity, counsel review, 14 placeholders).
  - Off-site backups and monitoring.

## Checklist

| # | Gate | Status | Evidence |
|---|---|---|---|
| 1 | `npm run check` (lint, typecheck, contracts, all tests, build, secret scan) exits 0 | **Pass**: run 2 exit 0, 340 s; pytest 1169 passed / 0 skipped; web 144; gateway 33 | `npm-run-check.log`. Run 1 was killed by a session interruption; see note A |
| 2 | Tests run from Git-tracked data only; CI fails instead of skipping (`ICP_REQUIRE_DATA=1`) | **Pass** locally (0 skips). Remote CI never ran: **Blocked** (no push allowed) | `checks/ci01-clean-checkout-simulation.txt` (before the fix), ci.yml |
| 3 | bandit `-ll --ignore-nosec` clean; 0 `# nosec` | **Pass** (Medium 0, High 0; 7 Low below threshold) | `bandit-ignore-nosec.txt` |
| 4 | pip-audit on `requirements/constraints.txt` | **Pass**: no known vulnerabilities (pip-audit 2.10.1, 2026-10-20) | `pip-audit.txt` |
| 5 | npm audit | **Not re-run in wave F**: it needs the registry, which the brief excluded. Earlier today: 0 vulnerabilities (prod and full) | audit 4 |
| 6 | Secret scan with self-test; fake-key exemption limited to test files | **Pass** | `scripts/secret-scan.mjs --self-test` |
| 7 | Admin cannot reset, demote or disable a peer admin; the owner can | **Pass** (403/403/403; owner 200) | `checks/security-spotcheck.json` |
| 8 | Only the requester can withdraw a deletion request | **Pass** (owner 404, admin 404, requester 200) | same |
| 9 | Export: only the owner's own sessions, no hashes, operator identity redacted | **Pass** | same |
| 10 | Idle session refused **and** revoked server-side; sign-out-everywhere-else; session list without token or user agent | **Pass** | same |
| 11 | Temporary password returns an expiry | **Pass** (`member.temporary_login_expires_at`). Expiry refusal (`TEMP_PASSWORD_EXPIRED`) is covered by the implementer's tests only | same |
| 12 | Data plane refuses DDL, PRAGMA writes, ATTACH, transactions and stacked statements; needs the secret | **Pass** against the real Worker code (bundled with the repo's esbuild): 8/8 refused with 403 without reaching D1; no secret → 401 | `checks/dataplane-check.json` |
| 13 | Billing webhook: bad signature 401, bad amount 409, wrong environment 409, the order untouched; a valid notification applies once; replay is idempotent; late `pending` ignored; no automatic plan change; `signature_key` not stored | **Pass** (test-only config with a random sandbox-format key) | `checks/security-spotcheck.json` |
| 14 | `EMAIL_ENABLED=true` refused at start-up | **Pass** (`ConfigError`) | same |
| 15 | Restore from backup on a copy (integrity, schema, server ready, reads equal live) | **Pass** (4.2 s to live, 6.8 s total). D1, KV and MLflow-mode restores **Open** | `checks/restore-rehearsal.json`, OPERATIONS.md |
| 16 | Test robustness | **Pass with findings**: the wave D waits are a legitimate determinism fix (note B); two order-dependent backend tests are reported as TEST-03 (note C) | — |
| 17 | Provenance and data facts preserved (Sep 2026 = 112.81) | **Pass** (live snapshot and restored copy identical) | `checks/restore-rehearsal.json` |
| 18 | Staging redeploy: D1 0005–0008, new image, Worker, smoke test, SEC-01 hosted check | **Blocked**: owner action (deployment prohibited) | DEPLOYMENT.md "Pending" |
| 19 | Always-on hosting with backups off the machine | **Blocked**: owner decision (OWNER_DECISIONS §9) | — |
| 20 | Live billing | **Blocked**: owner gates in BILLING.md (merchant/KYC, prices, PPN, refunds, production keys) | BILLING.md |
| 21 | Transactional e-mail | **Blocked**: provider, domain, SPF/DKIM/DMARC (§8) | — |
| 22 | Legal: entity named, counsel review, 14 placeholders filled | **Blocked**: owner + counsel (§1, §7) | LEGAL_AND_PRIVACY.md |
| 23 | Monitoring: uptime, alerting, on-call/SLA | **Blocked**: owner (§10) | OPERATIONS.md |
| 24 | Supply chain: hash-pinned Python deps, image provenance/scan, Dependabot | **Open** | RESIDUAL_RISKS.md |

## Notes

**A. Run 1 of `npm run check`.** The first run was killed by a session interruption during pytest (the task
was reported as `killed`). That run showed 2 failures, both with `WORKER_CRASHED`, in tests that start job
subprocesses: `test_v7_jobs::…metadata_store_fails…` and `test_product::test_public_snapshot…`. This is
consistent with externally killed child processes.
- Re-run 2 of the full check was green.
- The `test_v7_jobs` test passed 3/3 in isolation.
- Running `test_product::test_public_snapshot…` **alone fails for a different reason**: `points 3 != 4`.
  That is TEST-03 (note C), not a product defect.
- The log of the interrupted run is kept (`npm-run-check-run1-interrupted.log`) with synthetic test tokens
  redacted.

**B. Wave D waits (ops.test.tsx 5 s first render, flows.test.tsx 8 s navigation; lazy modules preloaded in
`beforeAll`).** Judgement: **legitimate.**
- The time being waited on is vitest/jsdom module transformation and a11y-tree walks under parallel test
  load, not product latency. Production uses prebuilt chunks.
- The assertions are unchanged. The preload makes the tests measure the page instead of the module loader.
- Residual: a genuine slowness regression of a few seconds would no longer fail these two files. There is no
  performance budget test (PERF), which is recorded as a residual.

**C. TEST-03: order-dependent backend tests (reproduced, not fixed: the cause is in the test files'
assumptions, not in `conftest.py`).** All files share one session-scoped app.
- `test_access_control.py` assumes that no production model has been promoted yet (it expects `503`). Running
  `pytest backend/tests/test_api.py backend/tests/test_access_control.py` gives 2 failures: line 228 gets
  201 instead of 503, and line 427 gets 422 instead of 503.
- `test_product.py::test_public_snapshot_insights_and_latest_forecast` assumes that an earlier test upgraded
  the session-scoped `owner` organisation to Professional (horizon 4). Run alone it gets 3 points (trial
  horizon).
- Both pass in the default alphabetical order, which is the order CI uses.
- Fix (test owner): make each test establish its own preconditions (promote or demote explicitly, or set the
  plan inside the test), or give `test_access_control` its own app fixture.

## Changes QA made in wave F
- `scripts/secret-scan.mjs`: the fake/dummy/"not-a-secret" exemption now applies **only in test files**
  (`tests/` or `test/` directories, `*.test.*`, `test_*.py`, `conftest.py`). The same synthetic-looking key
  elsewhere is reported. Self-test cases were added for both directions and for test-file detection. The
  previously global `/dummy/i` and `^(fake|sample|synthetic)-` exemptions were moved to the test-only list.
  The scan is still clean.
- Evidence folder created; no product code changed.
