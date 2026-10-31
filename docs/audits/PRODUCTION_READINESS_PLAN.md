# Production-readiness plan (2026-10-20)

Status: CURRENT. Inputs: [BASELINE.md](BASELINE.md) and four independent read-only audits (security &
identity, backend/data/ML, frontend/product/accessibility, QA/DevSecOps/commercial), consolidated in
[FULL_RISK_REGISTER.md](FULL_RISK_REGISTER.md). Owner-only questions: [OWNER_DECISIONS_REQUIRED.md](OWNER_DECISIONS_REQUIRED.md).

## Executive summary

- **No P0 found.** Tenant isolation, route authorization, report/export safety, provenance (93 months,
  0 disagreements; Sep 2026 = US$112.81) and the forecasting no-look-ahead fix were re-verified.
- **P1 – fix locally:** client IP likely lost behind the Worker → throttles become global (SEC-01, needs
  staging confirmation); CI may skip tests and depends on `# nosec` (CI-01, CI-02); the scheduled
  training workflow cannot succeed against bundle-mode staging (CI-04); restore never rehearsed (OPS-01).
- **P1 – owner decisions, not code:** always-on hosting vs the laptop + Quick Tunnel (OPS-06); billing
  (BILL-01: not implemented); legal texts with 14 placeholders (LEGAL-01/COPY-01).
- **P2 – confirmed defects to fix:** admin can take over a peer admin (SEC-03), owner can cancel a
  member's deletion request (SEC-05), edge-limiter bypass with junk tokens + audit-write amplification
  (SEC-02), queued jobs reaped as "interrupted" (JOB-02), worker not terminated on storage failure
  (JOB-01), weak session controls (SEC-06), keyboard focus loss / unannounced form errors / unconfirmed
  destructive actions / masked Indonesian server messages in the UI (A11Y-01..04, UX-02/03, COPY-02),
  interval coverage computed without an embargo (ML-01).
- Verdict before remediation: **NOT READY** for production; target of this work: local readiness gates met, with
  hosting, billing, e-mail and legal gates explicitly open.
- **Verdict after wave F (2026-10-20): READY FOR STAGING VERIFICATION**, not ready for production. Local gates
  are green and independently reproduced; staging redeploy and owner gates remain. See
  [LAUNCH_ACCEPTANCE.md](LAUNCH_ACCEPTANCE.md).

## Waves and ownership (implementation)

| Wave | Content | Owner agent | Files (exclusive) |
|---|---|---|---|
| A – security & integrity | SEC-01/02/03/05/07/09/10/11/12/13/14/15, AUTH-02/03/04/05, SEC-04 mitigation, SQL-01 (remove every `# nosec` by refactoring), D1 write amplification (session last-seen throttling) | Security | backend/icp_api/{security,middleware,errors,settings,main}.py, routers/{accounts,product,health,jobs,models,common}.py, services/{accounts,privacy,alerts,reports}.py, storage/**, cli.py (nosec lines only), apps/gateway/** |
| B – identity lifecycle | idle timeout + session list/revoke/"sign out everywhere"; password hash versioning + rehash; temporary-password expiry; e-mail-dependent flows (verification, reset, invitation delivery) as a disabled, tested token service (`EMAIL_ENABLED=false`); cookie-session migration as a design only | Security | same as A + migration `0007_*` |
| C – reliability & operations | JOB-01/02, DATA-01..05, ML-01..05 (no retraining; disclosure for existing artefacts), restore CLI + rehearsal procedure, migration data-preservation tests for 0002–0006, readiness check for "DB newer than code" | Backend/data | backend/icp_api/services/{jobs,datasets,forecasts,explain,verification,provenance,scenarios,insights}.py, tasks.py, worker.py, insights/**, routers/{data,forecasts}.py, pipeline/**, migrations tests, cli.py (new subcommands only) |
| D – product, UX, accessibility | A11Y-01..07, UX-01..06, COPY-02..04, PERF-01/02, session-management UI (after B), UI for any new error codes | Frontend | apps/web/** |
| E – billing (sandbox only) | provider comparison; provider-neutral billing domain + Midtrans Snap adapter (signature check, amount/order/environment validation, idempotent event processing, finite state machine, reconciliation) behind `BILLING_ENABLED=false`, synthetic payload contract tests; no prices invented, no live calls | Backend/data | backend/icp_api/billing/** (new), migration `0008_*`, tests |
| CI & DevSecOps | CI-01 (prove/fix the CI data dependency), CI-02 (bandit `--ignore-nosec` once A lands), CI-03..05, SUP-01/05/07 (secret-scan coverage), audit documents | QA/DevSecOps | .github/workflows/**, scripts/**, backend/tests/conftest.py (data fixtures only), package.json version, docs/audits/**, docs/** |
| F – launch acceptance | full regression, threat-model update, restore rehearsal, residual risks, launch checklist; no deploy | QA + coordinator | docs/audits/** |

Rules for every wave: regression test first where practical → narrowest fix → focused tests → broader
suite → independent QA review → docs/contract together → evidence. Shared files (`schemas.py`,
`cli.py`) take additive edits only; contracts are regenerated once by the coordinator; the local
database (`var/`) is migrated only by the coordinator after `icp-api backup`. No Git commands, no
deployment, no remote migrations, no live payments or e-mail.

## Order

1. Waves A, C, D and the CI work run in parallel (disjoint files); QA verifies each.
2. Waves B and E start after A/C are verified (they add migrations 0007/0008 on top of 0006).
3. Wave F last.
