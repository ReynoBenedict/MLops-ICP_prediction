# Evidence: production-readiness audit v7 (2026-10-20)

This folder supports [../../audits/LAUNCH_ACCEPTANCE.md](../../audits/LAUNCH_ACCEPTANCE.md).
- Everything here was captured locally by QA on 2026-10-20 (wave F), except the screenshots, which the
  frontend agent captured during wave D and QA reviewed.
- Accounts are throwaway `*@uji-lokal.test` accounts in organisations named "(uji lokal v7)".
- No file shows a real token, password or secret.
- Synthetic test tokens printed by pytest failure output were replaced with `<redacted>`.

| File | What it shows |
|---|---|
| `npm-run-check.log` | Run 2 of `npm run check`: **exit 0, 340 s**. ruff/format, mypy (89 files), ESLint/tsc, contracts up to date, pytest **1169 passed, 0 skipped**, web 144 passed, gateway 33 passed, builds, secret scan clean |
| `npm-run-check-run1-interrupted.log` | Run 1, killed by a session interruption during pytest: 2 `WORKER_CRASHED` failures in job-subprocess tests (see LAUNCH_ACCEPTANCE note A). Kept for honesty; tokens redacted |
| `bandit-ignore-nosec.txt` | bandit 1.9.4 `-r backend/icp_api pipeline/icp_ml -ll --ignore-nosec`: no issues; 0 `# nosec` markers |
| `pip-audit.txt` | pip-audit 2.10.1 on `requirements/constraints.txt` (`--no-deps --disable-pip`): no known vulnerabilities |
| `checks/security-spotcheck.py`, `checks/security-spotcheck.json` | Independent TestClient checks on a temporary database (17/17 pass): admin vs peer admin, deletion withdrawal, export contents, idle timeout and session revocation, temporary-password expiry, billing webhook (signature, amount, environment, replay, late status, no automatic plan change), `EMAIL_ENABLED=true` refused. Operator token and sandbox key are generated at run time and never printed |
| `checks/dataplane-check.mjs`, `checks/dataplane-check.json` | The real Worker (`apps/gateway/src/index.ts`, bundled with the repo's esbuild) against a fake D1: DDL, PRAGMA writes, ATTACH, BEGIN and stacked statements get 403 without reaching D1; a missing secret gets 401; `SELECT password_hash` is still allowed (documented residual SEC-10) |
| `checks/restore_rehearsal.py`, `checks/restore-rehearsal.json` | OPS-01 rehearsal on a copy of backup `20261020T024313Z`: integrity ok, schema 6, API live in 4.2 s, readiness all ok, snapshot identical to live (Sep 2026 = 112.81), 5 read endpoints 200, source backup unchanged |
| `checks/ci01-clean-checkout-simulation.txt` | Before the CI-01 fix: backend tests with `data/raw/wti.csv` absent (as in a clean CI checkout) gave 2 failed, 39 errors |
| `screenshots/D02-register-390-email-taken.png` | Registration with a taken address: field-level Indonesian message; password masked |
| `screenshots/D06-account-1280-reset-confirm.png` | Admin password reset behind a confirmation dialog (UX-02) |
| `screenshots/D14-account-1280-invite-email-unavailable.png` | Invitation of an existing address: generic `EMAIL_UNAVAILABLE` (SEC-04) |
| `screenshots/D15-account-1280-password-paused.png` | Password change throttled after repeated wrong current passwords (AUTH-05) |
| `screenshots/D16-sessions-1280-two-devices.png` | Active sessions list with absolute and idle expiry in WIB (SEC-06) |
| `screenshots/D17-sessions-1280-revoke-confirm.png` | Ending one other session behind a confirmation dialog |
