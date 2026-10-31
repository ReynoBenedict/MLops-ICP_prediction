# Remediation log (2026-10-20)

Status: CURRENT (final, wave F, 2026-10-20). One entry per change, newest wave last. Wave entries record the
implementers' claims; the **QA verdict per finding** (verified by QA vs claimed) is the status column of
[FULL_RISK_REGISTER.md](FULL_RISK_REGISTER.md) and the evidence is in [LAUNCH_ACCEPTANCE.md](LAUNCH_ACCEPTANCE.md).
IDs refer to [FULL_RISK_REGISTER.md](FULL_RISK_REGISTER.md).

Entry format: ID · files changed · what changed · verification (command → result) · verified by · status.

## Wave CI & DevSecOps (QA agent)

### CI-01 – CI runs the full suite from Git-tracked data
- Files: `backend/tests/conftest.py`, `pipeline/tests/conftest.py`, `.github/workflows/ci.yml`.
- Proof of the defect (before):
  - The Git index contains `data/raw/dataset.csv` and `data/seed/*.csv`. It does not contain `data/raw/wti.csv`,
    which is DVC-only (`data/.gitignore: /raw`). There is no DVC remote and no `dvc pull` in CI.
  - Simulating the clean checkout with `SEED_WTI_PATH=<absent>` and `pytest backend/tests` gave
    **2 failed, 899 passed, 39 errors**: KeyError 'wti', DATA_INSUFFICIENT and readiness `degraded`.
- Change:
  - Test fixtures seed from tracked files only: `SEED_ICP_PATH=data/raw/dataset.csv` and
    `SEED_WTI_PATH=data/seed/wti.csv` (EIA monthly series, same `period`/`date` reader).
  - `require_data()` skips when tracked data is missing, or **fails** when `ICP_REQUIRE_DATA=1`.
  - ci.yml sets `ICP_REQUIRE_DATA=1` for the check job.
- Verification:
  - Command: `ICP_REQUIRE_DATA=1 pytest -q -p no:cacheprovider`, with no reference to `data/raw/wti.csv` left in
    test paths (grep).
  - Result: **996 passed, 12 failed, 0 skipped** (4 min).
  - All 12 failures come from wave A's in-progress middleware edit
    (`AttributeError: 'Settings' object has no attribute 'gateway_accepted_secrets'` → 500s in the
    security/throttle tests). None is data-related: no KeyError 'wti', no DATA_INSUFFICIENT.
  - Re-run once wave A is consistent: pending.
- Status: Fixed locally (final full-suite confirmation pending).

### CI-02 – bandit without suppressions
- Files: `.github/workflows/ci.yml`.
- Change: the security job runs `bandit -r backend/icp_api pipeline/icp_ml -ll --ignore-nosec -q`, so inline
  `# nosec` markers no longer have any effect.
- Verification:
  - Before wave A: 13 Medium findings, exit 1.
  - After wave A (implementer's run, secvenv bandit 1.9.4): no issues, and 0 `# nosec` comments in either
    package. Independent re-run in the final QA verification.
- Status: Fixed locally (pending QA).

### SEC-10 (test side) – gateway simulator mirrors the data-plane allow-list
- Files: `backend/tests/conftest.py` (`GatewaySimulator`, `sql_skeleton`, `sql_rejection`, `MAX_SQL_PARAMS`),
  `backend/tests/test_v7_gateway_simulator.py` (new, 27 tests).
- Change:
  - The simulator now refuses exactly what the Worker refuses: one statement only;
    SELECT/INSERT/UPDATE/DELETE/WITH plus `PRAGMA table_info(<ident>)`; forbidden keywords outside literals
    and comments get 403. More than 100 bound parameters get 400.
  - Refused statements are recorded in `gw.rejected`.
  - The vectors are identical to `apps/gateway/test/v7-security.test.ts`.
  - The repository tests over the simulator (`test_storage.py`, gateway variant) therefore prove that the
    backend issues only statements the Worker accepts.
- Verification:
  - `pytest backend/tests/test_v7_gateway_simulator.py backend/tests/test_storage.py` → 50 passed.
  - `pytest backend/tests/test_v7_storage.py` → 10 passed.
  - ruff check and format are clean.
- Status: Fixed locally (pending QA).

### OPS-01 – restore rehearsal on a copy (2026-10-20)
- Script (session scratch, not in the repository): `ux7/q/scripts/restore_rehearsal.py`.
- Evidence: `ux7/q/restore-rehearsal-20261020T025442Z.json`, plus its `.log` and its directory's `server.log`.
- Steps and results:
  1. **Copy:** `var/backups/20261020T024313Z` was copied to a fresh scratch directory outside `var/`: 625 files,
     36,514,038 bytes, 0.8 s. The `metadata.db` sha256 equals the source's.
  2. **Integrity:** opened read-only. `PRAGMA integrity_check` returned ok for both metadata and MLflow; schema
     version 6.
     - Row counts match the stored restore-check output: forecast_runs 122, audit_events 552, users 26,
       sessions 143, and so on.
  3. **Serve:** an API process ran against the copy with `APP_ENV=test` (no `.env` loaded), `MODEL_SOURCE=bundle`,
     port **18765**, a throwaway operator token generated in memory, and no repository seeding.
     - It was live after 4.2 s.
     - On start it applied the newer migration `0007_identity_lifecycle` to the copy only, so system status
       reports schema 7/7.
  4. **Reads:**
     - `/health/ready` → 200 `ready`, every check `ok`: metadata_store, schema_version, object_store,
       model_bundle, production_model, datasets, job_runner.
     - `/api/v1/system/status` → 200 (bundle, sqlite).
     - `/api/v1/public/market-snapshot` → 200, 2026-09 = **112.81**, **identical** to the live dev API.
     - `/data/summary`, `/datasets/active`, `/models`, `/forecasts/latest` (model v5) and `/data/verification`
       → all 200.
  5. **Stop:** the process was terminated after 6.8 s in total.
  6. **Source untouched:** the source backup's checksum and file tree were unchanged afterwards. The live
     `var/` was not written.
- Measured RTO for the local topology is about 7 s, plus the operator's time. The RPO equals the backup age.
- Finding **OPS-08:** one MLflow experiment's `artifact_location` is an absolute path into the live `var/`, so
  the MLflow store is not relocatable without rewriting it.
- Not rehearsed:
  - D1 Time Travel or `wrangler d1 export` import: remote operations are prohibited.
  - KV restore.
  - An MLflow-mode restore.
- Status: Fixed locally (pending QA). The deployed-store restore stays an owner task.

### Documentation (wave A follow-up)
- `docs/SECURITY.md`:
  - client-IP header (SEC-01); edge limiter and aggregation of `auth.failed` rows (SEC-02);
  - registration timing and the `EMAIL_UNAVAILABLE` invite code (SEC-04);
  - admin boundaries (SEC-03), plan re-checks (SEC-11) and the password-change throttle (AUTH-05);
  - Cf/Cc neutralisation (SEC-14), Worker error headers (SEC-09) and the data-plane allow-list (SEC-10);
  - explicit `API_HOST`, rotation via `GATEWAY_ACCEPTED_SECRETS`, quota reservation and refund (SEC-07),
    and the 5-minute touch interval;
  - the export field list (SEC-12) and requester-only withdrawal (SEC-05);
  - the `nosec` description was replaced by `--ignore-nosec` and the secret-scan description;
  - Known gaps updated.
- `docs/THREAT_MODEL.md`: acceptance criterion 2 (bandit `--ignore-nosec`, 0 `# nosec`), the data-plane row
  (allow-list and rotation; remaining risk) and the throttle row (SEC-01).
- `docs/OPERATIONS.md`: new "Rotating the gateway shared secret" runbook; incident containment links to it;
  `restore-check` usage and the rehearsal record.
- `docs/ENVIRONMENTS.md`: `API_HOST` and `GATEWAY_ACCEPTED_SECRETS` rows (matrix, validation, Worker table);
  the seed rows now point at `data/seed/wti.csv`.
- `docs/STORAGE.md`: KV export retention by `expirationTtl` (180 d / 365 d, new objects only) and the Deletion
  bullet.

### CI-03 – pinning
- Files: `.github/workflows/ci.yml`.
- Change:
  - `actions/checkout`, `actions/setup-python` and `actions/setup-node` are pinned to commit SHAs. They were
    resolved on 2026-10-20 from the public GitHub API (`api.github.com/repos/<repo>/git/ref/tags/<tag>`; all
    three are lightweight tags pointing to commits), and the tag is kept in a comment.
  - pip is pinned via `PIP_VERSION=26.2.1`.
  - The job-level `--upgrade pip` was removed.
- Open (owner):
  - Confirm the SHAs on each action's release page.
  - Enable Dependabot or Renovate (a repository setting).
  - Add an image build and scan job.
- Status: Fixed locally (partly).

### CI-04 / CI-05 – continuous-training workflow
- Files: `.github/workflows/continuous-training.yml`.
- Change:
  - The `schedule` was removed, so the workflow is manual-dispatch only.
  - New `train` input, default `false`.
  - The workflow reads `/api/v1/system/status` and runs `train` only when `model_source=mlflow` and
    `train=true`. Otherwise it prints the operator-local retraining procedure (train locally, then
    `icp-api export-model`).
  - All inputs are passed through `env:` (no `${{ inputs.* }}` inside `run:`).
- Verification: both workflow files parse with PyYAML; triggers are `[workflow_dispatch]` and `[push, pull_request]`.
  The workflows were not run, because triggering them is prohibited.
- Status: Fixed locally.

### SUP-05 – SBOM
- Files: `package.json` (`"version": "2.0.0"`), `.github/workflows/ci.yml` (new SBOM step).
- Verification:
  - `npm sbom --sbom-format cyclonedx --omit dev` → CycloneDX 1.5, 51 components.
  - `--package-lock-only` (as in CI) → CycloneDX 1.5, 48 components.
  - `pip-audit … -f cyclonedx-json` → CycloneDX 1.4, 134 components.
  - All outputs were written to a temporary folder outside the repository.
- Status: Fixed locally.

### SUP-07 – secret scan
- Files: `scripts/secret-scan.mjs`.
- Change:
  - 19 rules: AWS, private keys, GitHub, Slack, Stripe secret and webhook keys, Midtrans server keys, Xendit,
    Google, npm, Docker Hub, Render, Resend, SendGrid, JWT, Cloudflare token assignments, connection strings, and
    quoted or unquoted credential assignments with a Shannon-entropy threshold.
  - Placeholders are exempt per value, never per line, so `<Field token="real">` is reported.
  - It now scans `apps/web/dist` and `apps/gateway/dist`, plus `.sh`, `.ps1`, `.pem`, `.key` and `.cjs` files.
  - It never opens `.env*` or `.dev.vars*` (except the `.example` files), and checks that `.gitignore` covers
    `.env`, `.env.*` and `.dev.vars`.
  - `--self-test` runs 17 positive and 13 negative synthetic samples, built at run time so the file contains no
    literal credential. The self-test also runs before every scan.
  - Output names the file, line and rule only.
- Verification:
  - `node scripts/secret-scan.mjs --self-test` → all samples behave as expected.
  - `node scripts/secret-scan.mjs` → no findings.
  - The first run produced 2 false positives: the event name `icp:password-change-required` and an f-string
    `tester:{ADMIN_TOKEN}`. Both are now handled by value rules and covered by self-test samples.
- Residual: no Git-history scan.
- Status: Fixed locally.

### DATA-06 – WTI seed defaults (image and docs)
- Files: `infra/docker/api.Dockerfile`, `docs/DEVELOPMENT.md`.
- Facts (wave C):
  - The active WTI dataset v3 and production model v5 use the EIA monthly mean. That series is byte-identical to
    `data/seed/wti.csv`.
  - The legacy `data/raw/wti.csv` fed only the inactive seed version v1.
- Change:
  - The image copies `data/raw/dataset.csv` and `data/seed/` and sets `ENV SEED_WTI_PATH=/app/data/seed/wti.csv`.
    The legacy file is no longer copied.
  - Nothing else in the image reads `data/raw`. Checked by grep:
    - `settings.seed_icp_path` → `data/raw/dataset.csv`, which is copied.
    - The pipeline CLI's `DEFAULT_ICP` uses the same file.
    - `ingestion/wti.py` is the legacy offline writer and is not called by the API.
    - `esdm --local` reads PDFs that `.dockerignore` excludes anyway.
  - DEVELOPMENT.md "First setup" no longer tells users to override `SEED_WTI_PATH`.
- The settings default change has landed: wave A changed `settings.seed_wti_path` to `data/seed/wti.csv`
  (`test_wti_seed_default_is_the_maintained_series`). The image, settings, pipeline CLI, tests and docs now agree.
- Open: `README.md:235-240` repeats the old override advice. README is outside the docs scope, so the coordinator
  should update it.
- Status: Fixed locally (pending QA).

### DATA-03 – storage docs
- Files: `docs/STORAGE.md`.
- Change: the "Downloads" text is replaced with the wave C wording: exports are tenant data, sent with
  `private, no-store`, and never edge-cached. A new retention paragraph covers the zero-cost KV topology: no
  lifecycle and no TTL, 1 GB storage, 1,000 writes/day, and manual pruning after a backup or an optional
  `expirationTtl`, which is not implemented.
- Status: Fixed locally (docs).

## Wave A – security & integrity (merged from notes-A.md, 2026-10-20; implementer's claims, QA verification pending)
- Files:
  - Backend: `security.py`, `middleware.py`, `settings.py`, `main.py`, `routers/{accounts,product,jobs}.py`,
    `services/{accounts,privacy,reports}.py`, `storage/{product,repository}.py`, and in `cli.py` only the
    clone-state block plus a constant.
  - Gateway: `apps/gateway/src/index.ts`.
  - Tests: new `backend/tests/test_v7_security.py` (31), new `apps/gateway/test/v7-security.test.ts` (8), one
    env line in `test_api.py`, one assertion in `gateway.test.ts`.
  - The tests were written in the same pass as the fixes. They were not run against pre-fix code (no git); the
    pre-fix behaviour is documented by audit 1's reproductions.
- **SEC-01:**
  - The Worker sends `X-ICP-Client-IP`; the backend prefers it, uses gateway headers only when the secret
    verifies, and ignores values that are not IPs.
  - Tests: `test_sec01_*` (the attacker gets 429 while the owner behind the same Worker egress gets 200) and the
    gateway "SEC-01".
  - Hosted confirmation pending.
- **SEC-02:**
  - The edge always applies the IP bucket; the per-credential bucket only for well-formed tokens.
  - The backend has an `auth_failed` per-client bucket before any write, and `AuthFailureAudit` writes 1 row per
    client per minute.
  - The router-level `default` limit covers `/auth/*`, `/org*`, `/admin/*`, `/jobs*` and `/audit`.
  - Test: 8 junk tokens → 6×401, 2×429, +1 audit row.
- **SEC-03:** the owner is required to reset, re-role or disable an admin. Test: `test_sec03_*`.
- **SEC-04:**
  - Registration of a taken address now does two scrypt operations, the same work as a success.
  - Invitations answer a generic `409 EMAIL_UNAVAILABLE` and are in the `auth` bucket.
  - Residual: the registration 409 still reveals the address.
- **SEC-05:** only the requester can withdraw; anyone else gets 404.
- **SEC-07:** atomic conditional usage reservation, refunded on 401/403/429. Tests include 20 threads against a
  limit of 5, which gives exactly 5 rows.
- **SEC-09:** the Worker's own error replies carry the security headers (404/405/413/429/502/503/504 tested).
- **SEC-10 / OPS-04:**
  - SQL allow-list on the data plane: one statement; refused statements get 403 and never reach D1.
  - `GATEWAY_ACCEPTED_SECRETS` on both sides, compared in constant time; each entry must be at least 32
    characters when deployed.
  - Residual: one secret serves both directions.
- **SEC-11:** the plan feature is re-checked on every scenario and alert write path and on scenario-comparison
  reports (9 paths → 403).
- **SEC-12:** the export field list (SECURITY.md); `resolved_by` is redacted in the customer APIs.
- **SEC-13:** parameter-less report kinds refuse non-empty params with 422.
- **SEC-14:** `cell_safe` skips leading Cc/Cf/Zs/Zl/Zp and always prefixes a leading control character.
- **SEC-15:**
  - Member helpers raise 403 instead of asserting; the request-id format is regression-tested.
  - The assert at `routers/forecasts.py:250` belongs to wave C and is still open.
- **AUTH-02:** NFKC + casefold. **AUTH-03:** registration race → 409 with a compensating delete of the empty
  organisation. **AUTH-05:** per-user throttle on password change.
- **SQL-01:** constant statements and maps everywhere; no `0.0.0.0` literal (`API_HOST` is required when
  deployed); 0 `nosec`; `test_sql01_*`.
- **OPS-07:** session and API-key "last used" timestamps are touched at most every 5 minutes.
- Wave C requests done by wave A:
  - The gateway forwards `idempotency-key`.
  - `MAX_SQL_PARAMS = 100`.
  - Report `issue_date` uses `jakarta_today()`; the PDF footer says WIB.
  - `Idempotency-Key` on `POST /reports` (`test_v7_report_requests.py`, 3 tests).
  - `seed_wti_path` defaults to `data/seed/wti.csv` (DATA-06).
  - KV `expirationTtl` for exports.
- New error codes (frontend titles): `EMAIL_UNAVAILABLE`; `IDEMPOTENCY_KEY_REUSED` on `/reports`; `CONFLICT` on
  registration; 403 `FORBIDDEN` for admin-on-admin actions; `RATE_LIMITED` on password change;
  `FEATURE_NOT_IN_PLAN` on more paths.
- Implementer gates:
  - ruff and format are clean (157 files); mypy reports no issues (80 files).
  - bandit `-ll --ignore-nosec`: Medium 0, High 0.
  - Full pytest: **1095 passed** (250 s). Gateway: 33 passed.
  - The coordinator restarted the dev API and regenerated contracts.
- Note: `test_access_control` showed 2 failures only with a non-alphabetical file order (pre-existing order
  dependence). This should be checked in the final verification.
- Status: Fixed locally (pending QA).

## Wave B – identity lifecycle (merged from notes-B.md, 2026-10-20; implementer's claims, QA verification pending)
- Files:
  - New: migration `0007_identity_lifecycle.sql` (additive: `users.temp_password_expires_at`,
    `users.email_verified_at`, `email_tokens` table plus an index), `backend/icp_api/services/email_tokens.py`,
    and `backend/tests/test_v7_identity.py` (13 tests).
  - Changed: `services/accounts.py`, `routers/accounts.py` (9 endpoints), `security.py`, `settings.py`,
    `main.py`, `schemas.py` (additive), `storage/product.py`, and `cli.py`. In `cli.py`, clone-state skips
    `email_tokens`.
  - `test_security_api.py` lists the 4 new public token routes.
  - The tests were written in the same pass as the code; all of the behaviour is new.
- **SEC-06 (part):**
  - Idle timeout `SESSION_IDLE_TIMEOUT_HOURS` (default 168 h, 0 = off). An idle session is revoked server-side.
  - New `GET /auth/sessions` (device label, no token and no raw user agent).
  - New `POST /auth/sessions/{id}/revoke` (404 for another user's session) and `POST /auth/sessions/revoke-others`.
  - Cookie migration: design only, now in SECURITY.md.
  - The legacy `sessions` array in `/auth/me` remains until the UI switches over.
- **AUTH-01:**
  - New hash format `scrypt$ln=15,r=8,p=1$salt$digest`. Legacy hashes still verify and are rehashed on sign-in.
  - Absurd or malformed parameters are refused.
  - Tests: legacy and ln=14 hashes are upgraded.
- **AUTH-04:**
  - Temporary passwords expire after `TEMP_PASSWORD_TTL_HOURS` (default 72, range 1–720). A correct but expired
    password gets `403 TEMP_PASSWORD_EXPIRED`; a wrong one still gets 401. Sessions opened with it end.
  - Responses carry `expires_at` / `temporary_login_expires_at`.
- **MAIL-01 (technical part):** disabled e-mail token service.
  - `EMAIL_ENABLED=true` is refused at start-up. The sender is a no-op.
  - Six endpoints answer `503 EMAIL_DISABLED` before any read or write (test: 0 token rows).
  - Tokens: SHA-256 only, single use, superseded, TTL 1 h / 48 h / 7 days, voided on anonymisation.
  - Residual: reset-request timing differs for known addresses once a provider sends synchronously.
- **New codes:** `EMAIL_DISABLED`, `TOKEN_INVALID`, `TEMP_PASSWORD_EXPIRED`, `EMAIL_ALREADY_VERIFIED`,
  `MEMBER_ALREADY_ACTIVE`; session `NOT_FOUND`.
- **Migration 0007:** additive. It is covered by the step-by-step preservation test and
  `test_migration_0007_is_additive_and_keeps_every_account_row`.
- **Implementer gates:**
  - ruff, format and mypy are clean (89 files); bandit `-ll --ignore-nosec` reports no issues.
  - Full pytest: **1169 passed** (471 s). Gateway: 33 passed.
  - The `test_access_control` order dependence (only in a non-alphabetical order) is pre-existing and still open.
- **Coordinator:**
  - Backup `var/backups/20261020T032539Z` was taken; restore-check ok.
  - The local `var/` was migrated to schema 8 (integrity ok; the only row difference is `app_schema`).
  - The dev API was restarted and the contracts regenerated (89 paths).
  - Live probes: `/auth/password-reset/request` → 503 `EMAIL_DISABLED`.
- Status: Fixed locally (pending QA).

## Wave C – reliability, data, ML (merged from notes-C.md, 2026-10-20; implementer's claims, QA verification pending)
- Files:
  - Backend services and routers: `services/jobs.py`, `worker.py`, `storage/sql.py` and `storage/objects.py`
    (granted hunks), `routers/health.py` (granted hunk), `routers/forecasts.py`, `services/forecasts.py`,
    `services/insights.py`, `services/explain.py`, `insights/context.py`.
  - `schemas.py`, additive only. **The contract must be regenerated.**
  - New `restore_check.py`, plus a new `cli.py restore-check` subcommand.
  - Pipeline: `backtest.py`, `evaluation.py`, `bundle.py`, `cli.py`, `sources/base.py`. `jakarta_today()` is used
    in `bi_jisdor.py`, `worldbank_cmo.py`, `forecasting.py` and `schema.py`.
  - New tests: `test_v7_jobs` (6), `test_v7_storage` (9), `test_v7_migrations` (3), `test_v7_evaluation` (13),
    `test_v7_idempotency` (6), `test_v7_restore_check` (5), `pipeline/tests/test_v7_coverage` (4) and
    `test_v7_data_defaults` (1). Five ML-05 tests are appended to `pipeline/tests/test_bundle.py`.
- **JOB-02:**
  - A queued job waiting for the runner slot now heartbeats from its own thread and from `reap()`.
  - Queued jobs of a dead process are still reaped.
  - The test failed before the fix.
  - Optional: wave A may move the UPDATE into the repository.
- **JOB-01:**
  - Heartbeat failures are tolerated for `job_stale_after_seconds/2`; after that the worker is terminated
    (`RUNNER_STORAGE_ERROR`).
  - Any escaping exception terminates the subprocess.
  - `_run` records `RUNNER_ERROR`.
  - The worker refuses to run a job that is not `running`.
- **DATA-01:**
  - D1 and gateway writes retry only on 429 and connection errors. 502/503/504 raise an "may or may not have been
    applied" error. Reads keep all retries.
  - A KV 412 on a retried PUT with identical bytes counts as success.
  - `submit` adopts its own inserted job.
  - S3 is unchanged.
  - The tests were written right after the code, not red-first, and would fail on the old behaviour.
- **DATA-02 (/forecasts):**
  - `Idempotency-Key` header; scope is the principal + org within 24 h; only succeeded runs are replayed.
  - A replay returns `Idempotent-Replayed: true` and records no new usage row. A different body with the same key
    returns 409 `IDEMPOTENCY_KEY_REUSED`.
  - Stored in `params_json`, so no migration is needed.
  - Limitation: two concurrent first requests with the same key can both run (no unique index).
  - The gateway must forward the header (wave A request 1). `/reports` is wave A request 4.
- **ML-01:**
  - Embargoed `split_coverage`.
  - At read time, embargoed coverage is recomputed from the stored predictions (`coverage_embargoed`,
    `coverage_check`); warnings use the stricter figure.
  - Disclosure: models exported before 2026-10-20 stored coverage without an embargo (v5 hold_last h6 50%: 0.739
    stored vs 0.435 embargoed).
  - No retraining.
- **ML-02:** a level is flagged when its shortfall is ≥ 10 points **or** its miss rate exceeds 1.5× nominal. The
  effective 95% threshold is 0.925, so the v5 hold_last h1 95% cell (0.857) is now flagged.
- **ML-03:** the evaluation and disclosure notes (id/en) now mention selection optimism.
- **ML-04:** `jakarta_today()` (fixed UTC+7). `reports.py:318` is still pending (wave A request 3).
- **DATA-04:**
  - Every table is filled at each migration step 0002…0006 and every row and value must survive.
  - Fresh-install parity is checked.
  - New non-critical readiness check `schema_version`: `degraded` when the database is newer than the code.
- **ML-05:**
  - A sha256 manifest in `meta.json`. Re-exports compare bytes and refuse differences without `--overwrite`.
  - Everything is verified before the pointer is written; the pointer carries hashes.
  - `load_model` verifies the payload; legacy bundles without a manifest still load.
- **DATA-05:**
  - Key regex `\Z`; `clip_export` fails closed.
  - D1 limit verified on developers.cloudflare.com (100 bound parameters). The gateway still allows 200 (wave A
    request 2).
- **OPS-01 (part):** `icp-api restore-check <backup-dir> [--live-db]` is read-only. On
  `var/backups/20261019T232744Z` it reported:
  - ok, but the backup is **schema v5** while live is v6;
  - 11/11 dataset objects verified; the v5 bundle has no manifest;
  - the live database has more rows in 13 tables.

  **Take a fresh backup before any migration or rehearsal.**
- **DATA-06:** investigated, and the pipeline CLI default fixed (see above).
- Implementer gates:
  - ruff and format pass; mypy has no issues (80 files); bandit `-ll --ignore-nosec` has no issues.
  - Full pytest: **1,086 passed** (355 s).
  - verify-provenance: 93 months, 0 disagreements, Sep 2026 = 112.81.
- Status: Fixed locally, pending QA verification and contract regeneration.

## Wave D – frontend (merged from notes-D.md, 2026-10-20; implementer's claims, QA verification pending)
- Files:
  - `apps/web/src/api/client.ts`, `components/ui.tsx`, `pages/AuthPages.tsx`, `pages/AccountPage.tsx`, `auth.tsx`,
    `pages/LandingPage.tsx`, `lib/plan.ts`, `format.ts`, `api/product-types.ts`, `components/forecast.tsx`,
    `pages/ExplorerPage.tsx`, `App.tsx`, `styles-brand.css`, `i18n/{common,account,landing,forecast}.ts`.
  - Tests: new `wave-d.test.tsx` (21); `pages.test.tsx` now has a stronger login contract; flake fixes in
    `ops.test.tsx` (preloaded lazy modules, explicit 5 s window) and `flows.test.tsx` (explicit 8 s navigation
    window). Assertions are unchanged.
- **COPY-02/UX-01:**
  - The server message is shown when it is in the active language.
  - New codes TIMEOUT, OFFLINE, EMAIL_TAKEN and SELF_EDIT.
  - Credential errors show no sign-in link.
  - The request id is shown for 4xx (except 401/404); there is no retry on 403.
- **A11Y-01:** loading buttons stay focusable (`aria-disabled`).
- **A11Y-02:** `Field` with `aria-invalid`/`aria-describedby`, focus on the first invalid field, and a polite live
  summary.
- **A11Y-03:** screen-reader text "(tidak termasuk)" on excluded plan features.
- **A11Y-04 / UX-02 / UX-03:**
  - Labelled role select with "Terapkan" and a ConfirmDialog.
  - ConfirmDialog for disabling a member, resetting a password and revoking a key.
  - Own row detected by user id or e-mail; button names include the member or key name.
- **A11Y-05..07:**
  - Targets at least 24 px (language toggle 36×28).
  - Action column header "Tindakan".
  - One-time secrets in live regions.
- **COPY-03/04:**
  - Thousands separators; WIB-labelled times; WIB `currentPeriod`; localised period codes in server warnings.
  - New mixed-scenario assumption sentence; a single toast after a password change.
- **UX-04..06:** timeout and offline errors are distinguished; CSV download is disabled when empty; the `/operasi`
  title no longer leaks the area.
- **PERF-01:** public `/plans` and market snapshot are cached in memory for 5 minutes (anonymous only), which removes
  the `/plans` waterfall.
- **PERF-02:** not split further (shared vendor chunk); accepted.
- **TEST-02:** `ops.test.tsx` passed 8/8 isolated and 8/8 runs with 4 parallel suites. The full web suite passed
  12/12 runs at 3 parallel suites. Rare `findBy` timeouts at 4–5 parallel suites were not masked.
- Implementer gates:
  - lint, typecheck and build OK.
  - Vitest: 13 files, 123 tests, all passed.
  - Entry about 173 kB gzipped.
  - Browser evidence in the session folder `ux7/evidence/INDEX-D.md`.
- Open: dedicated titles for new wave A error codes; feature flags in `whoami`.
- Status: Fixed locally, pending QA verification.

## Wave E – billing sandbox (merged from notes-C.md "Wave E", 2026-10-20; implementer's claims, QA verification pending)
- Files:
  - New: `backend/icp_api/billing/{__init__,config,domain,midtrans,store,service,router,cli}.py`, migration
    `0008_billing_sandbox.sql` (additive: `billing_orders`, `billing_events`), and `backend/tests/test_v7_billing.py`
    (25 contract and flow tests).
  - `test_v7_migrations.py` gains one test and now walks 0001→0008.
  - `cli.py` adds `billing-reconcile` (additive).
  - The coordinator wired `billing_router` into `main.py` and added `Idempotency-Key` / `Idempotent-Replayed` to
    the CORS headers.
- **BILL-02 (sandbox scope):**
  - Order state machine: created → pending → paid → refunded; failed, expired and cancelled are terminal.
    Illegal moves are recorded as `ignored_transition`.
  - Webhook checks, in order: enabled and configured → SHA512 signature compared in constant time → known
    order → environment → amount and currency → idempotent event log (UNIQUE) → compare-and-set status.
  - Snap adapter uses sandbox URLs only; the request body is built only from the stored order.
  - `billing-reconcile` is a dry run with exit codes 0/1/2 and no database writes (tested).
  - Every endpoint answers `503 BILLING_DISABLED` while disabled; production is refused.
- **Honest gaps:**
  - The `SB-Mid-server-` sandbox key prefix is an observed convention. It is not stated on Midtrans' access-keys
    page; the owner should confirm it.
  - No automatic plan activation on `paid`, no production URLs or keys, no refund initiation, no customer UI,
    no expiry of stale `created` orders.
- **Test secrets:** obviously fake values. The secret scan exempts values that spell out fake/dummy/
  "not-a-secret" (QA change, see SUP-07).
- **Implementer gates:**
  - ruff, format, mypy and bandit (`-ll --ignore-nosec`, plus all severities on `billing/`) are clean.
  - The full run showed 2 transient failures in wave B's area while wave B was still editing; both passed on
    rerun. The final full run is owed in wave F.
- **Coordinator live probes:** `/billing/status` and the Midtrans webhook → 503 `BILLING_DISABLED`.
- **Docs (QA):** new `docs/BILLING.md`; API.md billing section; ENVIRONMENTS.md billing and e-mail settings;
  SECURITY.md webhook paragraph; COMMERCIAL_READINESS.md rows; LEGAL_AND_PRIVACY.md "Not used" facts.
- Status: Fixed locally for sandbox (pending QA). Live billing remains an owner decision.

### Documentation (waves B/E follow-up, QA)
- `docs/ENVIRONMENTS.md`: new "Identity, e-mail and billing settings" table (names and defaults only).
- `docs/SECURITY.md`: hash versioning, temporary-password expiry, idle timeout and the session
  list/revoke endpoints, the disabled e-mail token design, billing webhook verification, the cookie-session
  migration design (new section) and updated Known gaps.
- `docs/BILLING.md` (new; added to the docs index together with `audits/`).
- `docs/API.md`: session endpoints, e-mail endpoints (503 while disabled), invitation e-mail,
  `EMAIL_UNAVAILABLE`, admin-on-admin `403`, requester-only withdrawal, `Idempotency-Key` convention, report
  params `422`, and the billing section.
- `docs/DEPLOYMENT.md` pending section:
  - back up D1 first, then apply **0005–0008**;
  - image and seed change;
  - required `API_HOST`/`METRICS_TOKEN`, and `EMAIL_ENABLED` / `BILLING_ENABLED` kept false;
  - Worker redeploy contents and the SEC-01 hosted check.
- `docs/COMMERCIAL_READINESS.md` (billing and e-mail rows) and `docs/LEGAL_AND_PRIVACY.md` (implementation facts:
  sessions, temporary passwords, e-mail tokens, export redaction, requester-only withdrawal, disabled e-mail
  and billing).
- Not in scope:
  - `apps/web/src/pages/legal-content.ts` (frontend) states the in-app privacy facts. It should be checked
    against the idle timeout, temporary-password expiry and export changes.
  - Flagged for the frontend owner.

## Wave F – launch acceptance (QA, 2026-10-20)
- **Full gate:** `npm run check` run 2 → **exit 0, 340 s** (pytest 1169 passed / 0 skipped, web 144, gateway 33,
  lint/mypy/contracts/build/secret scan). Run 1 was killed by a session interruption (2 `WORKER_CRASHED` in
  job-subprocess tests); both re-checked: the jobs test passes 3/3 in isolation, the other is order-dependent
  (TEST-03). Logs: `docs/evidence/2026-10-20-production-readiness/`.
- **Scans:** bandit 1.9.4 `-ll --ignore-nosec` no issues, 0 `# nosec`; pip-audit 2.10.1 no known vulnerabilities;
  npm audit not re-run (needs the registry; 0/0 earlier the same day).
- **Independent checks (QA's own scripts, not implementer tests):** 17/17 TestClient spot checks (SEC-03, SEC-05,
  SEC-12, SEC-06 idle/revoke, AUTH-04 expiry field, billing webhook signature/amount/environment/replay/late
  status/no plan change, `EMAIL_ENABLED=true` refused); 11/11 data-plane checks against the bundled real Worker;
  restore rehearsal (OPS-01).
- **Test-robustness review:** wave D waits judged legitimate (module transform under load, assertions
  unchanged); TEST-03 order dependence reproduced (`test_api.py` before `test_access_control.py` → 2 failures;
  `test_product.py::test_public_snapshot…` alone → 3 points instead of 4) and reported, not fixed.
- **Change by QA:** `scripts/secret-scan.mjs` fake/dummy/"not-a-secret" exemption restricted to test files, with
  self-test cases both ways.
- **Recommendation:** READY FOR STAGING VERIFICATION; not ready for production (owner gates: hosting, billing,
  e-mail, legal, monitoring, off-site backups).
