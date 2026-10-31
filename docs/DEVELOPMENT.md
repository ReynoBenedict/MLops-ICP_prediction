# Local development, testing and CI

Authoritative guide for working on the code locally. Deployment is in [DEPLOYMENT.md](DEPLOYMENT.md),
configuration in [ENVIRONMENTS.md](ENVIRONMENTS.md), day-to-day operation in [OPERATIONS.md](OPERATIONS.md).

## Prerequisites

- Node.js 22.12 or newer (npm workspaces: `apps/web`, `apps/gateway`, `packages/contracts`).
- Python 3.10, 3.11 or 3.12; 3.11 is recommended (CI and the API image use it). The project venv is
  `.venv`; run Python through `node scripts/py.mjs …` (it picks the venv interpreter on every OS).
  Pillow 13 needs Python 3.11 or later, so `requirements/constraints.txt` gives a 3.10 venv Pillow 12.3.0,
  which has known advisories (accepted residual risk DEP-01 in
  [audits/RESIDUAL_RISKS.md](audits/RESIDUAL_RISKS.md): the app decodes no untrusted images).
- Optional: Docker (staging image), Tesseract OCR (scanned ESDM decrees).

## First setup

WTI is seeded from the Git-tracked `data/seed/wti.csv` (EIA RWTC monthly mean, byte-identical to the active
WTI dataset version and to the series production model v5 was trained on). The DVC-only legacy
`data/raw/wti.csv` has different values and is not used by default (audit v7 DATA-06); `SEED_WTI_PATH`
remains available to override the seed file.

```bash
npm install
npm run setup
```

`npm run setup` (`scripts/setup.mjs`) creates `.venv`, generates local secrets into `.env` and
`apps/gateway/.dev.vars` (both gitignored), applies migrations, seeds datasets, trains and promotes a
first model (1-3 minutes; `-- --skip-train` skips it), generates the contract types and builds the web
app. A fresh setup seeds ICP from the original repository CSV; the reconciled official series needs
the ESDM ingestion (`node scripts/py.mjs -m icp_api.cli ingest icp --full`, slow, needs Tesseract).

## Running the stack

| Command | What runs |
|---|---|
| `npm run dev` | FastAPI on `http://127.0.0.1:8000` (auto-reload) + Vite on `http://localhost:5173`; Vite proxies `/api` and injects the local gateway secret |
| `npm run dev:edge` | Backend + production web build behind the real Worker in `wrangler dev` (`http://localhost:8787`) |
| `npm run stop` | Stops processes recorded in `var/dev.pids` |
| `npm run api` | Backend only, with reload |
| `npm run backup` | Consistent copy of `var/` stores into `var/backups/<UTC>/` (see [OPERATIONS.md](OPERATIONS.md#backups-and-restore)) |
| `npm run mlflow:ui` | MLflow UI on port 5000 |
| `node scripts/py.mjs -m icp_api.cli --help` | Operator CLI (migrate, seed, ingest, export-model, clone-state, backup, new-token, …) |
| `node scripts/py.mjs -m icp_ml.cli backtest` | Rolling-origin backtest of every configured method |

Sign-in for development: business users register an organisation from the landing page; the
operator token is in `.env` (`ADMIN_API_TOKENS`, `name:token`) and is entered on the operator tab of
the sign-in page. Never paste tokens into documents, tickets or screenshots.

### Windows notes

- uvicorn's reloader does not restart the worker when the process has no attached console (IDE
  run configurations, background shells). Run without `--reload`
  (`.venv/Scripts/python -m uvicorn icp_api.main:app --port 8000`) and restart by hand, or use an
  interactive terminal. See [OPERATIONS.md](OPERATIONS.md#common-problems).
- `MLFLOW_DISABLE_AGENT_HINT=1` silences an MLflow hint on stderr.
- A stale `.mypy_cache` after parallel runs can produce bogus attribute errors; delete it and rerun.
- npm 10 can crash with `edgesOut`; the repository `.npmrc` (`legacy-peer-deps=true`) avoids it.

## Contracts

`packages/contracts/openapi.json` is generated from the backend and TypeScript types from it:

```bash
npm run contracts         # regenerate after an API change
npm run contracts:check   # fails if either file drifts from the code (part of npm run check and CI)
```

## Tests and quality gates

| Command | Scope |
|---|---|
| `npm run lint` | ruff check + ruff format --check (Python), ESLint `--max-warnings 0` (web, gateway) |
| `npm run typecheck` | mypy (`backend/icp_api`, `pipeline/icp_ml`, `check_untyped_defs`), tsc for contracts, web and gateway |
| `npm test` | pytest (`pipeline/tests`, `backend/tests`), Vitest for web and gateway |
| `npm run build` | Vite build + Wrangler dry run of the Worker |
| `npm run landing-guard` | After a build: fails if the JavaScript the landing loads first (or a public page) contains the charting library (Recharts); prints the landing JS size, raw and gzip (`scripts/check-landing-bundle.mjs`, `--self-test` checks the guard itself) |
| `npm run secret-scan` | Pattern scan of tracked source for credentials and committed secret files |
| `node scripts/py.mjs -m icp_api.cli audit-prune` | Dry run of the audit-log retention (`--apply` deletes; see [OPERATIONS.md](OPERATIONS.md)) |
| `npm run check` | All of the above in that order (lint → typecheck → contracts:check → test → build → landing guard → secret scan) |
| `npm run smoke -- <url>` | End-to-end smoke against a running stack (guest checks; signed-in checks with `ICP_SMOKE_EMAIL`/`ICP_SMOKE_PASSWORD` or `ICP_SMOKE_TOKEN`) |

Test suites:

- **Pipeline** (`pipeline/tests`): sources and parsers, schema/completeness, features, forecasting
  semantics, backtest (incl. leakage checks), training/registry, bundle equality.
- **Backend** (`backend/tests`): API, storage adapters (SQLite and simulated D1/gateway protocols),
  products, and dedicated security suites: `test_access_control.py` (guest/role/API key/operator/
  cross-tenant matrix), `test_auth_abuse.py`, `test_security_hardening.py`, `test_report_safety.py`,
  `test_data_entitlements.py`, `test_plan_requests.py`, plus insight engine, verification and
  forecast-explanation tests. Markers: `slow` (trains real models), `network` (needs internet).
- **Web** (`apps/web/src/**/*.test.*`): route boundaries and session flows, pages, i18n dictionary
  completeness, API client, formatting.
- **Gateway** (`apps/gateway/test`): proxying, rate-limit keys, header hygiene, internal data plane
  with fake bindings.

Current counts and the latest full run are recorded in the newest report under
[REMEDIATION_REPORT_2026-10.md](REMEDIATION_REPORT_2026-10.md); do not copy numbers here.

## Continuous integration

`.github/workflows/ci.yml` runs on pushes and pull requests to `main` (Ubuntu, Python 3.11, Node 22,
`contents: read`, 30-minute timeout, actions pinned by commit SHA, pip pinned): install with
`requirements/constraints.txt`, then lint, typecheck, contracts check, tests, build (no deploy), the landing
bundle guard and secret scan, i.e. the same steps as `npm run check`. Tests use Git-tracked data only (`data/raw/dataset.csv`,
`data/seed/*.csv`; `data/raw/wti.csv` is DVC-only), and CI sets `ICP_REQUIRE_DATA=1` so a missing test
dataset fails the run instead of skipping tests. Locally, without that variable, missing data skips.

`.github/workflows/continuous-training.yml` is manual-dispatch only (no schedule): it refreshes data through
the deployed job API and trains only when the backend reports `model_source=mlflow` and the `train` input is
set; the bundle-mode staging backend refuses training by design (see
[DEPLOYMENT.md](DEPLOYMENT.md#7-continuous-training-optional-still-0)).

The `security` job in the same workflow (Ubuntu, 15-minute timeout) installs pinned pip-audit 2.10.1 and
bandit 1.9.4 into a separate venv and runs `pip-audit -r requirements/constraints.txt --no-deps --disable-pip`,
`npm audit --omit=dev --audit-level=moderate`, `bandit -r backend/icp_api pipeline/icp_ml -ll --ignore-nosec`
(inline `# nosec` markers have no effect; findings are fixed in code), the secret scan with its self-test
(`node scripts/secret-scan.mjs --self-test`), and generates CycloneDX SBOMs for Python and npm. The workflow
has not run on GitHub yet (nothing was pushed); each command was run locally.

The API image (`infra/docker/api.Dockerfile`) uses a base image pinned by digest
(`python:3.11-slim-bookworm@sha256:…`, overridable with `ARG PYTHON_IMAGE`).
