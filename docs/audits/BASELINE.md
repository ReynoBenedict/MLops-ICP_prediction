# Baseline – production-readiness audit (2026-10-20)

Status: CURRENT. Recorded before any change of this audit. No Git commands were run (prohibited by the
brief), so no commit/branch reference is recorded; the working tree contains the uncommitted
enhancements described in [REMEDIATION_REPORT_2026-10.md](../REMEDIATION_REPORT_2026-10.md).

## Repository root and platform

- Root: `C:\Users\User\PycharmProjects\MLops\MLops-ICP_prediction_OLD` (Windows 11, Git Bash + PowerShell).
- Despite the `_OLD` suffix this is the only clone that contains the ICP Intelligence platform
  (`apps/web`, `apps/gateway`, `backend/icp_api`, `pipeline/icp_ml`, `packages/contracts`, the docs named
  in the brief). The sibling `MLops-ICP_prediction` holds the earlier Streamlit course version
  (`app.py`, `components/`, `Dockerfile.*`, last modified 2026-05-23) and was not touched.
- Untracked owner files left untouched: `docs (2).zip`, root `CLAUDE.md` (empty, 0 bytes). The global
  `~/.claude/CLAUDE.md` is also empty.

## Constraint and memory documents

The documents named in the brief live in the assistant's project memory, not in the repository:
`project-icp-intelligence-constraints`, `icp-data-facts-2026-10`, `icp-local-dev-gotchas`,
`icp-test-accounts-and-screenshots`, `icp-access-policy-v4`, `icp-v5-insight-engine` (exact names), and
two under different names: `icp-zero-cost-hosting-2026-10` (brief: `...-decisions-2026-10`) and
`icp-ux-v3-2026-10` (brief: `icp-ux-v3-pass-2026-10`). Known staleness: the hosting note still mentions
Render Free as the backend; Render was abandoned (payment card required) – the current backend runs on
the operator machine behind a Cloudflare Quick Tunnel (docs/DEPLOYMENT.md).

## Runtime observed (read-only)

| Port / container | Owner | Notes |
|---|---|---|
| 127.0.0.1:8000 | ICP dev API (local SQLite `var/metadata.db`, schema 6) | started in earlier sessions |
| [::1]:5173 | ICP Vite dev server | proxies `/api` with the local gateway secret |
| 127.0.0.1:18000 + container `icp-api-staging` | staging backend image (older release) for the Quick Tunnel | not ours to change |
| [::1]:4173 | a `vite preview` from an earlier session | untouched |
| containers `mlops-icp_prediction_old-model-api-1` (unhealthy), grafana, kind, mqtt, postgres | other projects / legacy | untouched |

No Cloudflare tunnel process was running at baseline time, so the public staging URL's API was offline.

## Toolchain

Node 22.14.0, npm 10.9.2, Python 3.10.11 (.venv), Docker 27.4.0. Key packages: FastAPI 0.143.2,
Pydantic 2.14.1, httpx 0.28.1, pandas 2.3.3, scikit-learn 1.7.2, MLflow 3.17.1, ReportLab 4.5.1,
openpyxl 3.1.5, uvicorn 0.54.0, pytest 9.0.3, ruff 0.17.1, mypy 2.4.0; React 19.3.0, Vite 8.3.1,
Vitest 5.0.3, TypeScript 5.9.3, ESLint 10.11.0, Wrangler 4.150.0.

## Baseline quality gate

Command: `npm run check` (lint → typecheck → contract check → tests → build → secret scan), run while four
read-only audit agents were active on the same machine. Wall time 10 min 10 s. **Exit code 1.**

| Step | Result |
|---|---|
| ruff check / format check | pass (109 files) |
| mypy | pass (79 source files) |
| ESLint / tsc (web, gateway, contracts) | pass |
| API contract (`contracts:check`) | up to date (75 paths) |
| pytest (backend + pipeline) | 1002 passed, 1 warning (≈230 test functions; most items are parametrised sweeps) |
| web Vitest | **101 passed, 1 failed** (102) |
| gateway Vitest, build, secret scan | not reached (npm stops after the failing step) |

Failing test: `apps/web/src/pages/ops.test.tsx` › "names datasets instead of codes and shows 'Lolos · 1
peringatan'…" – `Unable to find role="group" and name "Dataset"` after the 3 s wait, with the test file
taking 19.2 s under load. Re-run in isolation: 3 of 3 passes (3.6–4.2 s each). Hypothesis (to be proven,
tracked as a finding): the operator page and its texts load lazily, and under CPU contention rendering
exceeds the test's wait time. Treated as an open defect in the test suite's robustness, not dismissed.

Earlier missions' figures (e.g. "1001 passed", "102/102") are superseded by this run.

## Coverage of this audit and blind spots (initial)

In scope: local source, configuration, migrations, tests, local runtime (dev API/web), official provider
documentation for billing/e-mail research. Out of scope / not testable here: remote D1/KV, the staging
Worker and tunnel, GitHub Actions runs, real e-mail, real payments, provider sandboxes (no credentials),
production hosting. Secrets were not read; tests needing privileged tokens use synthetic fixtures.
