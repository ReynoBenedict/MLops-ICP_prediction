# Deployment: zero-cost topology (Cloudflare Workers + serverless backend on Vercel Hobby)

> **Current state (2026-10-28):** the staging backend moves from the operator's laptop (Docker container
> behind a Cloudflare Quick Tunnel, which stopped whenever the laptop slept) to a **Vercel Function on the
> Hobby plan** (free, no card), so the laptop no longer has to stay on. Everything for it is prepared and
> verified locally (section "Backend on Vercel Hobby"); the first deploy is run with the commands in
> "Runbook: staging deploy". Until the gateway is redeployed with the Vercel URL, the 2026-10-17 Worker and
> the tunnel URL stay in place. The Quick Tunnel remains a documented manual fallback; Render Free stays
> excluded (it refused service creation without a payment method, section "Backend host: finding of
> 2026-10-17").

Target: **Rp0 / $0 per month**, no card, no paid trial, no usage-based billing risk. Every service
below is a free plan that *stops* (returns errors or pauses) at its quota instead of charging.

Sources checked (official pages, last-updated dates as shown on them): Cloudflare Workers platform
limits (2026-10-12), Workers Static Assets billing (2026-04-23) and headers, Wrangler configuration,
Workers KV and D1 limits, D1 migrations; Vercel [Functions limits](https://vercel.com/docs/functions/limitations)
(2026-08-24), [Python runtime](https://vercel.com/docs/functions/runtimes/python) (2026-08-12),
[FastAPI](https://vercel.com/docs/frameworks/backend/fastapi) (2026-08-27),
[Fluid compute](https://vercel.com/docs/fluid-compute) (2026-08-24),
[Fluid compute pricing](https://vercel.com/docs/functions/usage-and-pricing) (2026-06-16),
[Fair use guidelines](https://vercel.com/docs/limits/fair-use-guidelines) (2026-09-14),
[paused deployments](https://vercel.com/kb/guide/why-is-my-account-deployment-blocked) (2026-10-02),
[Deployment Protection](https://vercel.com/docs/deployment-protection) (2026-09-15),
[Instant Rollback](https://vercel.com/docs/instant-rollback) (2026-07-07), CLI
[deploy](https://vercel.com/docs/cli/deploy) (2026-09-18), [env](https://vercel.com/docs/cli/env) (2026-08-20),
[link](https://vercel.com/docs/cli/link) (2026-08-11), [global options](https://vercel.com/docs/cli/global-options) (2026-05-28).

## Topology

```
browser ──► Cloudflare Worker `icp-gateway-staging` (Workers Free)
             ├─ pages (/, /proyeksi, ...): the SPA document from static assets; a page view also sends one
             │                    non-blocking GET /health/live to the backend (≤ 1/min per isolate)
             ├─ static files (/assets, /fonts, /img, /brand, /favicon.svg): served directly, Worker not invoked
             ├─ /api/v1/*      → proxy to the backend with X-Gateway-Secret, edge rate limits;
             │                    anonymous GET /public/market-snapshot and /plans keep a
             │                    last-known-good copy in KV `edge-cache/public/*` (served when the backend is down)
             ├─ /api/internal/* (shared secret only; never reachable by the browser)
             │     ├─ d1/query      → D1 binding `DB`      (icp-metadata-staging, D1 Free)
             │     └─ objects/<key> → KV binding `OBJECTS` (icp-objects-staging, KV Free)
             └─ Cron Trigger 22:17 UTC daily → refresh both public copies through the same /api/v1 path
backend ──► Vercel Function `icp-api-staging` (Hobby, Python 3.12, region sin1, Fluid compute)
             built from var/deploy/vercel-backend/ by scripts/vercel-backend.mjs
             METADATA_BACKEND=d1_gateway   OBJECT_STORE_BACKEND=gateway   MODEL_SOURCE=bundle
             (no Cloudflare API token, no R2, no Postgres, no MLflow server, no job runner)
operator machine ──► MLflow (local SQLite + artifacts): train, validate, promote, then
                     `icp-api export-model` writes the promoted bundle into KV via the Worker; ingestion
                     (`icp-api ingest ...`) and data repairs run here against the gateway stores.
```

Why this shape:
- **R2** needs an "R2 subscription" enabled through checkout; it is not part of the plain free
  account, so objects live in **KV** instead (free: 1 GB, 25 MiB per value, 100k reads and 1k writes
  per day). Objects here are small (dataset CSVs, forecast/report exports, a ~100 KB model bundle).
- **D1** is reached through the Worker's binding rather than the REST API, so the backend holds no
  Cloudflare API token and the only secret shared between the two sides is `GATEWAY_SHARED_SECRET`.
  The response envelope is the D1 REST one, so the backend's executor code is shared.
- **MLflow** stays on the operator machine. Inference loads the promoted model from an exported
  bundle (same skops bytes, same trusted-type allow-list, identical predictions; tested in
  `pipeline/tests/test_bundle.py` and `backend/tests/test_api.py::test_bundle_mode_*`). The backend
  refuses training, validation, promotion and rollback with `409 OPERATION_UNAVAILABLE` in this mode,
  and MLflow is not even installed in the function (it is never imported in bundle mode).
- **Vercel Hobby** runs the unchanged FastAPI app as one Python function without a card and without a
  machine of ours: an instance starts on demand (imports about 2.1 s locally; on Vercel a start after an idle
  period also installs most dependencies first, see "Size of the function"), is paused between requests and
  scales by itself. It is the only free host found on 2026-10-27 that runs
  pandas/scikit-learn without a payment method (Hugging Face Spaces Docker/Gradio need PRO; Render 402;
  Koyeb, Fly.io, Railway, Cloud Run, Oracle, AWS, Azure need a plan, a trial or a card; Workers Python has
  10 ms CPU on Free; Cloudflare Containers need Workers Paid).
- Excluded earlier and still excluded: Replit deployments (paid), Backblaze B2 (overage billing), Neon (not needed).

## Free-tier limits that bound this deployment

| Service | Limit (free) | Behaviour at the limit | Project usage |
|---|---|---|---|
| Workers | 100k requests/day, 10 ms CPU/request, 50 subrequests/request, 5 Cron Triggers/account, static asset requests free and unlimited | errors (HTTP 1015/429/1027), no charge | gateway + data plane; each backend D1/KV call is one Worker request. Since 2026-10-28 every **page view** (the SPA document) also invokes the Worker once (`assets.run_worker_first`); files under `/assets`, `/fonts`, `/img`, `/brand` do not. One Cron Trigger (daily). An anonymous snapshot served from the backend's memory is one Worker request (the caller's); computing it adds three data-plane requests |
| D1 | 5 GB, 5M rows read/day, 100k rows written/day | errors, no charge | metadata; a forecast ≈ 5–10 rows read/written. The anonymous `GET /public/market-snapshot` reads every ICP evidence row when it is computed: 3 statements, about 280 rows read on the local store (up to about 420, growing with stored evidence and forecasts). The backend keeps the answer in memory and computes it at most once per 60 s (and after a write it depends on), so a repeat reads 0 rows and the endpoint costs at most about 1,440 computations ≈ 0.6M rows a day whatever the request volume (per backend instance; a serverless instance starts with an empty memo). Before this (2026-10-25 build) every call read them (SEC-C-01) |
| KV | 1 GB, 100k reads/day, 1k writes/day, 1k list/day, 25 MiB/value | errors, no charge | datasets, exports, reports, model bundle. **1k writes/day** = ~1k forecasts + reports per day; a failed export write no longer fails the forecast (`export_available=false`). The gateway's last-known-good copies of the two public answers are rewritten when their content changes (the snapshot's `generated_at` is ignored) and otherwise about once a day (visitors: from 24 h; the daily Cron Trigger: from 23 h), so about 2 writes a day in normal operation, never more than one per key per 5 minutes per isolate, see [OPERATIONS.md](OPERATIONS.md#stale-public-data-last-known-good-copies-at-the-gateway) |
| Vercel Hobby (functions) | Python bundle ≤ **500 MB** uncompressed (of which Vercel keeps at most 225 MiB inside the function and installs the rest at every cold start, see "Size of the function"); 2 GB memory / 1 vCPU; max duration 300 s (set to 60 s here); request and response body ≤ **4.5 MB**; one region; per month: **1,000,000 invocations, 4 h Active CPU, 360 GB-hrs Provisioned Memory**, 100 GB Fast Data Transfer, 10 GB Fast Origin Transfer; non-commercial personal use only | "sustained usage past these limits can lead to a paused deployment" (`503 DEPLOYMENT_PAUSED`); Vercel says it reaches out first; Hobby has no payment method, so nothing is billed | one function. Active CPU is billed only while code runs, Provisioned Memory only while a request is in flight ("After all requests complete, the instance is paused, and no CPU or memory charges apply until the next invocation"). Estimate for this project: a cold start ≈ **5–10 CPU-seconds** with Vercel's default packing (unpacking the dependencies 3–7 CPU-s, measured locally, plus imports 2–3 s), about 2–4 with `--bundle full`; a warm forecast or snapshot tens of milliseconds, a report well under a second (measured locally, below). The 4 h allowance is about 1,400–2,900 default cold starts a month |
| Vercel CLI uploads | ≤ 100 MB source per deploy on Hobby | deploy refused | the deploy folder is 1.1 MB (113 files), uploaded from a stage directory outside the repository; dependencies are installed by Vercel's build |

Memory figures in the Vercel row are decimal MB as Vercel states them; the bundle measurements below are
given in both units.

## Resources created (staging)

| Resource | Name / id | Created by |
|---|---|---|
| D1 database | `icp-metadata-staging` / `15fece89-2034-4f83-bc45-a308023e16f4` | `npx wrangler d1 create` (free) |
| KV namespace | `icp-objects-staging` / `067677c60b6946949ed2ff3cf518ea7a` | `npx wrangler kv namespace create` (free) |
| Worker | `icp-gateway-staging` → `https://icp-gateway-staging.icp-gateway.workers.dev` (`apps/gateway/wrangler.jsonc`, env `staging`) | `wrangler deploy --env staging` (deployed 2026-10-17; redeploy pending) |
| Vercel project | `icp-api-staging` (Hobby, region `sin1`, **not connected to Git**), production domain printed by the first deploy (normally `https://icp-api-staging.vercel.app` if that name is free) | `node scripts/vercel-backend.mjs --deploy` (`vercel link --yes --project icp-api-staging` creates it from the stage directory; not created yet) |
| Render service | none (every free-plan create returned 402, see below) | – |

Production uses separate resources (placeholders in `wrangler.jsonc`, `<D1_PRODUCTION_DATABASE_ID>`,
`<KV_PRODUCTION_NAMESPACE_ID>`), created the same way when needed.

## Backend on Vercel Hobby

### What is deployed

`node scripts/vercel-backend.mjs` builds `var/deploy/vercel-backend/` (gitignored) from the working tree,
in the repository layout so that `icp_api.settings.PROJECT_ROOT` is the bundle root and every file the API
reads is inside the bundle:

| Path in the deploy folder | From | Why |
|---|---|---|
| `index.py` | `infra/vercel/index.py` | Vercel entry point (FastAPI preset): puts `backend/` and `pipeline/` on `sys.path`, sets `sys.dont_write_bytecode`, imports `app` from `icp_api.serverless` |
| `requirements.txt` | `infra/vercel/requirements.txt` | 45 exact pins (full closure), same versions as `requirements/constraints.txt` (et_xmlfile, missing there, at the dev environment's 2.0.0) |
| `.python-version` | `infra/vercel/.python-version` | `3.12` (Vercel's default) |
| `vercel.json` | `infra/vercel/vercel.json` | `regions: ["sin1"]` (Singapore, next to the owner and the Cloudflare SIN edge), `fluid: true`, `functions."index.py".maxDuration: 60`, `excludeFiles` for the project's test folders |
| `backend/icp_api/**`, `pipeline/icp_ml/**` | the packages, without `__pycache__` | the API and the shared ML code |
| `pipeline/configs/training.yaml` | as is | read by `ModelService` (experiment name, promotion policy) |
| `infra/cloudflare/d1/migrations/*.sql` | as is | readiness compares the D1 schema with the number of migrations |
| `data/seed/*.csv`, `data/raw/dataset.csv` | as is | read only when a *local* store is bootstrapped (local checks of the bundle); never in staging |
| `.vercelignore` | generated | never uploads `.env*`, `.vercel`, bytecode |

A manifest with the SHA-256 of every file is written next to it (`var/deploy/vercel-backend.manifest.json`).
The build refuses to include `.env*`, `*.token`, `*.db` or a `secrets` path.

**The Vercel CLI never runs inside the repository.** `--deploy` (and `--deploy --dry-run`) copy the folder to a
*stage directory* outside any Git work tree (default `%LOCALAPPDATA%\icp-vercel-backend`, else
`~/.cache/icp-vercel-backend`; `--stage-dir` to change it), check every copied file against the manifest, and run
the pinned CLI only there. The project link (`.vercel/project.json`, ids only) stays in that directory between
deploys. The directory is emptied before each copy, so the script accepts only an empty one, one it marked itself
(`.icp-vercel-stage`, never uploaded) or an earlier stage copy: a mistyped `--stage-dir` is refused, never wiped.
Why: run inside the work tree, `vercel link --yes` creating a new project finds the repository's
`.git`, reads `origin` and connects the GitHub repository without asking (pinned CLI code), so every push would
start a Vercel build of the repository root, which fails and puts a red check on every commit of the public
repository. The script therefore refuses a stage directory inside a work tree, fences Git discovery at it
(`GIT_CEILING_DIRECTORIES`, inherited `GIT_DIR`-style variables removed), and after linking runs
`vercel git disconnect --yes`: the expected answer is "No Git repository connected" (exit 1); a connection that
exists anyway is removed ("Disconnected <org>/<repo>.", exit 0) and reported; any other answer stops the deploy. The CLI also runs no Git command against the
repository this way. `vercel link` writes a short-lived `VERCEL_OIDC_TOKEN` to `<stage>/.env.local` and creates a
`.gitignore` there; the script deletes both right after linking and again when it exits (`.vercelignore` keeps
`.env*` out of the upload anyway).

`backend/icp_api/serverless.py` builds the app for a serverless process (`load_settings(serverless=True)`,
`create_app(..., enable_jobs=False)`):
- **No bind address, no job runner.** The `API_HOST` rule does not apply (nothing binds a socket). An instance
  is paused between requests, so background jobs cannot run there: `POST /api/v1/jobs` answers
  `503 JOBS_DISABLED` (listing jobs and the audit log still work). Ingestion runs on the operator machine
  against the gateway stores (`icp-api ingest benchmarks|fx|icp`, [OPERATIONS.md](OPERATIONS.md#data-refresh-and-freshness)).
- **Only `/tmp` is writable.** Every writable default moves under `/tmp/icp` (SQLite path, local objects,
  source cache and state, MLflow defaults; explicit variables still win) and the libraries that write
  (`MPLCONFIGDIR`, `XDG_CACHE_HOME`, `XDG_CONFIG_HOME`, `SOURCE_CACHE_DIR`, `SOURCE_STATE_DIR`) point there.
  Verified on Linux with a read-only bundle and a read-only `HOME`: the only file written during health,
  sign-up, sign-in, snapshot, forecasts and PDF/Excel/CSV reports was `/tmp/icp/matplotlib/fontlist-v390.json`.
- **Lifespan events are optional.** Vercel documents FastAPI lifespan support, but nothing depends on it:
  the app is complete when the module is imported, and the production-model warm-up starts there (the
  lifespan's start returns the running warm-up instead of starting a second one). The model also loads on
  first use. Shutdown work is limited to 500 ms on Vercel; the app has none.
- `/health/live` and `/health/ready` stay public (status only); every other path, including `/` and
  `/docs`, needs the gateway secret (`403 GATEWAY_REQUIRED`); `/metrics` needs `METRICS_TOKEN`.

### Size of the function (measured 2026-10-27) and how Vercel packs it

The 45 pinned wheels were downloaded for Linux x86_64, CPython 3.12 (`pip download --only-binary=:all:
--platform manylinux2014_x86_64 --platform manylinux_2_28_x86_64 --platform manylinux_2_34_x86_64
--python-version 3.12 -c requirements/constraints.txt`) and installed with `pip install --no-index --target`:

| Measure | Size |
|---|---|
| Installed, no bytecode | **356.1 MB** (339.6 MiB): within the 500 MB Python limit, but **above the 225 MiB Vercel keeps inside a function** |
| Installed, `.pyc` for every module (upper bound) | 467.1 MB (445.4 MiB) |
| The wheel files (what a default cold start downloads, below) | 104 MB |
| Our code and data (deploy folder) | 1.1 MB |

**How the 500 MB limit is reached** (read 2026-10-28 in Vercel's own code: the Python builder `@vercel/python`
27.0.0, shipped with CLI 65.0.0, the newest copy at hand, and the runtime `vercel-runtime` 0.23.3, `vc_init.py`.
A remote build runs the builder version Vercel's build machines choose, not the one inside the pinned CLI 63.1.2
(`@vercel/python` 24.0.1, used only by a local `vercel build`); the packing lines of the build log, which the
deploy script prints, show which path was taken). The public pages only say "500 MB
uncompressed". The builder keeps at most **225 MiB** of a Python function inside the function. Above that it
switches on *runtime install*: public PyPI packages are left out of the function, and at **every cold start**
the runtime installs them with `uv sync --frozen --no-cache` from PyPI into `/tmp/_vc_deps` (the builder budgets
450 MiB of the 500 MiB `/tmp`) before it imports the app. If that install fails, the instance does not start.
With bytecode compilation on (Vercel's Python page says it compiles automatically; in builder 27.0.0 it is the
build environment's `VERCEL_PYTHON_COMPILEALL` switch) all 45 packages are installed this way and the function
keeps precompiled `.pyc` files for them, up to its 225 MiB; otherwise the largest packages that fit (about
179 MiB: scipy and numpy) stay inside and the rest is installed. `excludeFiles` cannot change this: it applies to the project's own files,
never to dependencies.

The deploy script offers both ways (`--bundle`, applied per deployment through build-time variables; the
project's variables are never changed for it). The mode of the last successful deploy is kept in
`var/deploy/vercel-bundle-mode.txt` and used when `--bundle` is not given, so a plain redeploy keeps the packing:

| | Default: `--bundle runtime-install` | `--bundle full` |
|---|---|---|
| What it is | Vercel's standard path for 225–500 MiB (documented, generally available) | build variables `VERCEL_PYTHON_DISABLE_RUNTIME_INSTALL=1` and `VERCEL_SUPPORT_LARGE_FUNCTIONS=1`: every dependency stays inside, and the function runs on the **large functions** path (beta, up to 5 GB; "new projects are eligible for large functions by default", no plan restriction stated; Hobby limits still apply and nothing can be billed). The first variable is a builder switch that Vercel's pages do not document; the second is the large-functions switch and prints the build-log line below |
| Cold start | download of about 104 MB of wheels, unpacking (3–7 CPU-s, measured locally) and the imports (2–3 s): estimated **6–14 s**, not measured on Vercel | the imports (2–3 s) plus loading a larger function: not measured |
| Needs PyPI (`files.pythonhosted.org`) at start-up | **yes**: during an outage no new instance starts | no |
| Active CPU per cold start | about 5–10 CPU-s | about 2–4 CPU-s |
| Build log (printed by the script) | `Bundle size (… MB) exceeds the standard size; optimizing dependencies.` | `Function "index.py" exceeds the standard size limit; enabling large functions (beta).` |
| Runtime log of each cold start (`--runtime-log`) | `Installing runtime dependencies...` and `Runtime dependencies installed in X s` | neither |

Decision on the first deploy (runbook step 4): keep the default when the first `/health/live` answered within
about 6 s **and** the runtime log reports an install of at most about 3 s. Otherwise redeploy with
`--bundle full --skip-env`, keep it if it builds, starts and answers faster, and redeploy without the flag if it
does not build or start. Shrinking the dependency set below 225 MiB is not a small change: numpy, scipy, pandas
and scikit-learn alone are about 254 MB (the model needs scikit-learn, which needs scipy).

Largest packages: scipy 119.9 MB, numpy 57.8, pandas 43.4, scikit-learn 33.4, fonttools 22.9, matplotlib 22.3,
pillow 20.4, kiwisolver 5.8, pydantic-core 5.2, reportlab 4.7. Left out: mlflow (never imported in bundle
mode), uvicorn (Vercel serves the ASGI app), boto3/psycopg2 (unused transports), the ingest extra (PDF/OCR,
operator machine only) and defusedxml (FX ingestion only).

**Do not strip the packages' `tests/` folders** by any other means (a pruned copy of the packages, for example):
removing them saved 42.6 MB but the app no longer started, because `numpy.testing` imports `numpy._core.tests`
and scipy imports `numpy.testing`. `vercel.json` excludes only the project's own test folders, and a test keeps
`**/tests/**` out of it in case the builder ever applies `excludeFiles` to dependencies.

### Measured cold start and timeouts

Linux (WSL2 Ubuntu 24.04 on the operator laptop), CPython 3.12.3, exactly the pinned wheels (no MLflow
installed), the read-only bundle served by uvicorn, copies of the local SQLite database and object store
(`APP_ENV=local`, `MODEL_SOURCE=bundle`), 3 fresh processes per row:

| From process start to | Time |
|---|---|
| app built (imports + `create_serverless_app`) | 1.97–2.17 s |
| first `GET /api/v1/auth/whoami` with a member session answered | 2.08–2.17 s |
| first `GET /health/ready` answering `ready` (model v5 loaded, every check `ok`) | 2.27–2.51 s |

Import profile (cumulative): scikit-learn 480 ms (pulled in by `icp_ml.config` → `backtest` → `methods`,
whose estimators subclass scikit-learn classes and are referenced by module path in the skops allow-list,
so deferring it is not a small change), pandas 299 ms, fastapi 253 ms, numpy 112 ms. Warm requests in the
same run: public snapshot 387 ms (first computation), forecast 30–50 ms, market report PDF + Excel + CSV
982 ms (first report; Matplotlib and ReportLab load), forecast report PDF + CSV 262 ms.

These local figures cover the **imports and the app only**. On Vercel, with the default packing, a cold start
first downloads and installs the dependencies (previous section; estimated 6–14 s in all), and the first
requests add the D1/KV round trips through the Worker from `sin1`. **Not measured yet: the cold start on Vercel
itself.** `scripts/vercel-backend.mjs --deploy` prints the first `/health/live` time and the build's packing
lines, and `--runtime-log` looks for the install lines in the last hour of request logs (Hobby keeps runtime logs
1 h; if the request logs do not carry them, the project's Logs page in the dashboard shows them); record the first
real numbers here. The first answer right after a deploy may meet an instance Vercel has already started, so it
can be quicker than a start after an idle period; the install line of the runtime log is the better measure.
Against the budgets:
- Public data (`PUBLIC_UPSTREAM_TIMEOUT_MS` 8 s, browser 10 s): 3.4× the local `ready` time, but **probably
  exceeded by a default cold start**. A slower start only turns that answer into the stored copy
  (`X-ICP-Cache: stale`), never into an error once a copy exists; the daily refresh keeps a copy in place. So
  the first visitor after an idle period will usually see the stored copy.
- Sign-in check (`whoami`, browser 8 s, `apps/web/src/api/client.ts`): 3.7× the local time, also probably
  exceeded by a default cold start. Then the app continues as a guest and re-checks the session in the
  background (`auth.tsx`), so a signed-in visitor first sees the guest view and is signed in again moments
  later; nothing is lost. Warm-on-visit (below) starts the instance when the page loads, which shortens this.
  Report a measured value above about 6 s to the web owners before changing that timeout.
- Everything else (`UPSTREAM_TIMEOUT_MS` 90 s at the gateway, `maxDuration` 60 s on Vercel, backend request
  limit 30 s): unchanged ordering, see [OPERATIONS.md](OPERATIONS.md#timeout-budget).

### Keeping the backend usable without keep-warm

- **Warm-on-visit.** When the Worker serves the SPA document of a browser's navigation (`Sec-Fetch-Mode:
  navigate` or `Sec-Fetch-Dest: document`), it starts `GET /health/live` to the backend in `ctx.waitUntil` and
  returns the page at once; at most one ping per backend per Worker isolate per minute
  (`WARM_PING_INTERVAL_MS`). The visitor's first API call (sign-in check, snapshot) then meets a starting
  instance. Worker log: `gateway.backend_warm_ping` with status and duration. Only people's browsers trigger it:
  a request without Fetch Metadata (an HTML `Accept` alone: scripts, crawlers, link previews, uptime monitors,
  Safari before 16.4) and user agents of crawlers, monitors and headless or audit browsers
  (`AUTOMATED_USER_AGENT`: `bot`, `crawl`, `spider`, `headless`, `lighthouse`, `uptime`, `monitor`, ...) never
  do, otherwise a monitor polling a page would become the keep-warm ruled out below. **Point any uptime monitor
  at `/api/gateway/health`** (answered by the Worker, never calls the backend), never at a page or at the
  backend's `/health/*`.
- **Daily refresh (Cron Trigger `17 22 * * *`, 05:17 WIB).** The Worker requests both anonymous public
  answers through `handleApi`, exactly like a visitor (contract C2), but with a 60 s upstream budget (nobody
  waits) and rewrites an unchanged copy from 23 h on, so the landing page has current figures even when nobody
  visited. Two backend invocations a day. Worker log: `gateway.public_refresh`.
- **Why no frequent keep-warm.** On Hobby the scarce resource is **Active CPU (4 h a month)**: every cold
  start spends CPU on the dependency install (default packing) and the imports, and Vercel does not document how
  long an idle instance survives, so a ping every few minutes that often meets a stopped instance would cost cold
  starts around the clock (288 a day at a 5-minute period; at 5–10 CPU-seconds each with the default packing
  that is 12–24 h a month, and still 4.8–9.6 h at 2–4 CPU-seconds with `--bundle full`: over the allowance
  either way). Each start with the default packing also downloads about 104 MB from PyPI. Memory is billed
  only while a request is in flight; an instance kept *busy* 24/7 would need 2 GB × 720 h = 1,440 GB-hrs,
  four times the 360 GB-hrs included. Vercel's fair-use guidelines also forbid "circumventing or otherwise
  misusing" its limits. So the design accepts an occasional cold start and hides it where it matters (stale
  public copy, guest-then-recheck sign-in, warm-on-visit).
- **Several instances, paused instances.** Vercel scales instances by itself (Fluid compute shares one
  instance between concurrent requests first). Everything the API keeps in process memory is then per instance
  (`API_INSTANCES=1` remains required but cannot be enforced by the platform):
  - rate limits and sign-in throttles count per instance; the Worker's edge limiters still apply per client IP;
  - the report `Idempotency-Key` reservation (`backend/icp_api/services/reports.py`, `reserve_idempotency`) is
    in-process by design, so a retry that reaches another instance while the first request is still generating
    can produce a second report (instead of `409 IDEMPOTENCY_IN_PROGRESS`); a retry after the first request
    ended still gets the stored report;
  - the public-snapshot memo starts empty in each instance (D1 row above).

  Vercel also pauses an instance once no request is in flight ("After all requests complete, the instance is
  paused"). Work that outlives its request therefore stops until the next request reaches the same instance, or
  never resumes: a report that hit the 30 s request deadline (`504`) while its worker thread went on, and the
  production-model warm-up thread (harmless: it continues with the next request, and the model also loads on
  first use). Residual risks to record in
  [SECURITY.md](SECURITY.md) (not done yet, outside this change): per-instance throttles and rate limits, and the
  per-instance idempotency reservation.

### Deployment Protection and the backend URL

Projects created by the CLI get **Standard Protection** with Vercel Authentication: generated deployment URLs
(`icp-api-staging-<hash>-<scope>.vercel.app`) require a Vercel login, the **production domain stays public**.
`BACKEND_URL` must therefore be the production domain (`productionUrl` printed by the script and saved in
`var/vercel-backend-url.txt`), never a deployment URL. The backend itself still refuses `/api/*` without the
gateway secret. Password protection is not available on Hobby (and not needed).

## Runbook: staging deploy

Run from the repository root on the operator machine, in this order. Steps 3–7 are remote (step 6 writes to
the staging stores through the Worker). The values marked "create" must exist first; their names, never
values, are listed here.

**Create (owner/main agent):**
- a Vercel account on the **Hobby** plan (no card) and a Vercel access token → `var/secrets/vercel.token`
  (one line; gitignored);
- `var/secrets/staging.vercel.env` from `var/secrets/staging.vercel.env.example`: the values of
  `APP_ENV, METADATA_BACKEND, OBJECT_STORE_BACKEND, MODEL_SOURCE, REQUIRE_GATEWAY_SECRET, LOG_FORMAT,
  GATEWAY_INTERNAL_URL, CORS_ALLOWED_ORIGINS, GATEWAY_SHARED_SECRET, ADMIN_API_TOKENS` copied from
  `var/secrets/staging.render.env` (drop `RENDER` and `PORT`), a **new** `METRICS_TOKEN`
  (`node scripts/py.mjs -m icp_api.cli new-token`), and `EMAIL_ENABLED=false`, `BILLING_ENABLED=false`,
  `API_INSTANCES=1`, `EXPOSE_API_DOCS=true`;
- a Wrangler login for the Cloudflare account (`npx wrangler whoami`).

```bash
# 1. local: build the deploy folder, copy it to the stage directory outside the repository (checked against the
#    manifest), validate the variable file (names/rules only), print the remote steps
node scripts/vercel-backend.mjs --deploy --dry-run

# 2. local checks of what will be deployed (optional but cheap)
npm run test -w @icp/gateway -- --maxWorkers=2
node scripts/py.mjs -m pytest backend/tests/test_serverless.py -q

# 3. D1: back up, then migrations 0005-0008 (staging is at schema 4; all additive)
npx wrangler d1 export icp-metadata-staging --remote --output=var/backups/d1-staging-<UTC stamp>.sql -c infra/cloudflare/d1/wrangler.jsonc
npx wrangler d1 migrations list icp-metadata-staging --remote -c infra/cloudflare/d1/wrangler.jsonc
npx wrangler d1 migrations apply icp-metadata-staging --remote -c infra/cloudflare/d1/wrangler.jsonc

# 4. backend: link/create icp-api-staging (from the stage directory), confirm it has no Git connection, set the
#    15 Production variables (stdin), deploy, print the build's packing lines, check /health
node scripts/vercel-backend.mjs --deploy
#    expect: "git connection: none"; build log "Bundle size (... MB) exceeds the standard size; optimizing
#            dependencies." (default packing); GET /health/live -> 200 {"status":"ok"}; /health/ready -> 200 ready
#            (or degraded while the model loads); GET /api/v1/plans without the secret -> 403 GATEWAY_REQUIRED
node scripts/vercel-backend.mjs --runtime-log   # "Runtime dependencies installed in X s" for each cold start so far
#    Decision ("Size of the function"): keep the default if /health/live answered within about 6 s and X is at
#    most about 3 s. Otherwise compare the full bundle, and keep whichever builds, starts and answers faster:
node scripts/vercel-backend.mjs --deploy --skip-env --bundle full   # expect "enabling large functions (beta)"
#    (if it does not build or start: node scripts/vercel-backend.mjs --deploy --skip-env --bundle runtime-install)
#    The mode of the last successful deploy is remembered (var/deploy/vercel-bundle-mode.txt) for later redeploys.
#    Then check once: Vercel dashboard > icp-api-staging > Settings > Git shows no connected repository.

# 5. gateway: point staging at the production domain (one line in wrangler.jsonc), rebuild, deploy
node scripts/vercel-backend.mjs --write-gateway-url
npm run build -w @icp/web && npm run landing-guard -- --max-gzip-bytes 160000
npm run build:staging -w @icp/gateway                          # dry run: bindings, vars, size
npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc

# 6. staging data repairs through the Worker (operator shell; set ONLY in this shell:
#    APP_ENV=local METADATA_BACKEND=d1_gateway OBJECT_STORE_BACKEND=gateway
#    GATEWAY_INTERNAL_URL=https://icp-gateway-staging.icp-gateway.workers.dev GATEWAY_SHARED_SECRET=<value>)
node scripts/py.mjs -m icp_api.cli migrate --check                 # schema v8 = expected v8
#    The local ESDM state was repaired on 2026-10-25, so it reports nothing to correct. Re-run the repair from
#    the pre-repair state kept in backup 20261025T075817Z (rehearsed locally: 26 documents, 28 evidence rows):
mkdir -p var/repair-staging-esdm && cp var/backups/20261025T075817Z/sources-esdm/decrees.json var/backups/20261025T075817Z/sources-esdm/reconciliation.json var/repair-staging-esdm/ && cp -r var/sources/esdm/pdf var/repair-staging-esdm/pdf
node scripts/py.mjs -m icp_api.cli repair-decree-references --state-dir var/repair-staging-esdm              # dry run
#    Back up again right before the write (the step-3 export predates migrations 0005-0008 and the new backend,
#    so restoring it would undo those too); this export is the backup that --backup-taken refers to:
npx wrangler d1 export icp-metadata-staging --remote --output=var/backups/d1-staging-<UTC stamp>-before-decree-repair.sql -c infra/cloudflare/d1/wrangler.jsonc
node scripts/py.mjs -m icp_api.cli repair-decree-references --state-dir var/repair-staging-esdm --apply --backup-taken
node scripts/py.mjs -m icp_api.cli backfill-dissent --dry-run
node scripts/py.mjs -m icp_api.cli backfill-dissent
node scripts/py.mjs -m icp_api.cli verify-provenance             # expected: 0 disagreements, 0 gaps

# 7. smoke test and the resilience checks
npm run smoke -- https://icp-gateway-staging.icp-gateway.workers.dev
curl -s -D - -o /dev/null https://icp-gateway-staging.icp-gateway.workers.dev/api/v1/plans | grep -i '^x-icp-cache'        # x-icp-cache: live
curl -s -D - -o /dev/null -H 'sec-fetch-mode: navigate' https://icp-gateway-staging.icp-gateway.workers.dev/ | grep -i '^content-security-policy'
npx wrangler kv key list --namespace-id 067677c60b6946949ed2ff3cf518ea7a --prefix edge-cache/ --remote   # both keys after the first live answers
npx wrangler tail --env staging -c apps/gateway/wrangler.jsonc   # a page view logs gateway.backend_warm_ping; 22:17 UTC logs gateway.public_refresh
```

After step 7, write the first measured Vercel cold start into "Measured cold start and timeouts" above: the
step 4 output (first `/health/live`, packing lines, `--runtime-log` install time) and, the next day, the first
`gateway.backend_warm_ping` after an idle hour (its `ms` is a real cold start seen from the Worker).

**Redeploying later:** backend code → `node scripts/vercel-backend.mjs --deploy --skip-env` (variables
unchanged) or `--deploy` (re-sends every variable; changed values only take effect in new deployments);
web app or Worker → `npm run build -w @icp/web && npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc`.

**Rollback:**
- **First backend deploy (tonight's case): there is nothing to roll back to on Vercel.** If the new backend does
  not come up, the API stays unavailable, as it is today: the live 2026-10-17 Worker points at the stopped laptop
  tunnel and predates the stored public copies (contract C2), so it has none to serve. Either leave that Worker
  in place (skip step 5), or run step 5 **without** `--write-gateway-url`: the new pages and images go live, and
  the placeholder `BACKEND_URL` makes every API call answer a clean `503 GATEWAY_MISCONFIGURED` (stored public
  copies exist only after the first live answers). `npx wrangler rollback --env staging -c apps/gateway/wrangler.jsonc`
  returns to the previous Worker at any time. Fix and redeploy the backend with `--deploy --skip-env`. The laptop
  fallback below needs a rebuilt Docker image, and Docker is not available on the operator machine tonight.
- Backend, later deploys: Vercel dashboard → project → Instant Rollback (Hobby: to the immediately previous
  production deployment). Rollback turns off automatic production-domain assignment; after fixing, promote the
  new deployment (`npx vercel@63.1.2 promote <deployment-url>` with `VERCEL_TOKEN` set, run from the stage
  directory) or use "Undo Rollback".
- Gateway: `npx wrangler rollback --env staging -c apps/gateway/wrangler.jsonc`.
- Backend on the laptop for a while (manual fallback): `npm run staging:backend` (below) deploys the Worker
  with `--var BACKEND_URL:<tunnel>`; a later plain `npx wrangler deploy --env staging` returns to Vercel.
- D1: Time Travel restore or the step-3 export, then fix forward.

**Troubleshooting the Vercel backend:** `vercel-backend: fix ... first` lists variable names that break a
staging rule; `stage directory ... is under a Git work tree` or `... is not empty and not a stage directory of this
script` → choose another `--stage-dir` (the script never empties a directory it did not create); `could not confirm
that the project has no Git connection` → check Project → Settings → Git, then re-run; a failed deploy prints
the CLI's answer (also in `var/deploy/vercel-deploy-last.json`, secrets redacted) with the
`inspect <url> --logs` command for the build log; `Runtime dependency installation failed` in the runtime log
(default packing) → PyPI was unreachable or `/tmp` full at a cold start: retry, or deploy with `--bundle full`;
`/health/live` not JSON or `401` → a protected deployment URL was used instead of the
production domain; `503 DEPLOYMENT_PAUSED` → Hobby usage limits (check Usage in the dashboard; the gateway
keeps serving the stored public copies); `504 FUNCTION_INVOCATION_TIMEOUT` → a request ran past 60 s;
`413 FUNCTION_PAYLOAD_TOO_LARGE` → a download over 4.5 MB (reports and exports are far smaller today; watch
organisation exports); runtime logs are kept 1 hour on Hobby (`node scripts/vercel-backend.mjs --runtime-log` prints the cold-start
install lines; the dashboard's Logs page shows everything).

## Order of operations (first setup of a new environment)

### 1. Secrets (never in files of the repository)

```bash
node scripts/py.mjs -m icp_api.cli new-token          # generate values; do not paste them anywhere public
npx wrangler secret put GATEWAY_SHARED_SECRET --env staging -c apps/gateway/wrangler.jsonc
```
The same value goes into the backend's variables as `GATEWAY_SHARED_SECRET` (Vercel: the env file used by
`scripts/vercel-backend.mjs`), and into the operator's shell for the export/seed commands below. Admin
tokens: `ADMIN_API_TOKENS=<name>:<token>` on the backend only.

### 2. D1 schema

```bash
# read infra/cloudflare/d1/migrations/*.sql first; they are additive only
npx wrangler d1 migrations list  icp-metadata-staging --remote -c infra/cloudflare/d1/wrangler.jsonc
npx wrangler d1 migrations apply icp-metadata-staging --remote -c infra/cloudflare/d1/wrangler.jsonc
```

### 3. Gateway Worker (assets + data plane)

```bash
npm ci
npm run build -w @icp/web
npm run build:staging -w @icp/gateway                      # dry run: bundles and validates bindings
npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc
curl https://icp-gateway-staging.<subdomain>.workers.dev/api/gateway/health
```
`/api/internal/health` answers only with the secret header (`x-gateway-secret`), reporting
`{"d1": true, "kv": true}`. `BACKEND_URL` may still be a placeholder at this point: the web app and the
data plane work; `/api/v1/*` answers `503 GATEWAY_MISCONFIGURED` until step 6 sets a real backend URL.

### 4. Seed data and export the model (operator machine → staging stores)

Set, in the shell only: `METADATA_BACKEND=d1_gateway`, `OBJECT_STORE_BACKEND=gateway`,
`GATEWAY_INTERNAL_URL=https://icp-gateway-staging.<subdomain>.workers.dev`, `GATEWAY_SHARED_SECRET`.
Keep `APP_ENV=local` and the local MLflow settings so the registry is read locally.

```bash
node scripts/py.mjs -m icp_api.cli migrate --check     # schema version of the remote D1 (via the Worker)
node scripts/py.mjs -m icp_api.cli export-model        # production alias (v5) → models/bundles/5/*, models/production.json
node scripts/py.mjs -m icp_api.cli clone-state --dry-run
node scripts/py.mjs -m icp_api.cli clone-state         # local SQLite rows + var/objects → D1 + KV
```
`clone-state` carries the verified local state over unchanged: the reconciled official ICP series
with its document evidence (`dataset_versions`, `source_documents`, `source_evidence`), benchmark
and FX versions, forecasts, promotions, accounts, scenarios, alerts and reports, plus every object
file (dataset CSVs, exports, the model bundle). It never copies `sessions` (login tokens) or
`app_schema`, only reads local files, and is idempotent (`INSERT OR IGNORE`, write-once objects), so
it can be re-run after local changes (it never updates a row that already exists remotely, which is why
the decree repair of the runbook is needed). This was executed for staging on 2026-10-17 (2,318 rows,
46 objects; a re-run added only the rows created locally in between). The alternatives are
`icp-api seed` (repository CSVs only, legacy series) or `icp-api ingest icp` from the operator machine
(slow: ESDM archive). `export-model` writes the bundle straight into KV through the Worker; re-running it is
idempotent (existing files are skipped, the pointer is refreshed).

### 5. Backend

`node scripts/vercel-backend.mjs --deploy` (section "Backend on Vercel Hobby" and the runbook). The Docker
image (`infra/docker/api.Dockerfile`) remains the way to run the backend on any container host and on the
laptop (Quick Tunnel fallback); `infra/render/` documents the Render variant that was refused.

### 6. Point the Worker at the backend

`node scripts/vercel-backend.mjs --write-gateway-url` writes the production domain into
`env.staging.vars.BACKEND_URL` of `apps/gateway/wrangler.jsonc` (the only place it is configured), then
`npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc` and:

```bash
npm run smoke -- https://icp-gateway-staging.<subdomain>.workers.dev
```

### 7. Continuous training (optional, still $0)

The GitHub Actions workflow trains **in CI**, not on the backend, when `MODEL_SOURCE=bundle`; it
cannot promote on the server. Keep it disabled for the bundle topology, or adapt it to run
`icp-api export-model` from a runner that has the MLflow store (future work; documented as not
verified).

## Environment variables (backend)

| Variable | Function | Component | Secret | Where set (staging) | Required |
|---|---|---|---|---|---|
| `APP_ENV` | strict validation, defaults | backend | no | Vercel project (Production) | yes (`staging`) |
| `METADATA_BACKEND` | `d1_gateway` | backend | no | Vercel project | yes |
| `OBJECT_STORE_BACKEND` | `gateway` (KV through the Worker) | backend | no | Vercel project | yes |
| `GATEWAY_INTERNAL_URL` | Worker origin for the data plane | backend, operator CLI | no | Vercel project / shell | yes |
| `GATEWAY_SHARED_SECRET` | authenticates Worker→backend and backend→Worker | Worker, backend, operator CLI | **yes** | `wrangler secret put`, Vercel project (Secret), shell | yes (≥ 32 chars) |
| `REQUIRE_GATEWAY_SECRET` | refuse direct calls | backend | no | Vercel project | yes (`true`) |
| `MODEL_SOURCE` | `bundle` | backend | no | Vercel project | yes |
| `MODEL_NAME`, `MODEL_ALIAS` | bundle/registry names | backend, operator CLI | no | Vercel project | default ok |
| `ADMIN_API_TOKENS` | operator tokens | backend | **yes** | Vercel project (Secret) | yes |
| `CORS_ALLOWED_ORIGINS` | the gateway origin | backend | no | Vercel project | yes |
| `METRICS_TOKEN` | protects `/metrics` | backend | **yes** | Vercel project (Secret) | **yes** (≥ 32 chars; staging and production refuse to start without it) |
| `EMAIL_ENABLED`, `BILLING_ENABLED` | must stay `false` (Hobby is non-commercial; billing would also need Pro) | backend | no | Vercel project | yes |
| `API_INSTANCES` | must be `1` | backend | no | Vercel project | yes |
| `PUBLIC_REGISTRATION_ENABLED` | self-service sign-up | backend | no | Vercel project | optional |
| `EIA_API_KEY` | benchmark ingestion | operator CLI | **yes** | shell (ingestion runs there) | optional |
| `LOG_FORMAT`, `LOG_LEVEL`, `EXPOSE_API_DOCS` | logging / docs | backend | no | Vercel project | optional |
| `API_HOST`, `PORT` | bind address | container hosts only | no | **not used** on Vercel (refused by the deploy script) | container: yes |
| `MLFLOW_TRACKING_URI`, `MLFLOW_ARTIFACT_ROOT` | **operator machine only** (training, export) | operator CLI | no | local `.env` | not on Vercel |
| `D1_*`, `S3_*`, `AWS_*` | REST/R2 transports | – | – | **unused** in this topology | no |

Worker: `BACKEND_URL` (https only in staging and production; no credentials in the URL; staging: the Vercel
production domain in `wrangler.jsonc`), `ENVIRONMENT`, `MAX_BODY_BYTES`, `UPSTREAM_TIMEOUT_MS`,
`PUBLIC_UPSTREAM_TIMEOUT_MS` (vars in `wrangler.jsonc`; a malformed number falls back to the default),
`GATEWAY_SHARED_SECRET` and optional `GATEWAY_ACCEPTED_SECRETS` (secrets), bindings `DB`, `OBJECTS` (also holds
the gateway's `edge-cache/` copies), `API_RATE_LIMITER`, `FORECAST_RATE_LIMITER`, `ASSETS`; staging Cron Trigger
`17 22 * * *`. The script entry point is `src/worker.ts`, which exports only the handler (`fetch`, `scheduled`).

## Operating the deployment

| Task | How |
|---|---|
| Redeploy the web app / Worker | `npm run build -w @icp/web && npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc` |
| Redeploy the backend | `node scripts/vercel-backend.mjs --deploy --skip-env` (or `--deploy` to re-send the variables) |
| Update the model | locally: train job → validate → promote (MLflow), then `icp-api export-model` with the staging store variables; the backend picks the new pointer up within `MODEL_REFRESH_SECONDS` |
| Refresh data (ICP, benchmarks, FX) | on the operator machine with the staging store variables: `node scripts/py.mjs -m icp_api.cli ingest icp|benchmarks|fx` (the serverless backend has no job runner) |
| Logs | Vercel dashboard → Logs (runtime logs kept 1 h on Hobby; JSON with `request_id`); `npx wrangler tail --env staging -c apps/gateway/wrangler.jsonc` (Worker) |
| Backup D1 | `npx wrangler d1 export icp-metadata-staging --remote --output=backup.sql -c infra/cloudflare/d1/wrangler.jsonc`; Time Travel restore is available on the free plan |
| Backup KV | `npx wrangler kv key list --namespace-id 067677c60b6946949ed2ff3cf518ea7a --remote` then `kv key get <key> --namespace-id … --remote` per object (datasets and bundles are write-once; the local MLflow store is the model backup; `edge-cache/` keys are disposable). Wrangler 4 uses local storage unless `--remote` is given |
| Purge the stale public copies | `npx wrangler kv key delete edge-cache/public/market-snapshot --namespace-id 067677c60b6946949ed2ff3cf518ea7a --remote` (same for `edge-cache/public/plans`); see [OPERATIONS.md](OPERATIONS.md#stale-public-data-last-known-good-copies-at-the-gateway) |
| Smoke test | `npm run smoke -- https://<gateway>` after any change |
| Roll back the Worker | `npx wrangler rollback --env staging -c apps/gateway/wrangler.jsonc` |
| Roll back the backend | Vercel Instant Rollback (Hobby: previous production deployment), then `vercel promote` to undo |
| Roll back the model | export the previous version (`icp-api export-model --version 4`) |
| Rotate the shared secret | `wrangler secret put` + Vercel variable (`--deploy` re-sends the env file; a new deployment picks it up) + shell; the Worker and backend must change together ([OPERATIONS.md](OPERATIONS.md#rotating-the-gateway-shared-secret)) |

## Backend host: finding of 2026-10-17 (Render refuses card-less workspaces)

With the Render CLI authenticated (workspace `My Workspace`, registry credential for the private
Docker Hub repository in place, image pushed), every attempt to create a service on the free plan
was refused by the Render API with `402 Payment information is required to complete this request`:
image-backed (private and public image, Singapore and Oregon), native Python from a public example
repository and Docker-from-Git alike. No probe service was created. The free-instance documentation
does not mention this requirement; it applies to this workspace as created in October 2026. Under
the project rule "no payment method", Render is therefore **not usable**. Nothing on Render was created or
left behind.

Alternatives checked against official pages the same day (re-checked 2026-10-27 for the move to Vercel):

| Host | Finding | Usable at Rp0 without a card? |
|---|---|---|
| Hugging Face Spaces | "Gradio and Docker Spaces run on compute and require a paid plan to create: PRO"; new free accounts get cpu-basic quota 0 | no |
| Modal | "$30/month free credits" but "you must have a payment method on file in order to use Modal" | no |
| Koyeb | no free compute | no |
| Sevalla | application hosting from $5/month; only static sites are free | no |
| Leapcell | free Hobby plan, Git-triggered deploys only, $1 card verification mentioned | no (Git deploy of the remote tree, card check) |
| Northflank | free sandbox, but "entering a card only verifies it" | no |
| Replit / Fly.io / Railway / Google Cloud Run / Oracle Always Free / AWS / Azure | paid plan, trial only or card required | no |
| Cloudflare Workers Python / Containers | Workers Python: 10 ms CPU on Free (no pandas/scikit-learn); Containers need Workers Paid | no |
| Cloudflare Quick Tunnel to a machine you control | free, no account, but "no uptime guarantee", random hostname, "for testing and development" | only as a manual fallback (the laptop must stay on) |
| **Vercel Hobby** | free, no card; Python 3.12 FastAPI functions up to 500 MB; non-commercial use only | **yes: the staging backend from 2026-10-28** |

## Self-hosted backend through a Quick Tunnel (manual fallback)

The staging topology from 2026-10-17 to 2026-10-27. Chosen by the owner after the Render finding and replaced by Vercel Hobby because the public API stopped
whenever the laptop slept. Kept as a fallback (for example while Vercel is paused or being debugged): the
backend container runs on the operator's machine and is published through a **Cloudflare Quick Tunnel**
(free, no account, no card; Cloudflare's own words: "no uptime guarantee", "for testing and development", a
new hostname on every start, up to 200 in-flight requests). The Worker, D1, KV and the model bundle are
unchanged; only `BACKEND_URL` points at the tunnel for that deploy.

```bash
npm run staging:backend                               # container + tunnel + Worker deploy with --var BACKEND_URL:<tunnel>
node scripts/staging-backend.mjs --deploy-only        # redeploy the Worker (e.g. after a web build) with the saved URL
npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc   # back to the Vercel backend (the value in wrangler.jsonc)
```
The tunnel hostname is machine-local state: it is kept in `var/staging-backend-url.txt`
(gitignored) and passed to Wrangler with `--var`, which overrides the Vercel domain in
`apps/gateway/wrangler.jsonc` for that deploy only. What the script does, step by step (also usable by hand):

1. `docker run -d --name icp-api-staging --restart unless-stopped --env-file var/secrets/staging.render.env -p 127.0.0.1:18000:10000 icp-api:staging-20261017`
   (the same variables as the Render plan; `PORT=10000` inside, bound to localhost only). Rebuild the image
   first for the current code (the 2026-10-17 image predates migrations 0005–0008), and add `METRICS_TOKEN`,
   `EMAIL_ENABLED`, `BILLING_ENABLED`, `API_INSTANCES` and `API_HOST=0.0.0.0` to that file.
2. `npx wrangler tunnel quick-start http://localhost:18000` (from `apps/gateway`) prints the
   `https://<random>.trycloudflare.com` hostname; the process must stay alive.
3. Save that hostname to `var/staging-backend-url.txt` and deploy with
   `npx wrangler deploy --env staging --var BACKEND_URL:<hostname>` (from `apps/gateway`).
4. `npm run smoke -- https://icp-gateway-staging.icp-gateway.workers.dev`.

Consequences: if the machine sleeps, the tunnel process stops or the network drops, the Worker
answers `504`/`502` for `/api/v1/*` (the web app shows its error state; the public pages show the stored
copies); assets, D1 and KV stay available, and no data is lost. The backend itself still refuses direct calls
through the tunnel without the gateway secret (`403 GATEWAY_REQUIRED`); the health endpoints are public by design.

Measured through the public URL on 2026-10-17: 11/11 smoke checks and 18/18 feature checks
(registration, login, profile, ICP summary, price history, forecast with scenario on bundle v5,
CSV export from KV, scenario save/list/compare in D1, market report PDF + CSV generation and
download, private routes and internal routes refused without credentials, direct backend access
refused, logout invalidates the session). Typical latency: SPA routes 0.2–0.4 s, cached API reads
≈ 1 s, the public market snapshot ≈ 5 s (many D1/KV round trips through the Worker; a first
optimisation target).

## What remains manual (consent flows only)

- Creating the Vercel account and an access token (dashboard, Account Settings → Tokens) once; the deploy
  script then works without the dashboard.
- `npx wrangler login` (browser consent) for the Cloudflare account.
- For the Quick Tunnel fallback only: Docker on the operator machine.

## Verification status

**2026-10-17 (Worker and stores):** D1 migrations 0001–0004 applied remotely (schema v4, 19 tables);
`wrangler secret put GATEWAY_SHARED_SECRET --env staging`; `wrangler deploy --env staging`
(41 assets); the deployed Worker answers `/` and `/app/*` (SPA), `/api/gateway/health`,
`/api/internal/health` → `{"d1": true, "kv": true}` with the secret and `401` without it;
`clone-state` and `export-model` through the Worker; a local backend process and the Docker image in
the Render configuration served readiness, data summary, latest forecast and new forecasts from the
remote stores. The new `workers.dev` hostname answered TLS handshake failures for about a minute
after the first deploy (certificate provisioning), then worked.

**2026-10-27/28 (Vercel backend, local only; nothing deployed):** the Linux wheel set installs offline from
the pins (no MLflow, boto3 or PyMuPDF present) and measures 356.1 MB; the read-only bundle served health,
sign-up, `whoami` (anonymous, member, operator), the public snapshot, plans, forecasts, market and forecast
reports (PDF, Excel, CSV), job listing, `503 JOBS_DISABLED` on job submission and `/metrics`; cold starts as
above; `backend/tests/test_serverless.py` (8 tests: settings, writable paths, app without and with lifespan,
the entry file in the deploy-folder layout in a fresh interpreter, requirement pins, function config) passes on
Python 3.10 (Windows) and 3.12 (Linux), and the targeted backend tests for bundle mode and the API pass on
Linux CPython 3.12 (69 tests). Gateway: 99 vitest tests (16 new: warm-on-visit, document headers, daily refresh,
wrangler.jsonc), `wrangler dev` entry check (`/api/gateway/health` 200; a page view sent exactly one
`GET /health/live` without the secret; the document carried the full `_headers` security set) and
`wrangler deploy --dry-run --env staging` (29.06 KiB).

**2026-10-28 (review fixes, local only; nothing deployed):** Vercel's builder and runtime code read for the
packing (above; `excludeFiles` never applies to dependencies), the deploy script reworked (stage directory
outside the repository, Git fence and check, OIDC token file removed after linking, deploy answer kept with
secrets redacted, `--bundle`, `--runtime-log`), warm-on-visit limited to browsers' navigations from people.

**Not verified yet:** the function on Vercel (build, packing, cold start with the dependency install, D1/KV
latency from `sin1`), the large-functions path on Hobby (`--bundle full`), whether the request logs carry the
start-up install line (`--runtime-log`), whether Cloudflare applies `_headers` to documents
returned through the Worker (the Worker adds the same headers when they are missing, so the answer is the
same either way), the Cron Trigger in production, KV eventual consistency under concurrent write-once puts
(keys are content-addressed or unique, so a lost race stores identical bytes).

## Pending for the next deployment (changes made locally on 2026-10-19/20)

Includes the later changes up to 2026-10-28. Everything below is in the runbook above, in order:
- **Back up D1 first**, then apply migrations **`0005_plan_change_requests.sql`, `0006_deletion_requests.sql`,
  `0007_identity_lifecycle.sql` and `0008_billing_sandbox.sql`** (all additive; staging is at schema 4).
  `/health/ready` reports the schema version to operators (`degraded` if the database is newer than the code,
  not ready if older). The local database was migrated to schema 8 on 2026-10-20 after a verified backup.
- Deploy the backend to Vercel (`node scripts/vercel-backend.mjs --deploy`) with `METRICS_TOKEN` (≥ 32 chars),
  `EMAIL_ENABLED=false` (`true` is refused), `BILLING_ENABLED=false` (sandbox only) and `API_INSTANCES=1`;
  optional `SESSION_IDLE_TIMEOUT_HOURS` / `TEMP_PASSWORD_TTL_HOURS` (see
  [ENVIRONMENTS.md](ENVIRONMENTS.md#identity-e-mail-and-billing-settings-backend)).
- Redeploy the Worker (after `--write-gateway-url`): security headers on its own error replies, client-IP header
  `X-ICP-Client-IP`, IP-keyed edge limiter, data-plane SQL allow-list, `GATEWAY_ACCEPTED_SECRETS` support,
  `Idempotency-Key` forwarding, `expirationTtl` on export objects, `/api/gateway/health` returning only the
  status, last-known-good public copies (contract C2), warm-on-visit, the daily Cron Trigger and the page
  routing through the Worker. After deploying, confirm the client-IP header once (SEC-01: log a hash of the
  received value).
- **Worker entry point (deploy blocker found 2026-10-25, fixed).** The v7 Worker exported a string
  (`CLIENT_IP_HEADER`) and an array (`EXPORT_TTL_SECONDS`) from its main module; workerd refuses to start such a
  script (`Uncaught TypeError: Incorrect type for map entry 'CLIENT_IP_HEADER'`). `main` is now `src/worker.ts`
  (handler only); `test/entrypoint.test.ts` guards it. Before any deploy, also start the Worker once with
  `npx wrangler dev -c apps/gateway/wrangler.jsonc` and request `/api/gateway/health` (done 2026-10-28).
- **Last-known-good copies of the public data (contract C2).** No new binding: the copies live in the existing
  `OBJECTS` namespace under `edge-cache/`. `PUBLIC_UPSTREAM_TIMEOUT_MS` = 8000 in every environment. Non-JSON
  upstream error pages become the JSON error envelope. After deploying, check with a **GET** that prints only the
  headers (`curl -I` sends HEAD, which is never cached) and list the two keys (runbook step 7). Until the backend
  answers once there is no copy, so an unreachable backend still yields the error envelope (not stale data).
- **Static headers** (`apps/web/public/_headers`): self-hosted fonts under `/fonts/*` are cached for a year;
  `Cross-Origin-Opener-Policy`, `Cross-Origin-Resource-Policy`, `X-Permitted-Cross-Domain-Policies`, a longer
  `Permissions-Policy` and `frame-src 'none'`/`worker-src 'none'`. Pages now pass through the Worker, which keeps
  the same `/*` set on them (`SITE_SECURITY_HEADERS`, kept identical to `_headers` by a test).
- Optional in staging/production: `LOGIN_ACCOUNT_FAILURES_MAX` (≥ `LOGIN_FAILURES_MAX`), `RATE_LIMIT_ADMIN_READ_PER_MINUTE`,
  `AUDIT_RETENTION_DAYS` (see [ENVIRONMENTS.md](ENVIRONMENTS.md#security-and-limit-settings-backend)).
- After deploying: correct the decree numbers in staging from the pre-repair ESDM state (runbook step 6; the
  already-repaired local state finds nothing), record the dissenting source readings (`backfill-dissent`), run
  `verify-provenance`, then the smoke test.

## Rollback and recovery

| Problem | Action |
|---|---|
| Bad model | `icp-api export-model --version <previous>` (pointer moves back; nothing is deleted) |
| Bad dataset import | Data → versions → Activate the previous version |
| Bad Worker release | `npx wrangler rollback --env staging` |
| Bad backend release | Vercel Instant Rollback (previous production deployment on Hobby); undo with `vercel promote <url>` after a fix |
| Backend host unavailable (paused, outage) | public pages keep serving the stored copies; for longer outages run the Quick Tunnel fallback (`npm run staging:backend`) |
| Bad D1 migration | D1 Time Travel restore, then fix forward |
| Leaked secret | rotate `GATEWAY_SHARED_SECRET` (Worker + Vercel variable + shell), admin tokens; revoke the Vercel access token in the dashboard |
