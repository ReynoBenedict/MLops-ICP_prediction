# Deployment: zero-cost topology (Cloudflare Workers + self-hosted backend)

> **Current state (2026-10-17):** the staging backend runs as a Docker container on the operator's
> machine behind a Cloudflare Quick Tunnel (section "Self-hosted backend through a Quick Tunnel").
> Render Free was the planned host but refused service creation without a payment method (section
> "Backend host: finding of 2026-10-17"); the Render steps below are kept as a ready-made alternative.

Target: **Rp0 / $0 per month**, no card, no paid trial, no usage-based billing risk. Every service
below is a free plan that *stops* (returns errors or suspends) at its quota instead of charging.

Sources checked on 2026-10-17 (official docs): Cloudflare Workers Free plan limits, Workers Static
Assets, Wrangler configuration (per-environment bindings), Workers KV and D1 limits and pricing,
Wrangler secrets, D1 migrations, R2 subscription requirement; Render free instance behaviour,
image-backed deploys, registry credentials, deploy hooks; Docker Hub free private repositories.

## Topology

```
browser ──► Cloudflare Worker `icp-gateway-staging` (Workers Free)
             ├─ static assets: apps/web/dist (SPA fallback)
             ├─ /api/v1/*      → proxy to the backend with X-Gateway-Secret, edge rate limits
             └─ /api/internal/* (shared secret only; never reachable by the browser)
                   ├─ d1/query      → D1 binding `DB`      (icp-metadata-staging, D1 Free)
                   └─ objects/<key> → KV binding `OBJECTS` (icp-objects-staging, KV Free)
backend ──► Docker image built from the local tree; staging: operator machine + Quick Tunnel
            (planned: Render Free, blocked without a payment method)
             METADATA_BACKEND=d1_gateway   OBJECT_STORE_BACKEND=gateway   MODEL_SOURCE=bundle
             (no Cloudflare API token, no R2, no Postgres, no MLflow server)
operator machine ──► MLflow (local SQLite + artifacts): train, validate, promote,
                     then `icp-api export-model` writes the promoted bundle into KV via the Worker.
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
  refuses training, validation, promotion and rollback with `409 OPERATION_UNAVAILABLE` in this mode.
- **Render Free** gives a Linux container for Python/ML without a card; it sleeps after 15 min idle
  (first request ≈ up to 60 s; the Worker waits 90 s), has no persistent disk (nothing persistent is
  stored there) and is suspended, not billed, at 750 h/month.
- Excluded: Replit deployments (Reserved VM / Autoscale are paid), Hugging Face Spaces (Docker Spaces
  require a paid plan), Backblaze B2 (pay-as-you-go overage), Neon (not needed: no Postgres).

## Free-tier limits that bound this deployment

| Service | Limit (free) | Behaviour at the limit | Project usage |
|---|---|---|---|
| Workers | 100k requests/day, 10 ms CPU/request, static assets included | errors (HTTP 1015/429), no charge | gateway + data plane; each backend D1/KV call is one Worker request |
| D1 | 5 GB, 5M rows read/day, 100k rows written/day | errors, no charge | metadata; a forecast ≈ 5–10 rows read/written |
| KV | 1 GB, 100k reads/day, 1k writes/day, 1k list/day, 25 MiB/value | errors, no charge | datasets, exports, reports, model bundle. **1k writes/day** = ~1k forecasts + reports per day; a failed export write no longer fails the forecast (`export_available=false`) |
| Render Free | 750 instance-hours/month per workspace, sleeps after 15 min idle, ephemeral FS, **5 GB outbound bandwidth/month** (Hobby workspace, plans of 2026-04-23) | suspended until next month, no charge (no payment method on file) | one web service; only JSON API responses leave Render (static assets are served by Cloudflare) |
| Docker Hub (Personal) | 1 private repository, 100 pulls/h per user | pull refused | one image |

## Resources created (staging)

| Resource | Name / id | Created by |
|---|---|---|
| D1 database | `icp-metadata-staging` / `15fece89-2034-4f83-bc45-a308023e16f4` | `npx wrangler d1 create` (free) |
| KV namespace | `icp-objects-staging` / `067677c60b6946949ed2ff3cf518ea7a` | `npx wrangler kv namespace create` (free) |
| Worker | `icp-gateway-staging` → `https://icp-gateway-staging.icp-gateway.workers.dev` (`apps/gateway/wrangler.jsonc`, env `staging`) | `wrangler deploy --env staging` (deployed 2026-10-17) |
| Render service | `icp-api-staging` | Render CLI, image deploy (`infra/render/README.md`) |

Production uses separate resources (placeholders in `wrangler.jsonc`, `<D1_PRODUCTION_DATABASE_ID>`,
`<KV_PRODUCTION_NAMESPACE_ID>`), created the same way when needed.

## Order of operations

### 1. Secrets (never in files)

```bash
node scripts/py.mjs -m icp_api.cli new-token          # generate values; do not paste them anywhere public
npx wrangler secret put GATEWAY_SHARED_SECRET --env staging -c apps/gateway/wrangler.jsonc
```
The same value goes into the Render service as `GATEWAY_SHARED_SECRET`, and into the operator's
local environment (not `.env` of a committed file) for the export/seed commands below. Admin
tokens: `ADMIN_API_TOKENS=<name>:<token>` on Render only.

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
data plane work; `/api/v1/*` answers 504 until step 5.

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
it can be re-run after local changes. This was executed for staging on 2026-10-17 (2,318 rows,
46 objects; a re-run added only the rows created locally in between). The alternatives are
`icp-api seed` (repository CSVs only, legacy series) or the `ingest_icp` job on staging (slow: ESDM
archive). `export-model` writes the bundle straight into KV through the Worker; re-running it is
idempotent (existing files are skipped, the pointer is refreshed).

### 5. Backend image and Render service

Follow `infra/render/README.md`: build the image from the working tree, push it to the private
Docker Hub repository, add the read-only registry credential in Render once, then create the free
web service with the Render CLI (`render services create --image … --plan free`) and the variables
in `infra/render/render.yaml` (secret values are passed from the shell, never from a file in the
repository). Then:

```bash
curl https://<render-service>.onrender.com/health/live           # may take ~60 s on first wake
curl https://<render-service>.onrender.com/health/ready          # metadata d1_gateway, objects gateway, model_bundle ok
```
Direct `/api/v1/*` calls return `403 GATEWAY_REQUIRED`.

The image (`icp-api:staging-20261017`) was run locally with exactly the Render variable set
(`RENDER=true`, `PORT=10000`, remote D1/KV through the staging Worker, bundle model): startup
≈ 1 s, all readiness checks `ok`, 168 MiB resident memory, no MLflow import, forecasts identical to
the MLflow-mode backend, `train` refused with `409 OPERATION_UNAVAILABLE`.

### 6. Point the Worker at the backend

Put the Render URL into `env.staging.vars.BACKEND_URL` in `apps/gateway/wrangler.jsonc`, redeploy
(`npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc`), then:

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
| `APP_ENV` | strict validation, defaults | backend | no | Render env | yes (`staging`) |
| `PORT` | listen port | backend | no | injected by Render | yes |
| `METADATA_BACKEND` | `d1_gateway` | backend | no | Render env | yes |
| `OBJECT_STORE_BACKEND` | `gateway` (KV through the Worker) | backend | no | Render env | yes |
| `GATEWAY_INTERNAL_URL` | Worker origin for the data plane | backend, operator CLI | no | Render env / shell | yes |
| `GATEWAY_SHARED_SECRET` | authenticates Worker→backend and backend→Worker | Worker, backend, operator CLI | **yes** | `wrangler secret put`, Render env, shell | yes (≥ 32 chars) |
| `REQUIRE_GATEWAY_SECRET` | refuse direct calls | backend | no | Render env | yes (`true`) |
| `MODEL_SOURCE` | `bundle` | backend | no | Render env | yes |
| `MODEL_NAME`, `MODEL_ALIAS` | bundle/registry names | backend, operator CLI | no | Render env | default ok |
| `ADMIN_API_TOKENS` | operator tokens | backend | **yes** | Render env | yes |
| `CORS_ALLOWED_ORIGINS` | the gateway origin | backend | no | Render env | yes |
| `METRICS_TOKEN` | protects `/metrics` | backend | **yes** | container / host env | **yes** (≥ 32 chars; staging and production refuse to start without it) |
| `PUBLIC_REGISTRATION_ENABLED` | self-service sign-up | backend | no | Render env | optional |
| `EIA_API_KEY` | benchmark ingestion | backend | **yes** | Render env | optional |
| `LOG_FORMAT`, `LOG_LEVEL`, `EXPOSE_API_DOCS` | logging / docs | backend | no | Render env | optional |
| `MLFLOW_TRACKING_URI`, `MLFLOW_ARTIFACT_ROOT` | **operator machine only** (training, export) | operator CLI | no | local `.env` | not on Render |
| `D1_*`, `S3_*`, `AWS_*` | REST/R2 transports | – | – | **unused** in this topology | no |

Worker: `BACKEND_URL`, `ENVIRONMENT`, `MAX_BODY_BYTES`, `UPSTREAM_TIMEOUT_MS` (vars in
`wrangler.jsonc`), `GATEWAY_SHARED_SECRET` (secret), bindings `DB`, `OBJECTS`,
`API_RATE_LIMITER`, `FORECAST_RATE_LIMITER`, `ASSETS`.

## Operating the deployment

| Task | How |
|---|---|
| Redeploy the web app / Worker | `npm run build -w @icp/web && npx wrangler deploy --env staging -c apps/gateway/wrangler.jsonc` |
| Redeploy the backend | build + push a new image tag, then Render *Manual Deploy* or the deploy-hook URL |
| Update the model | locally: train job → validate → promote (MLflow), then `icp-api export-model` with the staging store variables; the backend picks the new pointer up within `MODEL_REFRESH_SECONDS` |
| Logs | Render dashboard (backend, JSON with `request_id`); `npx wrangler tail --env staging -c apps/gateway/wrangler.jsonc` (Worker) |
| Backup D1 | `npx wrangler d1 export icp-metadata-staging --remote --output=backup.sql -c infra/cloudflare/d1/wrangler.jsonc`; Time Travel restore is available on the free plan |
| Backup KV | `npx wrangler kv key list --namespace-id 067677c60b6946949ed2ff3cf518ea7a` then `kv key get <key> --namespace-id …` per object (datasets and bundles are write-once; the local MLflow store is the model backup) |
| Smoke test | `npm run smoke -- https://<gateway>` after any change |
| Roll back the Worker | `npx wrangler rollback --env staging -c apps/gateway/wrangler.jsonc` |
| Roll back the model | export the previous version (`icp-api export-model --version 4`) |
| Rotate the shared secret | `wrangler secret put` + Render env + shell; the Worker and backend must change together |

## Backend host: finding of 2026-10-17 (Render refuses card-less workspaces)

With the Render CLI authenticated (workspace `My Workspace`, registry credential for the private
Docker Hub repository in place, image pushed), every attempt to create a service on the free plan
was refused by the Render API with `402 Payment information is required to complete this request`:
image-backed (private and public image, Singapore and Oregon), native Python from a public example
repository and Docker-from-Git alike. No probe service was created. The free-instance documentation
does not mention this requirement; it applies to this workspace as created in October 2026. Under
the project rule "no payment method", Render is therefore **not usable**, and the backend is not
hosted yet. Nothing on Render was created or left behind.

Alternatives checked against official pages the same day:

| Host | Finding | Usable at Rp0 without a card? |
|---|---|---|
| Hugging Face Spaces | "Gradio and Docker Spaces run on compute and require a paid plan to create: PRO" | no |
| Modal | "$30/month free credits" but "you must have a payment method on file in order to use Modal" | no |
| Koyeb | no free plan on the pricing page (Pro $29/month) | no |
| Sevalla | application hosting from $5/month; only static sites are free | no |
| Leapcell | free Hobby plan, Git-triggered deploys only, $1 card verification mentioned | no (Git deploy of the remote tree, card check) |
| Northflank | free sandbox, but "entering a card only verifies it" | no |
| Replit / Fly.io / Railway / Google Cloud Run / Oracle Always Free | paid plan or card required | no |
| Cloudflare Quick Tunnel to a machine you control | free, no account, but "no uptime guarantee", random hostname, "for testing and development" | only as a self-hosted staging demo; blocked by this session's permission policy (external ingress tunnel) |
| Cloudflare Workers Python | free, same platform, but FastAPI + pandas/scikit-learn inside the 10 ms CPU limit and without OCR/PDF tooling means rewriting the serving API | not a minimal change |

Decision for the owner (the runbook below stays valid for whichever is chosen):

1. **Add a card to Render and keep the free plan** (Render documents that free services are
   suspended, not billed, when quotas are exceeded; the card only has to exist). Then run the
   `render services create … --plan free` command in `infra/render/README.md`; everything else is
   ready. This contradicts the current "no payment method" rule, so it is the owner's call.
2. **Self-host the backend container** on any always-on machine you control (a PC, a NAS, a free
   student/office VM) and expose it with a named Cloudflare Tunnel (needs a domain on Cloudflare)
   or a Quick Tunnel (testing only); the Worker's `BACKEND_URL` then points at that hostname.
3. **Accept a Git-based free host** (Leapcell, Back4app Containers) – requires pushing the current
   tree to GitHub yourself (this project forbids automated Git operations) and, for Leapcell, a
   card verification.

## Self-hosted backend through a Quick Tunnel (the topology running since 2026-10-17)

Chosen by the owner after the Render finding: the backend container runs on the operator's
machine and is published through a **Cloudflare Quick Tunnel** (free, no account, no card;
Cloudflare's own words: "no uptime guarantee", "for testing and development", a new hostname on
every start, up to 200 in-flight requests). The Worker, D1, KV and the model bundle are unchanged;
only `BACKEND_URL` points at the tunnel.

```bash
npm run staging:backend                               # container + tunnel + Worker deploy
node scripts/staging-backend.mjs --deploy-only        # redeploy the Worker (e.g. after a web build) with the saved URL
```
The tunnel hostname is machine-local state: it is kept in `var/staging-backend-url.txt`
(gitignored) and passed to Wrangler with `--var`, so it never lands in the repository.
`apps/gateway/wrangler.jsonc` keeps the placeholder `https://<staging-backend-origin>`; a plain
`npx wrangler deploy --env staging` would therefore leave the API answering
`503 GATEWAY_MISCONFIGURED` - always deploy staging through the script.
What it does, step by step (also usable by hand):

1. `docker run -d --name icp-api-staging --restart unless-stopped --env-file var/secrets/staging.render.env -p 127.0.0.1:18000:10000 icp-api:staging-20261017`
   (the same variables as the Render plan; `PORT=10000` inside, bound to localhost only).
2. `npx wrangler tunnel quick-start http://localhost:18000` (from `apps/gateway`) prints the
   `https://<random>.trycloudflare.com` hostname; the process must stay alive.
3. Save that hostname to `var/staging-backend-url.txt` and deploy with
   `npx wrangler deploy --env staging --var BACKEND_URL:<hostname>` (from `apps/gateway`).
4. `npm run smoke -- https://icp-gateway-staging.icp-gateway.workers.dev`.

Consequences: if the machine sleeps, the tunnel process stops or the network drops, the Worker
answers `504`/`502` for `/api/v1/*` (the web app shows its error state); assets, D1 and KV stay
available, and no data is lost (nothing persistent lives on the machine except the local
development stores, which the staging backend does not use). Restarting = running the script again
(new hostname, automatic redeploy). The backend itself still refuses direct calls through the tunnel
without the gateway secret (`403 GATEWAY_REQUIRED`); the health endpoints are public by design.

Measured through the public URL on 2026-10-17: 11/11 smoke checks and 18/18 feature checks
(registration, login, profile, ICP summary, price history, forecast with scenario on bundle v5,
CSV export from KV, scenario save/list/compare in D1, market report PDF + CSV generation and
download, private routes and internal routes refused without credentials, direct backend access
refused, logout invalidates the session). Typical latency: SPA routes 0.2–0.4 s, cached API reads
≈ 1 s, the public market snapshot ≈ 5 s (many D1/KV round trips through the Worker; a first
optimisation target).

## What remains manual (consent flows only)

- `docker login` (device-code flow in the browser) before the first `docker push`.
- `render login` (opens the dashboard, "Authorize CLI"); afterwards the CLI creates the service,
  sets variables and triggers deploys without the dashboard.
- Creating the Docker Hub **read-only** access token and entering it once as a Render registry
  credential (Workspace Settings → Container Registry Credentials). Public images need no credential
  but would publish the backend source; a private repository is the default here.
- Blueprints are not used: Render reads them only from a connected Git repository, which would
  deploy the remote tree instead of the local working tree.

## Verification status (2026-10-17)

Executed against the account: D1 migrations 0001–0004 applied remotely (schema v4, 19 tables);
`wrangler secret put GATEWAY_SHARED_SECRET --env staging`; `wrangler deploy --env staging`
(41 assets); the deployed Worker answers `/` and `/app/*` (SPA), `/api/gateway/health`,
`/api/internal/health` → `{"d1": true, "kv": true}` with the secret and `401` without it;
`clone-state` and `export-model` through the Worker; a local backend process and the Docker image in
the Render configuration served readiness, data summary, latest forecast and new forecasts from the
remote stores. The new `workers.dev` hostname answered TLS handshake failures for about a minute
after the first deploy (certificate provisioning), then worked.

Also executed later the same day: `docker push` of `benedictreyno/icp-api:staging-20261017` to the
private Docker Hub repository (anonymous API answers 404 for the repository and its tags; the
authenticated client sees the linux/amd64 manifest); `render login`, workspace selection and the
registry credential check through the Render API; the Worker redeployed with a `503
GATEWAY_MISCONFIGURED` answer while `BACKEND_URL` is a placeholder (version `95164202…`).

Not yet verified: the backend on a public host (see the finding above), the end-to-end browser flow
through the gateway, cold-start latency, and KV eventual consistency under concurrent write-once
puts (keys are content-addressed or unique, so a lost race stores identical bytes).

## Rollback and recovery

| Problem | Action |
|---|---|
| Bad model | `icp-api export-model --version <previous>` (pointer moves back; nothing is deleted) |
| Bad dataset import | Data → versions → Activate the previous version |
| Bad Worker release | `npx wrangler rollback --env staging` |
| Bad backend release | Render → deploy the previous image tag |
| Bad D1 migration | D1 Time Travel restore, then fix forward |
| Leaked secret | rotate `GATEWAY_SHARED_SECRET` (Worker + Render + shell), admin tokens; registry token in Render |
