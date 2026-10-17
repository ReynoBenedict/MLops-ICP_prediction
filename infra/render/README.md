# Backend on Render Free (image deploy)

> **Finding 2026-10-17:** on this workspace the Render API refused every free-plan service
> creation with `402 Payment information is required` (image-backed and Git-backed alike) until a
> card is added. The steps below are verified up to and including the image push and the registry
> credential; the service creation needs the owner's decision on the card first
> (`docs/DEPLOYMENT.md`, "Backend host: finding of 2026-10-17").

The FastAPI backend runs on a **Render Free web service** created from a container image that is
built locally from the working tree. No Git connection, no Blueprint sync, no payment method.

Facts from the Render documentation (checked 2026-10-17): free web services get 750 instance-hours
per workspace per month, spin down after 15 minutes without inbound traffic and take up to about a
minute to serve the first request afterwards; their filesystem is ephemeral (no persistent disk on
the free plan); image-backed services can be deployed from Docker Hub, GHCR or another registry
without a repository; a workspace without a payment method has its free services suspended when the
quota is reached, it is never billed. Instance size for the free plan is small (512 MB class);
confirm the current figure in the dashboard when creating the service.

Everything persistent is outside Render: metadata in Cloudflare D1, objects (datasets, exports,
reports, the model bundle) in Cloudflare KV, both reached through the gateway Worker's internal data
plane (`METADATA_BACKEND=d1_gateway`, `OBJECT_STORE_BACKEND=gateway`). The model is served from an
exported bundle (`MODEL_SOURCE=bundle`); MLflow runs only on the operator machine.

Outbound bandwidth: a Hobby (free) workspace includes **5 GB/month** (Render's plans of
2026-04-23); only API responses to the Cloudflare Worker count, static assets never leave Render.
Without a payment method, exceeding it suspends the free services until the next month; nothing is
billed.

## 1. Build and push the image (operator machine)

```bash
docker build -f infra/docker/api.Dockerfile -t icp-api:staging-YYYYMMDD .
docker login            # device-code flow: a browser page asks for consent; nothing is typed here
docker tag icp-api:staging-YYYYMMDD docker.io/<dockerhub-user>/icp-api:staging-YYYYMMDD
docker push docker.io/<dockerhub-user>/icp-api:staging-YYYYMMDD
```

Use a **private** repository on Docker Hub (the Personal plan includes one private repository and
100 pulls/hour) or GHCR. Tag each release (`:staging-YYYYMMDD`) so Render can roll back to an earlier
image. The image contains code, the seed CSVs and the migrations, but no `.env`, no `.dev.vars`
and no secret (`.dockerignore`).

## 2. Registry credential (Render dashboard, once)

Workspace Settings → Container Registry Credentials → add Docker Hub username + a **read-only**
personal access token (Docker Hub → Account settings → Personal access tokens, permission
"Read-only"). This is the only dashboard step; the CLI cannot create credentials. Note the
credential id (`rgc_…`, shown by `render services create --help` examples or the dashboard URL).

## 3. Service (Render CLI, installed with `winget install Render.CLI`)

```bash
render login                                  # opens the dashboard: click "Authorize CLI"
render workspace set                          # pick the (free) workspace once
render services create --type web_service --runtime image --name icp-api-staging \
  --image docker.io/<dockerhub-user>/icp-api:staging-YYYYMMDD --registry-credential <rgc_id> \
  --plan free --region singapore --health-check-path /health/live \
  --env-var APP_ENV=staging --env-var METADATA_BACKEND=d1_gateway --env-var OBJECT_STORE_BACKEND=gateway \
  --env-var GATEWAY_INTERNAL_URL=https://icp-gateway-staging.icp-gateway.workers.dev \
  --env-var CORS_ALLOWED_ORIGINS=https://icp-gateway-staging.icp-gateway.workers.dev \
  --env-var MODEL_SOURCE=bundle --env-var REQUIRE_GATEWAY_SECRET=true --env-var LOG_FORMAT=json \
  --env-var "GATEWAY_SHARED_SECRET=$(cat var/secrets/staging.gateway_secret)" \
  --env-var "ADMIN_API_TOKENS=$(cat var/secrets/staging.admin_tokens)" \
  --confirm --output json
```

Secret values are read from files under `var/secrets/` (gitignored) by the shell; they are never
written into the repository or printed. Render pulls the image and starts
`python -m icp_api.cli serve`, which binds `0.0.0.0:$PORT` (`PORT` is injected by Render).

The service URL (`https://icp-api-staging[-xxxx].onrender.com`) is then put into
`apps/gateway/wrangler.jsonc` → `env.staging.vars.BACKEND_URL`, and the Worker is redeployed.

## 3b. Update / redeploy

```bash
render deploys create <service-id> --image docker.io/<dockerhub-user>/icp-api:staging-YYYYMMDD --wait
```
Image-backed services do not redeploy on their own when a tag is pushed. Rollback = deploy the
previous tag. `render logs <service-id>` streams the JSON logs.

## 4. Cold starts

After 15 idle minutes the instance sleeps; the first request takes up to ~60 s. The Worker's
`UPSTREAM_TIMEOUT_MS` for staging/production is 90 s so that request succeeds instead of failing at
the edge. The web app keeps its loading state during that time. If the wake must be hidden from
users, a free external uptime pinger (every 10–14 minutes against `/health/live`) keeps the service
awake but consumes the 750 h/month quota faster (one always-on service fits: 31 × 24 = 744 h).

## 5. Logs

Dashboard → service → Logs (JSON lines with `request_id`; see docs/OPERATIONS.md). There is no
log retention beyond what the free plan shows; copy anything you need to keep.

## 6. Jobs on a sleeping instance

Background jobs run inside the API process. Submit ingestion jobs while the service is awake (the
submitting request wakes it) and keep them within their timeouts; if the instance is restarted
mid-job the stale-job reaper marks the job `INTERRUPTED` and it can be retried. Training is refused
in bundle mode: train and promote locally, then `icp-api export-model` (see docs/DEPLOYMENT.md).
