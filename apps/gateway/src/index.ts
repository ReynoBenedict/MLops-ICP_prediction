/**
 * ICP gateway Worker.
 *
 * - Static assets (the React app) are served by Workers Static Assets; this script only runs for
 *   `/api/*` (see `assets.run_worker_first` in wrangler.jsonc).
 * - `/api/v1/*` is forwarded to the Python backend with a shared secret the browser never sees, a
 *   request ID, the client IP, a body-size limit, an upstream timeout and edge rate limits.
 * - `/api/internal/*` is the **data plane for the backend**: D1 queries and KV objects through the
 *   Worker's bindings, so the backend needs no Cloudflare API token (zero-cost topology, see
 *   docs/DEPLOYMENT.md). Only callers presenting the same shared secret can reach it; the browser
 *   never can (it does not know the secret). These routes bypass the proxy and the edge rate limits
 *   (they are backend-to-Worker traffic, not client traffic) and never touch BACKEND_URL.
 * - No business logic lives here: validation, authorization and all domain rules are enforced by the
 *   backend. The gateway only decides *whether* a request may reach it.
 *
 * Free-plan limits that bound the data plane (Cloudflare docs, 2026-10): Workers 100k requests/day;
 * D1 5M rows read/day, 100k rows written/day, 5 GB; KV 100k reads/day, 1k writes/day, 1k list/day,
 * 1 GB, 25 MiB per value. Exceeding a limit returns errors, never charges.
 */

export interface Env {
  ASSETS?: Fetcher;
  BACKEND_URL?: string;
  GATEWAY_SHARED_SECRET?: string;
  ENVIRONMENT?: string;
  MAX_BODY_BYTES?: string;
  UPSTREAM_TIMEOUT_MS?: string;
  API_RATE_LIMITER?: RateLimit;
  FORECAST_RATE_LIMITER?: RateLimit;
  /** D1 binding (application metadata) - internal data plane only. */
  DB?: D1Database;
  /** KV binding (datasets, exports, model bundles) - internal data plane only. */
  OBJECTS?: KVNamespace;
}

const ALLOWED_METHODS = new Set(["GET", "HEAD", "POST", "OPTIONS"]);
const FORWARDED_REQUEST_HEADERS = ["accept", "content-type", "authorization", "if-none-match"];
const DROPPED_RESPONSE_HEADERS = ["connection", "keep-alive", "transfer-encoding", "server", "x-powered-by", "via"];
const REQUEST_ID = /^[A-Za-z0-9._:-]{8,128}$/;
const EXPORT_PATH = /^\/api\/v1\/forecasts\/[A-Za-z0-9_]+\/export\.csv$/;
const INTERNAL_PREFIX = "/api/internal/";
const OBJECT_KEY = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$/; // mirrors icp_api.storage.objects.validate_key
const MAX_SQL_CHARS = 100_000;
const MAX_SQL_PARAMS = 200;
const MAX_OBJECT_BYTES = 25 * 1024 * 1024; // KV value limit

export function errorResponse(status: number, code: string, message: string, requestId: string,
                              extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify({ error: { code, message, request_id: requestId, details: {} } }), {
    status,
    headers: { "content-type": "application/json", "x-request-id": requestId, "cache-control": "no-store", ...extra },
  });
}

function apiSecurityHeaders(headers: Headers, requestId: string): Headers {
  for (const h of DROPPED_RESPONSE_HEADERS) headers.delete(h);
  headers.set("x-request-id", requestId);
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "no-referrer");
  headers.set("x-frame-options", "DENY");
  return headers;
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function rateLimitKey(request: Request): Promise<string> {
  const auth = request.headers.get("authorization");
  if (auth) return `auth:${(await sha256Hex(auth)).slice(0, 24)}`; // never use the token itself as a key
  return `ip:${request.headers.get("cf-connecting-ip") ?? "unknown"}`;
}

async function checkRateLimit(request: Request, env: Env, url: URL): Promise<boolean> {
  const isForecast = request.method === "POST" && url.pathname === "/api/v1/forecasts";
  const limiter = isForecast ? env.FORECAST_RATE_LIMITER : env.API_RATE_LIMITER;
  if (!limiter) return true; // binding not configured (e.g. unit tests); the backend still limits
  const { success } = await limiter.limit({ key: `${isForecast ? "fc" : "api"}:${await rateLimitKey(request)}` });
  return success;
}

async function fetchUpstream(upstream: Request, timeoutMs: number, retries: number): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort("timeout"), timeoutMs);
    try {
      return await fetch(upstream.clone(), { signal: controller.signal });
    } catch (err) {
      // Only connection failures of idempotent requests are retried; timeouts never are.
      if (controller.signal.aborted || attempt >= retries) throw err;
    } finally {
      clearTimeout(timer);
    }
  }
}

// ---------------------------------------------------------------------------------------------
// Internal data plane (backend -> Worker -> D1 / KV)
// ---------------------------------------------------------------------------------------------

/** True when BACKEND_URL is an absolute http(s) URL and not a `<placeholder>`. */
export function backendConfigured(value: string | undefined): boolean {
  if (!value || value.includes("<") || value.includes(">")) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "https:" || parsed.protocol === "http:";
  } catch {
    return false;
  }
}

/** Constant-time comparison of the presented secret with the configured one. */
export async function secretMatches(presented: string | null, expected: string | undefined): Promise<boolean> {
  if (!presented || !expected || expected.length < 32) return false;
  // Hashing first makes the comparison length-independent (no length information leaks); the
  // digests are then compared without early exit. `crypto.subtle.timingSafeEqual` exists on
  // Workers but not in every Node runtime used for tests, so the loop is written out.
  const [a, b] = await Promise.all([sha256Hex(presented), sha256Hex(expected)]);
  let diff = a.length ^ b.length;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i % b.length);
  return diff === 0;
}

function jsonResponse(status: number, body: unknown, requestId: string, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: apiSecurityHeaders(new Headers({ "content-type": "application/json", "cache-control": "no-store", ...extra }), requestId),
  });
}

/** D1 REST-API-shaped envelope, so the backend's `D1HttpExecutor` parser is reused unchanged. */
function d1Envelope(results: unknown[], changes: number, duration: number): unknown {
  return { success: true, errors: [], messages: [], result: [{ success: true, results, meta: { changes, duration } }] };
}

function d1ErrorEnvelope(message: string): unknown {
  return { success: false, errors: [{ code: 7500, message }], messages: [], result: [] };
}

type SqlParam = string | number | boolean | null;

function isSqlParam(v: unknown): v is SqlParam {
  return v === null || typeof v === "string" || typeof v === "number" || typeof v === "boolean";
}

async function handleD1Query(request: Request, env: Env, requestId: string): Promise<Response> {
  if (!env.DB) return errorResponse(503, "DATA_PLANE_MISCONFIGURED", "No D1 binding on this Worker.", requestId);
  if (request.method !== "POST") {
    return errorResponse(405, "METHOD_NOT_ALLOWED", "Method not allowed.", requestId, { allow: "POST" });
  }
  let body: { sql?: unknown; params?: unknown };
  try {
    body = (await request.json()) as { sql?: unknown; params?: unknown };
  } catch {
    return jsonResponse(400, d1ErrorEnvelope("request body is not JSON"), requestId);
  }
  const sql = body.sql;
  const params = body.params ?? [];
  if (typeof sql !== "string" || !sql.trim() || sql.length > MAX_SQL_CHARS) {
    return jsonResponse(400, d1ErrorEnvelope("sql must be a non-empty string"), requestId);
  }
  if (!Array.isArray(params) || params.length > MAX_SQL_PARAMS || !params.every(isSqlParam)) {
    return jsonResponse(400, d1ErrorEnvelope("params must be an array of string/number/boolean/null"), requestId);
  }
  const started = Date.now();
  try {
    const result = await env.DB.prepare(sql).bind(...(params as SqlParam[])).all();
    const changes = Number(result.meta?.changes ?? 0);
    return jsonResponse(200, d1Envelope(result.results ?? [], changes, Date.now() - started), requestId);
  } catch (err) {
    // D1 raises for SQL errors incl. constraint violations ("UNIQUE constraint failed: ..."); the
    // backend maps the message to IntegrityError/StorageError exactly as for the REST API.
    const message = err instanceof Error ? err.message : String(err);
    return jsonResponse(400, d1ErrorEnvelope(message), requestId);
  }
}

async function handleObject(request: Request, env: Env, rawKey: string, requestId: string): Promise<Response> {
  if (!env.OBJECTS) return errorResponse(503, "DATA_PLANE_MISCONFIGURED", "No KV binding on this Worker.", requestId);
  let key: string;
  try {
    key = decodeURIComponent(rawKey);
  } catch {
    return errorResponse(400, "INVALID_OBJECT_KEY", "Object key is not valid.", requestId);
  }
  if (!OBJECT_KEY.test(key) || key.includes("..") || key.includes("//") || key.endsWith("/")) {
    return errorResponse(400, "INVALID_OBJECT_KEY", "Object key is not valid.", requestId);
  }
  if (request.method === "PUT") {
    const declared = Number(request.headers.get("content-length") ?? "0");
    if (declared > MAX_OBJECT_BYTES) {
      return errorResponse(413, "PAYLOAD_TOO_LARGE", `Object exceeds ${MAX_OBJECT_BYTES} bytes.`, requestId);
    }
    const data = await request.arrayBuffer();
    if (data.byteLength > MAX_OBJECT_BYTES) {
      return errorResponse(413, "PAYLOAD_TOO_LARGE", `Object exceeds ${MAX_OBJECT_BYTES} bytes.`, requestId);
    }
    if (request.headers.get("if-none-match") === "*") {
      // Write-once. KV is eventually consistent, so this is best-effort; the backend only writes
      // content-addressed or uniquely named keys, which makes a lost race harmless.
      const existing = await env.OBJECTS.getWithMetadata(key, "stream");
      if (existing.value !== null) {
        await existing.value.cancel();
        return errorResponse(412, "OBJECT_EXISTS", "Object already exists.", requestId);
      }
    }
    const contentType = request.headers.get("content-type") ?? "application/octet-stream";
    await env.OBJECTS.put(key, data, { metadata: { contentType, size: data.byteLength } });
    return jsonResponse(201, { key, size: data.byteLength }, requestId);
  }
  if (request.method === "GET" || request.method === "HEAD") {
    const found = await env.OBJECTS.getWithMetadata<{ contentType?: string; size?: number }>(key, "arrayBuffer");
    if (found.value === null) return errorResponse(404, "OBJECT_NOT_FOUND", "Object not found.", requestId);
    const headers = apiSecurityHeaders(new Headers({
      "content-type": found.metadata?.contentType ?? "application/octet-stream",
      "content-length": String(found.value.byteLength),
      "cache-control": "no-store",
    }), requestId);
    return new Response(request.method === "HEAD" ? null : found.value, { status: 200, headers });
  }
  return errorResponse(405, "METHOD_NOT_ALLOWED", "Method not allowed.", requestId, { allow: "GET, HEAD, PUT" });
}

export async function handleInternal(request: Request, env: Env, requestId: string): Promise<Response> {
  const url = new URL(request.url);
  if (!env.GATEWAY_SHARED_SECRET) {
    return errorResponse(503, "GATEWAY_MISCONFIGURED", "The API gateway is not configured.", requestId);
  }
  if (!(await secretMatches(request.headers.get("x-gateway-secret"), env.GATEWAY_SHARED_SECRET))) {
    return errorResponse(401, "GATEWAY_AUTH_REQUIRED", "Internal routes require the gateway secret.", requestId);
  }
  const rest = url.pathname.slice(INTERNAL_PREFIX.length);
  if (rest === "health") {
    return jsonResponse(200, { status: "ok", environment: env.ENVIRONMENT ?? "unknown", d1: Boolean(env.DB), kv: Boolean(env.OBJECTS) }, requestId);
  }
  if (rest === "d1/query") return handleD1Query(request, env, requestId);
  if (rest.startsWith("objects/") && rest.length > "objects/".length) {
    return handleObject(request, env, rest.slice("objects/".length), requestId);
  }
  return errorResponse(404, "NOT_FOUND", "Unknown internal path.", requestId);
}

export async function handleApi(request: Request, env: Env, ctx: ExecutionContext, requestId: string): Promise<Response> {
  const url = new URL(request.url);

  if (url.pathname === "/api/gateway/health") {
    return new Response(JSON.stringify({ status: "ok", environment: env.ENVIRONMENT ?? "unknown",
                                         backend_configured: backendConfigured(env.BACKEND_URL) }),
      { headers: apiSecurityHeaders(new Headers({ "content-type": "application/json", "cache-control": "no-store" }), requestId) });
  }
  if (url.pathname.startsWith(INTERNAL_PREFIX)) {
    return handleInternal(request, env, requestId);
  }
  if (!url.pathname.startsWith("/api/v1/")) {
    return errorResponse(404, "NOT_FOUND", "Unknown API path.", requestId);
  }
  if (!ALLOWED_METHODS.has(request.method)) {
    return errorResponse(405, "METHOD_NOT_ALLOWED", "Method not allowed.", requestId, { allow: [...ALLOWED_METHODS].join(", ") });
  }
  if (!env.BACKEND_URL || !env.GATEWAY_SHARED_SECRET || !backendConfigured(env.BACKEND_URL)) {
    // Covers the `https://<placeholder>` values kept in wrangler.jsonc until a backend exists:
    // a clear 503 instead of a URL-parse failure surfacing as an unexpected 500.
    return errorResponse(503, "GATEWAY_MISCONFIGURED", "The API backend is not configured for this environment.", requestId);
  }

  const maxBody = Number(env.MAX_BODY_BYTES ?? "2000000");
  const declared = Number(request.headers.get("content-length") ?? "0");
  if (declared > maxBody) {
    return errorResponse(413, "PAYLOAD_TOO_LARGE", `Request body exceeds ${maxBody} bytes.`, requestId);
  }
  let body: ArrayBuffer | undefined;
  if (request.method === "POST") {
    body = await request.arrayBuffer();
    if (body.byteLength > maxBody) {
      return errorResponse(413, "PAYLOAD_TOO_LARGE", `Request body exceeds ${maxBody} bytes.`, requestId);
    }
  }

  if (!(await checkRateLimit(request, env, url))) {
    return errorResponse(429, "RATE_LIMITED", "Too many requests. Slow down and retry later.", requestId, { "retry-after": "60" });
  }

  // Immutable forecast exports can be served from the edge cache (anonymous GETs only).
  const cacheable = request.method === "GET" && EXPORT_PATH.test(url.pathname) && !request.headers.has("authorization")
    && typeof caches !== "undefined";
  if (cacheable) {
    const hit = await caches.default.match(request);
    if (hit) return new Response(hit.body, { status: hit.status, headers: apiSecurityHeaders(new Headers(hit.headers), requestId) });
  }

  const target = new URL(url.pathname + url.search, env.BACKEND_URL);
  const headers = new Headers();
  for (const name of FORWARDED_REQUEST_HEADERS) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  headers.set("x-request-id", requestId);
  headers.set("x-gateway-secret", env.GATEWAY_SHARED_SECRET); // replaces anything the client sent
  headers.set("cf-connecting-ip", request.headers.get("cf-connecting-ip") ?? "unknown");
  headers.set("x-forwarded-proto", url.protocol.replace(":", ""));
  headers.set("x-forwarded-host", url.host);

  const upstream = new Request(target.toString(), { method: request.method, headers, body, redirect: "manual" });
  const idempotent = request.method === "GET" || request.method === "HEAD";
  let response: Response;
  try {
    response = await fetchUpstream(upstream, Number(env.UPSTREAM_TIMEOUT_MS ?? "30000"), idempotent ? 1 : 0);
  } catch {
    return errorResponse(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE",
      "The forecasting backend did not respond. Try again shortly.", requestId);
  }
  if (response.status >= 300 && response.status < 400) {
    return errorResponse(502, "UPSTREAM_REDIRECT", "Unexpected redirect from the backend.", requestId);
  }

  const out = new Response(response.body, { status: response.status, headers: apiSecurityHeaders(new Headers(response.headers), requestId) });
  if (cacheable && response.status === 200 && (response.headers.get("cache-control") ?? "").includes("public")) {
    ctx.waitUntil(caches.default.put(request, out.clone()));
  }
  return out;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const incoming = request.headers.get("x-request-id") ?? "";
    const requestId = REQUEST_ID.test(incoming) ? incoming : `gw_${crypto.randomUUID().replaceAll("-", "")}`;
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      try {
        return await handleApi(request, env, ctx, requestId);
      } catch {
        return errorResponse(500, "GATEWAY_ERROR", "Unexpected gateway error.", requestId);
      }
    }
    // Non-API paths normally never reach the Worker (assets are served first); keep a safe fallback.
    if (env.ASSETS) return env.ASSETS.fetch(request);
    return errorResponse(404, "NOT_FOUND", "Not found.", requestId);
  },
} satisfies ExportedHandler<Env>;
