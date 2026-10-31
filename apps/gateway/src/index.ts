/**
 * ICP gateway Worker.
 *
 * - Static assets (the React app) are served by Workers Static Assets. This script runs first for `/api/*`
 *   and for page documents (`assets.run_worker_first` in wrangler.jsonc: every path except the static
 *   folders `/assets`, `/brand`, `/fonts`, `/img` and `/favicon.svg`, which never invoke it). A page is
 *   answered with the asset as served, plus the site's security headers when the platform left them off.
 * - `/api/v1/*` is forwarded to the Python backend with a shared secret the browser never sees, a
 *   request ID, the client IP, a body-size limit, an upstream timeout and edge rate limits.
 * - `/api/internal/*` is the **data plane for the backend**: D1 queries and KV objects through the
 *   Worker's bindings, so the backend needs no Cloudflare API token (zero-cost topology, see
 *   docs/DEPLOYMENT.md). Only callers presenting the same shared secret can reach it; the browser
 *   never can (it does not know the secret). These routes bypass the proxy and the edge rate limits
 *   (they are backend-to-Worker traffic, not client traffic) and never touch BACKEND_URL.
 * - No business logic lives here: validation, authorization and all domain rules are enforced by the
 *   backend. The gateway only decides *whether* a request may reach it.
 * - One exception to "no edge caching": the anonymous public data (`GET /api/v1/public/market-snapshot`
 *   and `GET /api/v1/plans`, no Authorization header, no query string) is kept as a last-known-good
 *   copy in KV under `edge-cache/public/<name>` and served, marked stale, while the backend is
 *   unreachable (contract C2, docs/OPERATIONS.md "Stale public data").
 * - Upstream error answers (4xx/5xx) that are not JSON (Cloudflare 52x/530 HTML pages, edge or proxy
 *   error pages) are replaced by the standard error envelope, so `/api/` never hands the browser an HTML
 *   error page. Successful answers pass through as the backend sent them (file downloads are not JSON).
 * - Serverless backend (Vercel Hobby, docs/DEPLOYMENT.md): when the Worker serves the SPA document of a
 *   browser's navigation (Fetch Metadata; never for crawlers, uptime monitors or scripts) it sends one
 *   non-blocking `GET /health/live` to the backend (at most once a minute per isolate), so the visitor's
 *   first API call meets a starting instance; and a daily Cron Trigger
 *   refreshes the two last-known-good public copies through the same path as a visitor's request. There
 *   is deliberately no frequent keep-warm (it would spend the backend's free compute allowance).
 *
 * Free-plan limits that bound the data plane (Cloudflare docs, 2026-10): Workers 100k requests/day;
 * D1 5M rows read/day, 100k rows written/day, 5 GB; KV 100k reads/day, 1k writes/day, 1k list/day,
 * 1 GB, 25 MiB per value. Exceeding a limit returns errors, never charges.
 */

export interface Env {
  ASSETS?: Fetcher;
  BACKEND_URL?: string;
  GATEWAY_SHARED_SECRET?: string;
  /** Previous secrets still accepted on `/api/internal/*` during a rotation (comma-separated). */
  GATEWAY_ACCEPTED_SECRETS?: string;
  ENVIRONMENT?: string;
  MAX_BODY_BYTES?: string;
  UPSTREAM_TIMEOUT_MS?: string;
  /** Upstream timeout for the anonymous public routes that have a last-known-good copy (default 8000). */
  PUBLIC_UPSTREAM_TIMEOUT_MS?: string;
  API_RATE_LIMITER?: RateLimit;
  FORECAST_RATE_LIMITER?: RateLimit;
  /** D1 binding (application metadata) - internal data plane only. */
  DB?: D1Database;
  /** KV binding (datasets, exports, model bundles) - internal data plane; the Worker itself also
   *  keeps the last-known-good public answers under the reserved `edge-cache/` prefix. */
  OBJECTS?: KVNamespace;
}

const DEFAULT_MAX_BODY_BYTES = 2_000_000;
const DEFAULT_UPSTREAM_TIMEOUT_MS = 30_000;
const DEFAULT_PUBLIC_UPSTREAM_TIMEOUT_MS = 8_000;
const DEPLOYED_ENVIRONMENTS = new Set(["staging", "production"]);

/**
 * A numeric Worker variable, or the default when it is missing or not a positive integer. A typo must
 * not silently disable a limit (`Number("") > n` is always false) or abort every request (a NaN
 * timeout fires immediately).
 */
export function positiveIntVar(value: string | undefined, fallback: number): number {
  if (value === undefined || !/^\s*\d+\s*$/.test(value)) return fallback;
  const n = Number(value);
  return Number.isSafeInteger(n) && n > 0 ? n : fallback;
}

const ALLOWED_METHODS = new Set(["GET", "HEAD", "POST", "OPTIONS"]);
// `idempotency-key` lets POST /forecasts and /reports be retried safely (backend DATA-02).
const FORWARDED_REQUEST_HEADERS = ["accept", "content-type", "authorization", "if-none-match", "idempotency-key"];
const DROPPED_RESPONSE_HEADERS = ["connection", "keep-alive", "transfer-encoding", "server", "x-powered-by", "via"];
const REQUEST_ID = /^[A-Za-z0-9._:-]{8,128}$/;
const INTERNAL_PREFIX = "/api/internal/";
const OBJECT_KEY = /^[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$/; // mirrors icp_api.storage.objects.validate_key
const MAX_SQL_CHARS = 100_000;
// Cloudflare D1 limit: max bound parameters per query (developers.cloudflare.com/d1/platform/limits, 2026-10-20).
const MAX_SQL_PARAMS = 100;
/** KV retention for generated exports, mirroring the R2 lifecycle rules in docs/STORAGE.md
 *  (forecast exports 180 days, report files 365 days; the D1 records stay). */
export const EXPORT_TTL_SECONDS: ReadonlyArray<[string, number]> = [
  ["exports/forecasts/", 180 * 86_400],
  ["exports/reports/", 365 * 86_400],
];
/** Header carrying the visitor address to the backend (see `handleApi`). */
export const CLIENT_IP_HEADER = "x-icp-client-ip";
/** Statement kinds the backend issues through the data plane; everything else (DDL, ATTACH,
 *  PRAGMA writes, VACUUM, transactions, ...) is refused before it reaches D1. */
const SQL_ALLOWED_START = /^(SELECT|INSERT|UPDATE|DELETE|WITH)\b/i;
/** The one PRAGMA the backend uses (clone-state reads a table's columns). */
const SQL_PRAGMA_TABLE_INFO = /^PRAGMA\s+table_info\(\s*"?[A-Za-z_][A-Za-z0-9_]*"?\s*\)\s*;?\s*$/i;
const SQL_FORBIDDEN_WORD = /\b(ATTACH|DETACH|PRAGMA|VACUUM|REINDEX|ANALYZE|CREATE|DROP|ALTER|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b/i;
const MAX_OBJECT_BYTES = 25 * 1024 * 1024; // KV value limit

export function errorResponse(status: number, code: string, message: string, requestId: string,
                              extra: Record<string, string> = {}): Response {
  // The Worker's own replies (404/405/413/429/5xx) carry the same security headers as proxied ones.
  return new Response(JSON.stringify({ error: { code, message, request_id: requestId, details: {} } }), {
    status,
    headers: apiSecurityHeaders(new Headers({ "content-type": "application/json", "cache-control": "no-store", ...extra }), requestId),
  });
}

function apiSecurityHeaders(headers: Headers, requestId: string): Headers {
  for (const h of DROPPED_RESPONSE_HEADERS) headers.delete(h);
  headers.set("x-request-id", requestId);
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "no-referrer");
  headers.set("x-frame-options", "DENY");
  // API responses are JSON or file downloads: nothing may render, frame or embed them cross-origin.
  headers.set("content-security-policy", "default-src 'none'; frame-ancestors 'none'");
  headers.set("cross-origin-resource-policy", "same-origin");
  headers.set("strict-transport-security", "max-age=31536000; includeSubDomains");
  return headers;
}

async function sha256Hex(value: string | ArrayBuffer): Promise<string> {
  const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** Session tokens and API keys have fixed prefixes; anything else is not a customer credential. */
const KNOWN_CREDENTIAL = /^Bearer (icp_sess_|icp_live_)[A-Za-z0-9_-]{20,}$/;

/**
 * Limiter keys for one request. The client IP bucket always applies, so rotating junk
 * Authorization headers can never produce a fresh bucket. A well-formed session token or API key
 * additionally gets its own bucket (one credential cannot exceed the limit from many addresses).
 * Sign-in, registration and password changes use a separate IP bucket.
 */
export async function rateLimitKeys(request: Request, url: URL): Promise<string[]> {
  const ip = `ip:${request.headers.get("cf-connecting-ip") ?? "unknown"}`;
  if (url.pathname.startsWith("/api/v1/auth/")) return [`signin:${ip}`];
  const keys = [ip];
  const auth = request.headers.get("authorization");
  if (auth && KNOWN_CREDENTIAL.test(auth)) keys.push(`auth:${(await sha256Hex(auth)).slice(0, 24)}`); // never the token itself
  return keys;
}

async function checkRateLimit(request: Request, env: Env, url: URL): Promise<boolean> {
  const isForecast = request.method === "POST" && url.pathname === "/api/v1/forecasts";
  const limiter = isForecast ? env.FORECAST_RATE_LIMITER : env.API_RATE_LIMITER;
  if (!limiter) return true; // binding not configured (e.g. unit tests); the backend still limits
  let ok = true;
  for (const key of await rateLimitKeys(request, url)) {
    const { success } = await limiter.limit({ key: `${isForecast ? "fc" : "api"}:${key}` });
    ok = ok && success;
  }
  return ok;
}

/**
 * One upstream call with a single deadline for all attempts: a retry after a connection failure only
 * gets the time that is left, so the gateway never waits longer than `timeoutMs` in total.
 */
async function fetchUpstream(upstream: Request, timeoutMs: number, retries: number): Promise<Response> {
  const deadline = Date.now() + timeoutMs;
  for (let attempt = 0; ; attempt++) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error("upstream deadline exceeded");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort("timeout"), remaining);
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

/**
 * True when BACKEND_URL is an absolute http(s) URL and not a `<placeholder>`. Deployed environments
 * (staging, production) require https: the gateway secret travels in every upstream request.
 */
export function backendConfigured(value: string | undefined, environment?: string): boolean {
  if (!value || value.includes("<") || value.includes(">")) return false;
  try {
    const parsed = new URL(value);
    if (parsed.username || parsed.password) return false; // credentials never belong in the URL
    if (DEPLOYED_ENVIRONMENTS.has(environment ?? "")) return parsed.protocol === "https:";
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

/** Current secret or any still-accepted previous one (zero-downtime rotation); all are compared. */
export async function anySecretMatches(presented: string | null, env: Env): Promise<boolean> {
  const candidates = [env.GATEWAY_SHARED_SECRET, ...(env.GATEWAY_ACCEPTED_SECRETS ?? "").split(",").map((s) => s.trim())]
    .filter((s): s is string => Boolean(s));
  let ok = false;
  for (const candidate of candidates) ok = (await secretMatches(presented, candidate)) || ok;
  return ok;
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

/** SQL with string literals, quoted identifiers and comments blanked out (keywords remain). */
function sqlSkeleton(sql: string): string {
  let out = "";
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]!;
    if (ch === "'" || ch === '"' || ch === "`" || ch === "[") {
      const close = ch === "[" ? "]" : ch;
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === close) {
          if (close !== "]" && sql[j + 1] === close) { j += 2; continue; } // doubled quote
          break;
        }
        j++;
      }
      out += " _ ";
      i = j;
    } else if (ch === "-" && sql[i + 1] === "-") {
      while (i < sql.length && sql[i] !== "\n") i++;
      out += " ";
    } else if (ch === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 1;
      out += " ";
    } else {
      out += ch;
    }
  }
  return out;
}

/**
 * Statement allow-list for the data plane: one statement, starting with SELECT/INSERT/UPDATE/
 * DELETE/WITH and containing no DDL, ATTACH, PRAGMA or transaction keyword outside literals - or
 * exactly `PRAGMA table_info(<identifier>)`. Returns an error message, or null when allowed.
 */
export function sqlRejection(sql: string): string | null {
  const trimmed = sql.trim();
  if (SQL_PRAGMA_TABLE_INFO.test(trimmed)) return null;
  const skeleton = sqlSkeleton(trimmed).trim();
  if (!SQL_ALLOWED_START.test(skeleton)) return "statement type not allowed on the data plane";
  const semicolon = skeleton.indexOf(";");
  if (semicolon >= 0 && skeleton.slice(semicolon + 1).trim() !== "") return "only one statement per request";
  if (SQL_FORBIDDEN_WORD.test(skeleton)) return "statement contains a keyword not allowed on the data plane";
  return null;
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
  const rejected = sqlRejection(sql);
  if (rejected) return jsonResponse(403, d1ErrorEnvelope(rejected), requestId);
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
  if (key.startsWith(EDGE_CACHE_PREFIX)) {
    // Reserved for the Worker's own last-known-good copies: the data plane can neither read nor
    // overwrite them, and a backend object can never collide with one.
    return errorResponse(400, "INVALID_OBJECT_KEY", "Object key uses a reserved prefix.", requestId);
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
    const ttl = EXPORT_TTL_SECONDS.find(([prefix]) => key.startsWith(prefix))?.[1];
    await env.OBJECTS.put(key, data, { metadata: { contentType, size: data.byteLength }, ...(ttl ? { expirationTtl: ttl } : {}) });
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
  if (!(await anySecretMatches(request.headers.get("x-gateway-secret"), env))) {
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

// ---------------------------------------------------------------------------------------------
// Last-known-good copies of anonymous public data (contract C2)
// ---------------------------------------------------------------------------------------------

/** KV prefix owned by the Worker. Backend objects live under datasets/, exports/, models/, mlflow/. */
export const EDGE_CACHE_PREFIX = "edge-cache/";
/**
 * The only cacheable requests, by exact path. The KV key comes from this table, never from the
 * request, so no client input can choose or extend a key.
 */
export const PUBLIC_CACHE_KEYS: ReadonlyMap<string, string> = new Map([
  ["/api/v1/public/market-snapshot", `${EDGE_CACHE_PREFIX}public/market-snapshot`],
  ["/api/v1/plans", `${EDGE_CACHE_PREFIX}public/plans`],
]);
/** At most one KV write per key in this interval (KV Free: 1,000 writes/day for the whole account). */
export const EDGE_CACHE_WRITE_INTERVAL_MS = 5 * 60_000;
/**
 * An unchanged copy is rewritten once it is this old, so its `storedAt` (the X-ICP-Cached-At of a stale
 * answer) is never more than about a day older than the last live answer the Worker saw. Costs at most one
 * write per key per day (best effort across isolates).
 */
export const EDGE_CACHE_REFRESH_MS = 24 * 60 * 60_000;
/** Public answers are a few KB; anything larger is passed through but not stored. */
export const EDGE_CACHE_MAX_BYTES = 256 * 1024;
/**
 * Top-level fields that differ on every answer without the data changing (the snapshot's
 * `generated_at`, contract C1). They are left out of the change detection only; the stored and served
 * bytes are always the backend's own.
 */
export const EDGE_CACHE_VOLATILE_FIELDS: readonly string[] = ["generated_at"];
export const CACHE_STATE_HEADER = "x-icp-cache";
export const CACHED_AT_HEADER = "x-icp-cached-at";

/**
 * KV metadata of a stored copy. `sha256` covers the stored bytes and guards integrity (a copy whose
 * bytes do not match is never served). `contentSha256` covers the parsed JSON without the volatile
 * fields and decides whether a live answer changed; entries written before it existed lack it.
 */
interface EdgeCacheMeta { storedAt: string; sha256: string; contentSha256?: string }
/**
 * What this isolate last wrote or read for a key; trusted for one write interval. `writeFailedAt`
 * remembers a failed put (e.g. the daily write quota), so no put is retried within the interval.
 */
interface EdgeCacheMemo { meta: EdgeCacheMeta | null; body: ArrayBuffer | null; checkedAt: number; writeFailedAt?: number }

// Keyed by the KV binding object, so every namespace (and every test fake) has its own memo.
const edgeMemo = new WeakMap<KVNamespace, Map<string, EdgeCacheMemo>>();
/** Keys whose write check is running in this isolate: concurrent live answers do not start a second one. */
const edgeWritesInFlight = new WeakMap<KVNamespace, Set<string>>();

function memoFor(kv: KVNamespace): Map<string, EdgeCacheMemo> {
  let memo = edgeMemo.get(kv);
  if (!memo) {
    memo = new Map();
    edgeMemo.set(kv, memo);
  }
  return memo;
}

/**
 * The KV key for a request that may use the last-known-good copy, or null. Eligible: GET, one of the
 * exact paths above, no query string and no Authorization header at all (even an empty or malformed
 * one bypasses the cache completely).
 */
export function publicCacheKey(request: Request, url: URL): string | null {
  if (request.method !== "GET" || url.search !== "" || request.headers.has("authorization")) return null;
  return PUBLIC_CACHE_KEYS.get(url.pathname) ?? null;
}

function isJsonContentType(headers: Headers): boolean {
  const type = (headers.get("content-type") ?? "").split(";")[0]!.trim().toLowerCase();
  return type === "application/json" || (type.startsWith("application/") && type.endsWith("+json"));
}

/** The parsed JSON object or array encoded as UTF-8 (what the public routes return), else undefined. */
function parseJsonDocument(body: ArrayBuffer): object | undefined {
  try {
    const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body));
    return typeof parsed === "object" && parsed !== null ? parsed : undefined;
  } catch {
    return undefined;
  }
}

/** A JSON object or array encoded as UTF-8; anything else is not stored. */
function isJsonDocument(body: ArrayBuffer): boolean {
  return parseJsonDocument(body) !== undefined;
}

/**
 * SHA-256 of what the answer says rather than of how it was serialised at that instant: the parsed
 * JSON without the top-level volatile fields, re-serialised (the backend emits a fixed key order). Two
 * snapshots that differ only in `generated_at` have the same content hash.
 */
async function contentSha256(body: ArrayBuffer): Promise<string> {
  const parsed = parseJsonDocument(body);
  if (parsed === undefined) return sha256Hex(body); // never stored; keeps the function total
  if (!Array.isArray(parsed)) {
    const record = parsed as Record<string, unknown>;
    for (const field of EDGE_CACHE_VOLATILE_FIELDS) delete record[field];
  }
  return sha256Hex(JSON.stringify(parsed));
}

/** Metadata as written by `rememberPublicAnswer`, normalised; null when absent or malformed. */
const SHA256_HEX = /^[0-9a-f]{64}$/;

function edgeCacheMeta(value: unknown): EdgeCacheMeta | null {
  if (!value || typeof value !== "object") return null;
  const { storedAt, sha256, contentSha256: content } = value as Record<string, unknown>;
  if (typeof storedAt !== "string" || typeof sha256 !== "string" || !SHA256_HEX.test(sha256)) return null;
  const time = Date.parse(storedAt);
  if (!Number.isFinite(time)) return null;
  // Re-serialised, so a hand-edited entry can never inject anything into the X-ICP-Cached-At header.
  const meta: EdgeCacheMeta = { storedAt: new Date(time).toISOString(), sha256 };
  // Optional and only used for change detection: missing or malformed means "compare the bytes".
  if (typeof content === "string" && SHA256_HEX.test(content)) meta.contentSha256 = content;
  return meta;
}

async function readEdgeCache(kv: KVNamespace, key: string, now: number): Promise<EdgeCacheMemo> {
  const found = await kv.getWithMetadata<unknown>(key, "arrayBuffer");
  const meta = found.value === null ? null : edgeCacheMeta(found.metadata);
  const entry: EdgeCacheMemo = { meta, body: meta ? found.value : null, checkedAt: now };
  const previous = memoFor(kv).get(key);
  if (previous && writeBackingOff(previous, now)) entry.writeFailedAt = previous.writeFailedAt; // a re-read is no retry
  memoFor(kv).set(key, entry);
  return entry;
}

/** True while a failed put of this key is younger than one write interval. */
function writeBackingOff(entry: EdgeCacheMemo | undefined, now: number): boolean {
  return entry?.writeFailedAt !== undefined && now - entry.writeFailedAt < EDGE_CACHE_WRITE_INTERVAL_MS;
}

/**
 * Store a live 200 JSON answer when its content differs from the stored copy's and the stored copy is
 * at least one write interval old, or when the content is the same but the stored copy is at least
 * `EDGE_CACHE_REFRESH_MS` (a day) old ("refreshed": the stored time is then never more than about a day
 * older than the last live answer). "Content" is `contentSha256` (the JSON without volatile fields such
 * as `generated_at`); an entry written before that field existed is compared by its byte SHA-256.
 * Costs at most one KV read per key and isolate per interval, and at most one write per key per
 * interval (best effort across isolates: KV is eventually consistent, ~60 s). A failed put (e.g. the
 * daily write quota) is remembered for one interval and rethrown once, so an exhausted quota does not
 * cost a put (and a log line) per live answer. `refreshAfterMs` is the age from which an unchanged copy
 * is rewritten: `EDGE_CACHE_REFRESH_MS` for visitors, `SCHEDULED_REFRESH_AFTER_MS` for the daily refresh.
 */
export async function rememberPublicAnswer(kv: KVNamespace, key: string, body: ArrayBuffer, now: number,
                                           refreshAfterMs = EDGE_CACHE_REFRESH_MS): Promise<"written" | "refreshed" | "unchanged" | "throttled" | "backoff" | "busy"> {
  let busy = edgeWritesInFlight.get(kv);
  if (!busy) {
    busy = new Set();
    edgeWritesInFlight.set(kv, busy);
  }
  // Answers that arrive while a check for the key is running are skipped (the next one is checked).
  if (busy.has(key)) return "busy";
  busy.add(key);
  try {
    return await rememberIfChanged(kv, key, body, now, refreshAfterMs);
  } finally {
    busy.delete(key);
  }
}

async function rememberIfChanged(kv: KVNamespace, key: string, body: ArrayBuffer, now: number,
                                 refreshAfterMs: number): Promise<"written" | "refreshed" | "unchanged" | "throttled" | "backoff"> {
  let entry = memoFor(kv).get(key);
  if (writeBackingOff(entry, now)) return "backoff";
  if (!entry || now - entry.checkedAt >= EDGE_CACHE_WRITE_INTERVAL_MS) entry = await readEdgeCache(kv, key, now);
  const [sha256, content] = await Promise.all([sha256Hex(body), contentSha256(body)]);
  const last = entry.meta;
  const age = last ? now - Date.parse(last.storedAt) : Number.POSITIVE_INFINITY;
  const same = last !== null && (last.contentSha256 !== undefined ? last.contentSha256 === content : last.sha256 === sha256);
  if (same && age < refreshAfterMs) return "unchanged";
  if (age < EDGE_CACHE_WRITE_INTERVAL_MS) return "throttled";
  const meta: EdgeCacheMeta = { storedAt: new Date(now).toISOString(), sha256, contentSha256: content };
  try {
    await kv.put(key, body, { metadata: meta });
  } catch (err) {
    // Keep what the isolate knows about the stored copy: it is still served while the backend is down.
    memoFor(kv).set(key, { ...entry, writeFailedAt: now });
    throw err;
  }
  memoFor(kv).set(key, { meta, body, checkedAt: now });
  return same ? "refreshed" : "written";
}

/**
 * The stored copy, verified against its own SHA-256 (a truncated or edited value is never served).
 * "No usable copy" is remembered like a copy (for one interval), so an outage before the first copy
 * exists costs one KV read per key and isolate per interval, not one per request.
 */
async function lastKnownGood(kv: KVNamespace, key: string, now: number): Promise<{ body: ArrayBuffer; meta: EdgeCacheMeta } | null> {
  try {
    let entry = memoFor(kv).get(key);
    if (!entry || now - entry.checkedAt >= EDGE_CACHE_WRITE_INTERVAL_MS) entry = await readEdgeCache(kv, key, now);
    if (!entry.meta || !entry.body) return null;
    if ((await sha256Hex(entry.body)) !== entry.meta.sha256) {
      // Remembered as "no usable copy": no re-read per request, and the next live answer replaces it
      // at once instead of being throttled by the corrupt entry's timestamp.
      memoFor(kv).set(key, { ...entry, meta: null, body: null });
      return null;
    }
    return { body: entry.body, meta: entry.meta };
  } catch {
    return null; // a KV failure must never turn into a worse error than the upstream one
  }
}

function staleAnswer(hit: { body: ArrayBuffer; meta: EdgeCacheMeta }, requestId: string): Response {
  const headers = apiSecurityHeaders(new Headers({
    "content-type": "application/json",
    "cache-control": "no-store",
    [CACHE_STATE_HEADER]: "stale",
    [CACHED_AT_HEADER]: hit.meta.storedAt,
  }), requestId);
  // A fresh copy of the bytes: the memo keeps its own buffer.
  return new Response(hit.body.slice(0), { status: 200, headers });
}

/** Upstream statuses that mean "the origin did not answer in time" (504, Cloudflare 522/524). */
const UPSTREAM_TIMEOUT_STATUSES = new Set([504, 522, 524]);

/**
 * An upstream error answer (4xx or 5xx) that is not JSON. The backend answers every error with the JSON
 * envelope (exception handlers in backend/icp_api/errors.py, `_send_json` in middleware.py; the gateway
 * forwards no `Origin` header, so CORS preflight replies never occur), so such a page was produced in
 * front of it: the Cloudflare edge of a tunnel (52x/530, 1015/1020), the Render router, a proxy.
 */
function nonJsonUpstreamError(response: Response): boolean {
  return response.status >= 400 && !isJsonContentType(response.headers);
}

/**
 * The upstream gave no usable answer: no response (network error, timeout), any status >= 500, or a
 * non-JSON error page from in front of the backend. JSON 4xx answers come from the backend and are kept.
 */
function upstreamUnavailable(response: Response | null): boolean {
  return !response || response.status >= 500 || nonJsonUpstreamError(response);
}

/**
 * A non-JSON upstream error page becomes the standard envelope (504 for 504/522/524, otherwise 502);
 * the page is discarded, never forwarded, and only its status is logged.
 */
function normalisedUpstreamError(status: number, requestId: string): Response {
  console.warn(JSON.stringify({ event: "gateway.upstream_non_json_error", request_id: requestId, upstream_status: status }));
  return UPSTREAM_TIMEOUT_STATUSES.has(status)
    ? errorResponse(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "The forecasting backend did not respond. Try again shortly.", requestId)
    : errorResponse(502, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "The forecasting backend is temporarily unavailable. Try again shortly.", requestId);
}

async function discardBody(response: Response | null): Promise<void> {
  try { await response?.body?.cancel(); } catch { /* already consumed or closed */ }
}

/**
 * Knobs used only by the daily scheduled refresh (no visitor waits for it): a longer upstream budget for
 * the public routes, so a cold backend can answer, and an earlier rewrite of an unchanged copy.
 */
export interface ApiOptions { publicTimeoutMs?: number; refreshAfterMs?: number }

export async function handleApi(request: Request, env: Env, ctx: ExecutionContext, requestId: string,
                                options: ApiOptions = {}): Promise<Response> {
  const url = new URL(request.url);

  if (url.pathname === "/api/gateway/health") {
    // Public: status only. Environment and backend configuration are visible to operators through
    // /api/v1/system/status (admin token) and to the backend through /api/internal/health (secret).
    return new Response(JSON.stringify({ status: "ok" }),
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
  if (!env.BACKEND_URL || !env.GATEWAY_SHARED_SECRET || !backendConfigured(env.BACKEND_URL, env.ENVIRONMENT)) {
    // Covers the `https://<placeholder>` values kept in wrangler.jsonc until a backend exists:
    // a clear 503 instead of a URL-parse failure surfacing as an unexpected 500.
    return errorResponse(503, "GATEWAY_MISCONFIGURED", "The API backend is not configured for this environment.", requestId);
  }

  const maxBody = positiveIntVar(env.MAX_BODY_BYTES, DEFAULT_MAX_BODY_BYTES);
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

  // No edge caching of API responses (forecast exports are tenant data: private, no-store), except
  // the last-known-good copy of the two anonymous public routes (see `publicCacheKey`).
  const cacheKey = publicCacheKey(request, url);

  const target = new URL(url.pathname + url.search, env.BACKEND_URL);
  const headers = new Headers();
  for (const name of FORWARDED_REQUEST_HEADERS) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  headers.set("x-request-id", requestId);
  headers.set("x-gateway-secret", env.GATEWAY_SHARED_SECRET); // replaces anything the client sent
  // The visitor address travels in a dedicated header the backend trusts only together with the
  // gateway secret. Only allow-listed client headers are copied above, so a client-supplied
  // X-ICP-Client-IP never passes. CF-Connecting-IP is kept as a fallback, but Cloudflare replaces it
  // with the Worker's egress address on subrequests to another Cloudflare-proxied host (Quick
  // Tunnel, Render) - confirm the received value on staging.
  const clientIp = request.headers.get("cf-connecting-ip") ?? "unknown";
  headers.set(CLIENT_IP_HEADER, clientIp);
  headers.set("cf-connecting-ip", clientIp);
  headers.set("x-forwarded-proto", url.protocol.replace(":", ""));
  headers.set("x-forwarded-host", url.host);

  const upstream = new Request(target.toString(), { method: request.method, headers, body, redirect: "manual" });
  const idempotent = request.method === "GET" || request.method === "HEAD";
  // Public routes answer within the browser's 10 s public timeout (contract C3), stale if need be.
  const timeoutMs = cacheKey
    ? options.publicTimeoutMs ?? positiveIntVar(env.PUBLIC_UPSTREAM_TIMEOUT_MS, DEFAULT_PUBLIC_UPSTREAM_TIMEOUT_MS)
    : positiveIntVar(env.UPSTREAM_TIMEOUT_MS, DEFAULT_UPSTREAM_TIMEOUT_MS);
  let response: Response | null;
  try {
    response = await fetchUpstream(upstream, timeoutMs, idempotent ? 1 : 0);
  } catch {
    response = null; // network error or timeout
  }

  const kv = cacheKey ? env.OBJECTS : undefined; // no KV binding: the cache is inert
  if (cacheKey && kv) {
    if (response && response.status === 200 && isJsonContentType(response.headers)) {
      const liveBody = await response.arrayBuffer();
      const liveHeaders = apiSecurityHeaders(new Headers(response.headers), requestId);
      liveHeaders.delete("content-length"); // recomputed by the runtime for the buffered body
      liveHeaders.set(CACHE_STATE_HEADER, "live");
      if (liveBody.byteLength <= EDGE_CACHE_MAX_BYTES && isJsonDocument(liveBody)) {
        // Off the response path; a failed write (e.g. the daily KV write quota) only skips the copy.
        ctx.waitUntil(rememberPublicAnswer(kv, cacheKey, liveBody.slice(0), Date.now(), options.refreshAfterMs).catch((err: unknown) => {
          console.warn(JSON.stringify({ event: "gateway.edge_cache_write_failed", request_id: requestId, key: cacheKey,
            error: String(err instanceof Error ? err.message : err).slice(0, 200) }));
        }));
      }
      return new Response(liveBody, { status: 200, headers: liveHeaders });
    }
    if (upstreamUnavailable(response)) {
      const hit = await lastKnownGood(kv, cacheKey, Date.now());
      if (hit) {
        console.warn(JSON.stringify({ event: "gateway.public_stale_served", request_id: requestId, key: cacheKey,
          stored_at: hit.meta.storedAt, upstream_status: response?.status ?? "no_response" }));
        await discardBody(response);
        return staleAnswer(hit, requestId);
      }
    }
  }

  if (!response) {
    return errorResponse(504, "UPSTREAM_TIMEOUT_OR_UNAVAILABLE",
      "The forecasting backend did not respond. Try again shortly.", requestId);
  }
  if (response.status >= 300 && response.status < 400) {
    await discardBody(response);
    return errorResponse(502, "UPSTREAM_REDIRECT", "Unexpected redirect from the backend.", requestId);
  }
  if (nonJsonUpstreamError(response)) {
    await discardBody(response);
    return normalisedUpstreamError(response.status, requestId);
  }

  return new Response(response.body, { status: response.status, headers: apiSecurityHeaders(new Headers(response.headers), requestId) });
}

// ---------------------------------------------------------------------------------------------
// Serverless backend: warm-on-visit and the daily refresh of the public copies
// ---------------------------------------------------------------------------------------------

/**
 * At most one warm-up ping per backend and isolate in this interval. Deliberately no frequent or timed
 * keep-warm: on Vercel Hobby every cold start spends Active CPU from a 4 h/month allowance (docs/DEPLOYMENT.md).
 */
export const WARM_PING_INTERVAL_MS = 60_000;
/**
 * `waitUntil` work may run up to 30 s after the response. A cold start, including the dependency install Vercel
 * runs at start-up for a function of this size (docs/DEPLOYMENT.md "Size of the function"), is expected to answer
 * within that; a slower one is still started by the ping and only its log line reads `no_response`.
 */
export const WARM_PING_TIMEOUT_MS = 25_000;
/** Upstream budget of the daily refresh for the public routes: nobody waits, so a cold start may finish. */
export const SCHEDULED_PUBLIC_TIMEOUT_MS = 60_000;
/**
 * The daily refresh rewrites an unchanged copy from this age on (visitors: `EDGE_CACHE_REFRESH_MS`), so
 * a once-a-day schedule keeps every copy's `storedAt` within about a day although runs start at slightly
 * different times.
 */
export const SCHEDULED_REFRESH_AFTER_MS = 23 * 60 * 60_000;

/** Last ping per backend URL in this isolate. */
const lastWarmPing = new Map<string, number>();

/**
 * Security headers of `/*` in apps/web/public/_headers. Cloudflare does not promise to apply `_headers` to
 * responses that pass through Worker code (assets.run_worker_first), so the Worker adds them to a document
 * that arrives without them. test/resilience.test.ts keeps this list identical to the file.
 */
export const SITE_SECURITY_HEADERS: ReadonlyArray<readonly [string, string]> = [
  ["Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; manifest-src 'self'; media-src 'self'; frame-src 'none'; worker-src 'none'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'"],
  ["X-Content-Type-Options", "nosniff"],
  ["Referrer-Policy", "strict-origin-when-cross-origin"],
  ["X-Frame-Options", "DENY"],
  ["Cross-Origin-Opener-Policy", "same-origin"],
  ["Cross-Origin-Resource-Policy", "same-origin"],
  ["X-Permitted-Cross-Domain-Policies", "none"],
  ["Permissions-Policy", "accelerometer=(), camera=(), display-capture=(), geolocation=(), gyroscope=(), hid=(), magnetometer=(), microphone=(), midi=(), payment=(), serial=(), usb=(), browsing-topics=()"],
];

/**
 * A browser's navigation to a page (the SPA document), from the Fetch Metadata request headers that every current
 * browser sends on navigations. A request without them (an HTML `Accept` alone) comes from a script, crawler,
 * link preview or uptime monitor, or from a browser too old to send them (Safari before 16.4), and is not a page
 * view here: such clients must never start the backend, or a monitor polling a page would become the keep-warm
 * that docs/DEPLOYMENT.md rules out.
 */
export function isDocumentNavigation(request: Request): boolean {
  if (request.method !== "GET") return false;
  return request.headers.get("sec-fetch-mode") === "navigate" || request.headers.get("sec-fetch-dest") === "document";
}

/**
 * User agents of crawlers, link previews, uptime monitors and headless or audit browsers (best effort). They may
 * send Fetch Metadata like a browser, but their page views must not start the backend either.
 */
export const AUTOMATED_USER_AGENT = /bot|crawl|spider|slurp|headless|lighthouse|pingdom|uptime|monitor|externalhit|preview/i;

/** Whether a request may start the backend early: a browser's page navigation from a person, not a robot. */
export function isWarmingVisit(request: Request): boolean {
  return isDocumentNavigation(request) && !AUTOMATED_USER_AGENT.test(request.headers.get("user-agent") ?? "");
}

async function pingBackend(backend: string): Promise<void> {
  const started = Date.now();
  let status: number | "no_response" = "no_response";
  try {
    // /health/live is public on the backend (no secret needed) and touches no dependency.
    const res = await fetch(new URL("/health/live", backend).toString(), {
      headers: { accept: "application/json" }, signal: AbortSignal.timeout(WARM_PING_TIMEOUT_MS),
    });
    status = res.status;
    await discardBody(res);
  } catch { /* the page never depends on the ping */ }
  console.log(JSON.stringify({ event: "gateway.backend_warm_ping", status, ms: Date.now() - started }));
}

/**
 * Start a non-blocking `GET /health/live` to the backend (in `waitUntil`, never awaited by the page), at most
 * once per `WARM_PING_INTERVAL_MS` per backend and isolate. Returns what happened, for tests and logs.
 */
export function warmBackend(env: Env, ctx: ExecutionContext, now: number): "sent" | "throttled" | "not_configured" {
  const backend = env.BACKEND_URL;
  if (!backend || !backendConfigured(backend, env.ENVIRONMENT)) return "not_configured";
  const last = lastWarmPing.get(backend);
  if (last !== undefined && now - last < WARM_PING_INTERVAL_MS) return "throttled";
  lastWarmPing.set(backend, now);
  ctx.waitUntil(pingBackend(backend));
  return "sent";
}

/** The asset answer with the site's security headers, when the platform did not already attach them. */
function withSiteHeaders(response: Response): Response {
  if (response.headers.has("content-security-policy")) return response;
  const out = new Response(response.body, response);
  for (const [name, value] of SITE_SECURITY_HEADERS) if (!out.headers.has(name)) out.headers.set(name, value);
  return out;
}

/**
 * The daily Cron Trigger: both anonymous public answers go through `handleApi` exactly like a visitor's
 * request (contract C2), with a longer upstream budget and the earlier refresh age. Two backend
 * invocations per run; a failure is logged and leaves the stored copies as they are.
 */
export async function refreshPublicCopies(env: Env, ctx: ExecutionContext): Promise<Record<string, string>> {
  const outcome: Record<string, string> = {};
  for (const path of PUBLIC_CACHE_KEYS.keys()) {
    const requestId = `cron_${crypto.randomUUID().replaceAll("-", "")}`;
    let detail: string;
    try {
      const res = await handleApi(new Request(new URL(path, "https://scheduled.invalid"), { headers: { accept: "application/json" } }),
        env, ctx, requestId, { publicTimeoutMs: SCHEDULED_PUBLIC_TIMEOUT_MS, refreshAfterMs: SCHEDULED_REFRESH_AFTER_MS });
      detail = `${res.status} ${res.headers.get(CACHE_STATE_HEADER) ?? "-"}`;
      await discardBody(res);
    } catch (err) {
      detail = `error ${String(err instanceof Error ? err.message : err).slice(0, 120)}`;
    }
    outcome[path] = detail;
    console.log(JSON.stringify({ event: "gateway.public_refresh", request_id: requestId, path, outcome: detail }));
  }
  return outcome;
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
    // Pages reach the Worker first (assets.run_worker_first); files under /assets, /fonts, /img and /brand
    // are served directly. A person's page view starts the backend early, without waiting for it.
    if (isWarmingVisit(request)) {
      try { warmBackend(env, ctx, Date.now()); } catch { /* never let the ping affect the page */ }
    }
    if (env.ASSETS) return withSiteHeaders(await env.ASSETS.fetch(request));
    return errorResponse(404, "NOT_FOUND", "Not found.", requestId);
  },

  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    await refreshPublicCopies(env, ctx);
  },
} satisfies ExportedHandler<Env>;
