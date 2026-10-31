/**
 * Typed API client. The browser only ever talks to its own origin (`/api/...`): the Vite dev proxy
 * locally, the Cloudflare Worker gateway in production. No backend URL or secret is bundled.
 *
 * Credentials (never part of the build):
 * - user session token (`icp_sess_…`) in localStorage "icp.sessionToken" (sessions last 30 days)
 * - operator token in sessionStorage "icp.operatorToken" (kept for the browser tab only)
 * When both exist the operator token wins (it is strictly more privileged).
 */
import type { ErrorResponse } from "@icp/contracts";

export const SESSION_KEY = "icp.sessionToken";
export const OPERATOR_KEY = "icp.operatorToken";

/**
 * Client timeouts (ms). A request that gets no answer in time ends as ApiError TIMEOUT (status 0)
 * with `details.timeout_seconds`, so the UI can say how long it waited.
 * - public snapshot / plan catalogue: short, the landing page must never wait long on them;
 * - /auth/whoami: the app shell waits for it, so a hanging backend may hold the boot state at most this long;
 * - forecast run, scenario run and comparison: model work on the server;
 * - report generation: renders PDF/Excel files.
 */
export const TIMEOUTS = {
  default: 35_000,
  public: 10_000,
  whoami: 8_000,
  compute: 60_000,
  // Above the gateway's 90 s upstream budget, so a slow report ends with the gateway's own answer.
  report: 95_000,
  // Protected file downloads (reports, exports, dataset versions): headers and body together.
  download: 60_000,
} as const;

/** Timeout for a request when the caller does not set one (see TIMEOUTS). */
export function timeoutFor(path: string, method: "GET" | "POST" = "GET"): number {
  const p = path.split("?")[0] ?? path;
  if (p === "/auth/whoami") return TIMEOUTS.whoami;
  if (method === "GET" && (p === "/public/market-snapshot" || p === "/plans")) return TIMEOUTS.public;
  if (method === "POST" && (p === "/forecasts" || p === "/scenarios/compare" || /^\/scenarios\/[^/]+\/run$/.test(p))) return TIMEOUTS.compute;
  if (method === "POST" && p === "/reports") return TIMEOUTS.report;
  return TIMEOUTS.default;
}

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly requestId: string | null,
    public readonly details: Record<string, unknown> = {},
  ) {
    super(message);
  }
}

function read(storage: () => Storage, key: string): string | null {
  try { return storage().getItem(key); } catch { return null; }
}
function write(storage: () => Storage, key: string, value: string | null): void {
  try {
    if (value) storage().setItem(key, value);
    else storage().removeItem(key);
  } catch { /* storage unavailable: credential simply is not remembered */ }
}

// Setting or removing a credential (sign-in, sign-out, expiry) also drops the private shared GETs, so no
// answer of an earlier session stays in memory after it ends.
export const getSessionToken = (): string | null => read(() => localStorage, SESSION_KEY);
export const setSessionToken = (t: string | null): void => { write(() => localStorage, SESSION_KEY, t); forgetPrivateGets(); };
export const getOperatorToken = (): string | null => read(() => sessionStorage, OPERATOR_KEY);
export const setOperatorToken = (t: string | null): void => { write(() => sessionStorage, OPERATOR_KEY, t); forgetPrivateGets(); };

/** Token sent with requests: operator token when present, else the user session. */
export function getToken(): string | null {
  return getOperatorToken() ?? getSessionToken();
}

/** Legacy helper kept for tests: sets the operator token. */
export function setToken(token: string | null): void {
  setOperatorToken(token);
}

function isErrorResponse(value: unknown): value is ErrorResponse {
  return typeof value === "object" && value !== null && "error" in value
    && typeof (value as { error: { code?: unknown } }).error?.code === "string";
}

export interface RequestOptions {
  method?: "GET" | "POST";
  query?: Record<string, string | number | boolean | undefined | null>;
  json?: unknown;
  text?: string;
  contentType?: string;
  timeoutMs?: number;
  signal?: AbortSignal;
  /** Send the request anonymously even when a token is stored. */
  anonymous?: boolean;
  /**
   * Do not announce a 401 to the auth layer (used by sign-out: a session that is already over is
   * the expected outcome there, not a reason for a "session expired" notice).
   */
  quietAuth?: boolean;
  /**
   * Extra request headers (e.g. `Idempotency-Key`, so a retried POST /reports returns the original
   * report). The client's own headers (Accept, Authorization, Content-Type) cannot be overridden.
   */
  headers?: Record<string, string>;
}

const RESERVED_HEADERS = new Set(["accept", "authorization", "content-type"]);

export function buildUrl(path: string, query?: RequestOptions["query"]): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
  }
  const qs = params.toString();
  return `/api/v1${path}${qs ? `?${qs}` : ""}`;
}

export async function api<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  return (await request<T>(path, opts)).data;
}

/** One API call: parsed JSON body plus the response headers (for cache metadata). */
async function request<T>(path: string, opts: RequestOptions = {}): Promise<{ data: T; headers: Headers }> {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(opts.headers ?? {})) {
    if (!RESERVED_HEADERS.has(name.toLowerCase())) headers[name] = value;
  }
  headers.Accept = "application/json";
  const token = opts.anonymous ? null : getToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  let body: BodyInit | undefined;
  if (opts.json !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(opts.json);
  } else if (opts.text !== undefined) {
    headers["Content-Type"] = opts.contentType ?? "text/plain";
    body = opts.text;
  }

  const controller = new AbortController();
  let timedOut = false;
  const timeoutMs = opts.timeoutMs ?? timeoutFor(path, opts.method ?? "GET");
  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeoutMs);
  const onAbort = () => controller.abort();
  opts.signal?.addEventListener("abort", onAbort, { once: true });
  let res: Response;
  try {
    res = await fetch(buildUrl(path, opts.query), { method: opts.method ?? "GET", headers, body, signal: controller.signal });
  } catch {
    if (opts.signal?.aborted) throw new ApiError(0, "ABORTED", "Request cancelled.", null);
    throw networkError(timedOut, timeoutMs);
  } finally {
    clearTimeout(timer);
    opts.signal?.removeEventListener("abort", onAbort);
  }

  const requestId = res.headers.get("x-request-id");
  if (res.status === 204) return { data: undefined as T, headers: res.headers };
  const isJson = (res.headers.get("content-type") ?? "").includes("application/json");
  const payload: unknown = isJson ? await res.json().catch(() => null) : null;
  if (!res.ok) {
    // A stored credential the server no longer accepts (expired, revoked, signed out elsewhere):
    // tell the auth layer once so it can re-check the session instead of showing stale private UI.
    if (res.status === 401 && token && !opts.quietAuth) notifyAuthExpired();
    // Rate limits: keep the wait time from the body or the Retry-After header (the gateway only sends the header).
    const retryHeader = Number(res.headers.get("retry-after"));
    const retry = res.status === 429 && Number.isFinite(retryHeader) && retryHeader > 0 ? { retry_after_seconds: retryHeader } : {};
    if (isErrorResponse(payload)) {
      const e = payload.error;
      if (res.status === 403 && e.code === PASSWORD_CHANGE_CODE) notifyPasswordChange();
      throw new ApiError(res.status, e.code, e.message, e.request_id ?? requestId, { ...retry, ...(e.details ?? {}) });
    }
    throw new ApiError(res.status, res.status === 429 ? "RATE_LIMITED" : "HTTP_ERROR", `Unexpected response (HTTP ${res.status}).`, requestId, retry);
  }
  if (!isJson || payload === null || typeof payload !== "object") {
    throw new ApiError(res.status, "INVALID_RESPONSE", "The server returned an unexpected response.", requestId);
  }
  return { data: payload as T, headers: res.headers };
}

/**
 * A request that never got an HTTP answer. Three different situations, three different messages:
 * the browser is offline (OFFLINE), the server took longer than the client timeout (TIMEOUT: the
 * connection works, the server is slow or starting), or the connection failed (NETWORK_ERROR).
 */
function networkError(timedOut: boolean, timeoutMs: number): ApiError {
  if (timedOut) return new ApiError(0, "TIMEOUT", "The server did not answer in time.", null, { timeout_seconds: Math.round(timeoutMs / 1000) });
  let offline = false;
  try { offline = typeof navigator !== "undefined" && navigator.onLine === false; } catch { /* non-browser */ }
  if (offline) return new ApiError(0, "OFFLINE", "This device is offline.", null);
  return new ApiError(0, "NETWORK_ERROR", "Could not reach the server. Check your connection and try again.", null);
}

export const AUTH_EXPIRED_EVENT = "icp:auth-expired";
function notifyAuthExpired(): void {
  try { window.dispatchEvent(new Event(AUTH_EXPIRED_EVENT)); } catch { /* non-browser */ }
}

/**
 * A member still using a temporary password gets 403 PASSWORD_CHANGE_REQUIRED from every endpoint
 * except the password/profile ones. The auth layer listens and routes the member to /akun#keamanan.
 */
export const PASSWORD_CHANGE_CODE = "PASSWORD_CHANGE_REQUIRED";
export const PASSWORD_CHANGE_EVENT = "icp:password-change-required";
function notifyPasswordChange(): void {
  try { window.dispatchEvent(new Event(PASSWORD_CHANGE_EVENT)); } catch { /* non-browser */ }
}

/**
 * Resolve to `null` only when the resource does not exist (404), so "nothing published yet" is
 * distinguishable from "server unreachable" (which must surface as an error, not as an empty state).
 */
export async function nullOn404<T>(p: Promise<T>): Promise<T | null> {
  try { return await p; } catch (e) {
    if (e instanceof ApiError && e.status === 404) return null;
    throw e;
  }
}

/**
 * Download a protected file (reports, forecast exports, dataset versions) with the bearer token,
 * then hand it to the browser. Never use a plain <a href> for these: the request would carry no
 * credential and the server answers 401.
 */
export async function downloadWithAuth(path: string, query: RequestOptions["query"], filename: string): Promise<void> {
  const headers: Record<string, string> = {};
  const token = getToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, TIMEOUTS.download);
  let res: Response;
  let blob: Blob;
  try {
    res = await fetch(buildUrl(path, query), { headers, signal: controller.signal });
    blob = res.ok ? await res.blob() : new Blob();
  } catch {
    throw networkError(timedOut, TIMEOUTS.download);
  } finally {
    clearTimeout(timer);
  }
  if (!res.ok) {
    const payload: unknown = await res.json().catch(() => null);
    if (res.status === 401 && token) notifyAuthExpired();
    const rid = res.headers.get("x-request-id");
    if (isErrorResponse(payload)) {
      if (res.status === 403 && payload.error.code === PASSWORD_CHANGE_CODE) notifyPasswordChange();
      throw new ApiError(res.status, payload.error.code, payload.error.message, payload.error.request_id ?? rid, payload.error.details ?? {});
    }
    throw new ApiError(res.status, res.status === 429 ? "RATE_LIMITED" : "HTTP_ERROR", `Unexpected response (HTTP ${res.status}).`, rid);
  }
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/**
 * GET shared by several components that ask for the same resource at the same time (e.g. the shell's
 * status dot and the health page both read /system/status). Concurrent callers share one in-flight
 * request and a result younger than `maxAgeMs` is reused, so one page load costs one request against
 * the operator rate-limit budget instead of two (four under React StrictMode in development).
 * Keyed by credential + URL: a different principal never sees another one's result. `fresh` forces a
 * new request (explicit "re-check" buttons).
 */
const shared = new Map<string, { at: number; p: Promise<unknown> }>();
function forgetPrivateGets(): void { shared.clear(); }
export function sharedGet<T>(path: string, opts: { query?: RequestOptions["query"]; maxAgeMs?: number; fresh?: boolean; signal?: AbortSignal } = {}): Promise<T> {
  const key = `${getToken() ?? ""} ${buildUrl(path, opts.query)}`;
  const now = Date.now();
  const hit = shared.get(key);
  if (!opts.fresh && hit && now - hit.at < (opts.maxAgeMs ?? 10_000)) return withAbort(hit.p as Promise<T>, opts.signal);
  // The shared request itself is never cancelled by one caller: others may still be waiting for it.
  const p = api<T>(path, { query: opts.query });
  shared.set(key, { at: now, p });
  p.catch(() => { if (shared.get(key)?.p === p) shared.delete(key); });
  return withAbort(p, opts.signal);
}

function withAbort<T>(p: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return p;
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new ApiError(0, "ABORTED", "Request cancelled.", null));
    if (signal.aborted) return onAbort();
    signal.addEventListener("abort", onAbort, { once: true });
    p.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}

/**
 * Public data with its delivery state. The gateway may answer a public request from its
 * last-known-good copy while the backend is unreachable (contract C2): `X-ICP-Cache: stale` and
 * `X-ICP-Cached-At: <ISO time the copy was stored>`. Anything else is live data.
 */
export interface PublicResult<T> {
  data: T;
  /** True when the gateway served its stored copy because the backend did not answer. */
  stale: boolean;
  /** When the stored copy was saved (ISO 8601), only for stale answers with a valid timestamp. */
  cachedAt: string | null;
}

/** Read the gateway cache headers of a public answer. */
export function cacheMeta(headers: Headers): { stale: boolean; cachedAt: string | null } {
  const stale = (headers.get("x-icp-cache") ?? "").trim().toLowerCase() === "stale";
  const raw = headers.get("x-icp-cached-at")?.trim() ?? "";
  const cachedAt = stale && raw && !Number.isNaN(Date.parse(raw)) ? raw : null;
  return { stale, cachedAt };
}

/**
 * Public, anonymous reference data (the plan catalogue, the public market snapshot) kept in this
 * tab's memory for a few minutes, so moving between pages does not refetch it. A stale answer is
 * kept for a short time only, so live data shows again soon after the backend is back. Only
 * anonymous requests go through here: nothing tied to a credential is ever cached, and nothing is
 * written to browser storage or to any shared cache (the server still answers `Cache-Control: no-store`).
 */
const PUBLIC_TTL_MS = 5 * 60_000;
const PUBLIC_STALE_TTL_MS = 30_000;
const publicCache = new Map<string, { at: number; ttl: number; p: Promise<PublicResult<unknown>> }>();
export function publicGetMeta<T>(path: string, opts: { signal?: AbortSignal; maxAgeMs?: number } = {}): Promise<PublicResult<T>> {
  const key = buildUrl(path);
  const now = Date.now();
  const hit = publicCache.get(key);
  if (hit && now - hit.at < Math.min(hit.ttl, opts.maxAgeMs ?? PUBLIC_TTL_MS)) return withAbort(hit.p as Promise<PublicResult<T>>, opts.signal);
  const p = request<T>(path, { anonymous: true }).then(({ data, headers }) => ({ data, ...cacheMeta(headers) }));
  const entry = { at: now, ttl: PUBLIC_TTL_MS, p: p as Promise<PublicResult<unknown>> };
  publicCache.set(key, entry);
  p.then((r) => { if (r.stale) entry.ttl = PUBLIC_STALE_TTL_MS; }, () => { if (publicCache.get(key) === entry) publicCache.delete(key); });
  return withAbort(p, opts.signal);
}

/** Public data only (callers that do not show the delivery state). */
export function publicGet<T>(path: string, opts: { signal?: AbortSignal; maxAgeMs?: number } = {}): Promise<T> {
  return publicGetMeta<T>(path, opts).then((r) => r.data);
}

/** Test helper: forget shared results (credentialed and public). */
export function clearSharedGets(): void { shared.clear(); publicCache.clear(); }
