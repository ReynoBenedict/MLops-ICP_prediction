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
const DEFAULT_TIMEOUT_MS = 35_000;

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

export const getSessionToken = (): string | null => read(() => localStorage, SESSION_KEY);
export const setSessionToken = (t: string | null): void => write(() => localStorage, SESSION_KEY, t);
export const getOperatorToken = (): string | null => read(() => sessionStorage, OPERATOR_KEY);
export const setOperatorToken = (t: string | null): void => write(() => sessionStorage, OPERATOR_KEY, t);

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
}

export function buildUrl(path: string, query?: RequestOptions["query"]): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== null && v !== "") params.set(k, String(v));
  }
  const qs = params.toString();
  return `/api/v1${path}${qs ? `?${qs}` : ""}`;
}

export async function api<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
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
  const timer = setTimeout(() => controller.abort(), opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  opts.signal?.addEventListener("abort", () => controller.abort());
  let res: Response;
  try {
    res = await fetch(buildUrl(path, opts.query), { method: opts.method ?? "GET", headers, body, signal: controller.signal });
  } catch {
    if (opts.signal?.aborted) throw new ApiError(0, "ABORTED", "Request cancelled.", null);
    throw new ApiError(0, "NETWORK_ERROR", "Could not reach the server. Check your connection and try again.", null);
  } finally {
    clearTimeout(timer);
  }

  const requestId = res.headers.get("x-request-id");
  if (res.status === 204) return undefined as T;
  const isJson = (res.headers.get("content-type") ?? "").includes("application/json");
  const payload: unknown = isJson ? await res.json().catch(() => null) : null;
  if (!res.ok) {
    // A stored credential the server no longer accepts (expired, revoked, signed out elsewhere):
    // tell the auth layer once so it can re-check the session instead of showing stale private UI.
    if (res.status === 401 && token) notifyAuthExpired();
    if (isErrorResponse(payload)) {
      const e = payload.error;
      throw new ApiError(res.status, e.code, e.message, e.request_id ?? requestId, e.details ?? {});
    }
    throw new ApiError(res.status, "HTTP_ERROR", `Unexpected response (HTTP ${res.status}).`, requestId);
  }
  if (!isJson || payload === null || typeof payload !== "object") {
    throw new ApiError(res.status, "INVALID_RESPONSE", "The server returned an unexpected response.", requestId);
  }
  return payload as T;
}

export const AUTH_EXPIRED_EVENT = "icp:auth-expired";
function notifyAuthExpired(): void {
  try { window.dispatchEvent(new Event(AUTH_EXPIRED_EVENT)); } catch { /* non-browser */ }
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

/** URL for an anonymous, immutable download served through the same origin (dataset/forecast CSVs). */
export function downloadUrl(path: string, query?: RequestOptions["query"]): string {
  return buildUrl(path, query);
}

/** Download a protected file (reports) with the bearer token, then hand it to the browser. */
export async function downloadWithAuth(path: string, query: RequestOptions["query"], filename: string): Promise<void> {
  const headers: Record<string, string> = {};
  const token = getToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(buildUrl(path, query), { headers });
  if (!res.ok) {
    const payload: unknown = await res.json().catch(() => null);
    if (isErrorResponse(payload)) throw new ApiError(res.status, payload.error.code, payload.error.message, payload.error.request_id ?? null);
    throw new ApiError(res.status, "HTTP_ERROR", `Unexpected response (HTTP ${res.status}).`, null);
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
