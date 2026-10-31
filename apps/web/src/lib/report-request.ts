/**
 * POST /reports, safe to retry (W2-R1).
 *
 * Every submission carries an `Idempotency-Key`. Retrying the same submission (same body) reuses its
 * key, so when the first request did finish on the server, the server returns that report instead of
 * generating and counting a second one (backend: same caller, same body, within 24 h, successful
 * reports only). A changed form is a different request and gets a new key; so does a new submission
 * after a definite outcome (the caller forgets the attempt then).
 *
 * A request can also end without a definite answer: the browser's own timeout, the gateway's
 * upstream timeout (504) or a broken connection (502, network error). The report may then still have
 * been recorded (succeeded or failed), so the page must say the outcome is unknown and point to the
 * list instead of calling it a server outage. A retry with the same key can also meet the first
 * request still generating on the server (409 IDEMPOTENCY_IN_PROGRESS, `details.retry_after_seconds`
 * and Retry-After): nothing new is started, the outcome is still unknown, and the retry waits as asked.
 */
import { ApiError } from "../api/client";

export type ReportFormat = "pdf" | "xlsx" | "csv";
/** Canonical format order, so ticking formats in a different order is still the same request. */
export const REPORT_FORMATS: readonly ReportFormat[] = ["pdf", "xlsx", "csv"];

export interface ReportBody { kind: string; params: Record<string, unknown>; formats: ReportFormat[] }
/** `firstSentAt`: when this submission was first sent (ms); a retry with the same key keeps it. */
export interface ReportAttempt { key: string; body: ReportBody; fingerprint: string; firstSentAt?: number }

/** The backend's accepted key shape (`IDEMPOTENCY_KEY_PATTERN`). */
export const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9._:-]{8,128}$/;

function randomId(): string {
  try {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID();
  } catch { /* insecure context: fall through */ }
  try {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
  } catch { /* no Web Crypto at all */ }
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}${Math.random().toString(36).slice(2)}`;
}

export function newIdempotencyKey(): string {
  return `rep_${randomId()}`;
}

/** The body exactly as it is sent (formats in canonical order). */
export function reportBody(kind: string, params: Record<string, unknown>, formats: readonly ReportFormat[]): ReportBody {
  return { kind, params, formats: REPORT_FORMATS.filter((f) => formats.includes(f)) };
}

/**
 * The attempt for a submission: the previous one (same key) when the body is unchanged, otherwise a
 * new attempt with a new key. The fingerprint is the JSON that is sent, so a reused key always comes
 * with the same body (the server answers 409 IDEMPOTENCY_KEY_REUSED otherwise).
 */
export function nextAttempt(previous: ReportAttempt | null, body: ReportBody): ReportAttempt {
  const fingerprint = JSON.stringify(body);
  return previous && previous.fingerprint === fingerprint ? previous : { key: newIdempotencyKey(), body, fingerprint };
}

/**
 * Why the outcome of a request is unknown, or null when the answer is definite. `in_progress`: the
 * server is still generating the first request with this key (409 IDEMPOTENCY_IN_PROGRESS, after a
 * retry that followed a 504); its report appears in the list once it ends, succeeded or failed.
 */
export type UnknownOutcome = "client_timeout" | "gateway_timeout" | "connection" | "in_progress";

export function unknownOutcome(err: unknown): UnknownOutcome | null {
  if (!(err instanceof ApiError)) return null;
  if (err.status === 409 && err.code === "IDEMPOTENCY_IN_PROGRESS") return "in_progress";
  if (err.status === 0) {
    if (err.code === "TIMEOUT") return "client_timeout";
    return err.code === "NETWORK_ERROR" || err.code === "OFFLINE" ? "connection" : null;
  }
  if (err.status === 504) return "gateway_timeout";
  if (err.status === 502 || err.code === "UPSTREAM_TIMEOUT_OR_UNAVAILABLE") return "connection";
  return null;
}
