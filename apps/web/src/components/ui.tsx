/**
 * Design-system primitives (docs/DESIGN_SYSTEM.md; styles in styles/components.css). Every page
 * composes these; page files contain no ad-hoc styling beyond layout utilities from styles/base.css. All visible text comes from the i18n dictionary.
 */
import {
  CircleAlert, CircleCheck, CircleX, Info, LoaderCircle, Lock, Minus, TrendingDown, TrendingUp, TriangleAlert,
  type LucideIcon,
} from "lucide-react";
import { useEffect, useId, useLayoutEffect, useRef, useState, type ButtonHTMLAttributes, type CSSProperties, type ReactNode, type Ref } from "react";
import { createPortal } from "react-dom";
import { Link, useLocation } from "react-router-dom";
import { ApiError, TIMEOUTS } from "../api/client";
import { listText } from "../format";
import { useT } from "../i18n";
import { IllUpgrade } from "./illustrations";
import { useModal } from "./modal";

export type Tone = "good" | "warn" | "bad" | "info" | "neutral" | "brand" | "outline" | "cat-estimate" | "cat-forecast";

/* ---------- Status vocabulary (one mapping for the whole app) --------------- */
const STATUS_TONE: Record<string, Tone> = {
  ok: "good", ready: "good", succeeded: "good", fresh: "good", stable: "good", passed: "good", success: "good", active: "good", verified: "good",
  degraded: "warn", lagging: "warn", moderate: "warn", stale: "warn", single_source: "warn", machine_extracted: "warn", pending: "warn",
  legacy_unverified: "warn", suspended: "warn",
  running: "info", queued: "neutral", no_change: "neutral", cancelled: "neutral", not_configured: "neutral", unknown: "neutral", disabled: "neutral",
  down: "bad", not_ready: "bad", failed: "bad", significant: "bad", failure: "bad", denied: "bad", conflict: "bad", rejected: "bad",
};
const TONE_ICON: Record<Tone, LucideIcon> = {
  good: CircleCheck, warn: TriangleAlert, bad: CircleX, info: Info, neutral: Minus, brand: Info, outline: Minus, "cat-estimate": Minus, "cat-forecast": Minus,
};

export function statusTone(status: string): Tone {
  return STATUS_TONE[status] ?? "neutral";
}

/** Status is never color-only: icon + text label always accompany the tone. */
export function StatusBadge({ status, label }: { status: string; label?: string }) {
  const { td } = useT();
  const tone = statusTone(status);
  const Icon = status === "running" ? LoaderCircle : TONE_ICON[tone];
  return (
    <span className={`badge ${tone}`}>
      <Icon size={13} aria-hidden="true" className={status === "running" ? "spin" : undefined} />
      {label ?? td("status", status)}
    </span>
  );
}

export function Badge({ tone = "neutral", icon: Icon, children, title }: { tone?: Tone; icon?: LucideIcon; children: ReactNode; title?: string }) {
  return (
    <span className={`badge ${tone}`} title={title}>
      {Icon && <Icon size={13} aria-hidden="true" />}
      {children}
    </span>
  );
}

/* ---------- Buttons ------------------------------------------------------- */
type BtnVariant = "primary" | "secondary" | "ghost" | "danger" | "danger-solid";
export function Button({ variant = "secondary", size, icon: Icon, loading, children, className = "", ref, ...rest }:
  ButtonHTMLAttributes<HTMLButtonElement> & { variant?: BtnVariant; size?: "sm" | "lg"; icon?: LucideIcon; loading?: boolean; ref?: Ref<HTMLButtonElement> }) {
  const cls = ["btn", variant !== "secondary" ? variant : "", size ?? "", className].filter(Boolean).join(" ");
  return (
    // While a request runs the button stays focusable (aria-disabled, clicks ignored) instead of
    // `disabled`: disabling the focused button would drop keyboard focus to <body> (A11Y-01).
    // Plain buttons by default: inside a <form> an untyped button submits it (a download click
    // re-generated the report). Submit buttons say `type="submit"`.
    <button ref={ref} type="button" className={cls} {...rest} disabled={rest.disabled}
      aria-disabled={loading || rest["aria-disabled"] || undefined} aria-busy={loading || undefined}
      onClick={loading ? (e) => { e.preventDefault(); e.stopPropagation(); } : rest.onClick}>
      {loading ? <LoaderCircle size={16} className="spin" aria-hidden="true" /> : Icon ? <Icon size={16} aria-hidden="true" /> : null}
      {children}
    </button>
  );
}

/* ---------- Form fields ---------------------------------------------------- */
export interface FieldControlProps { id: string; "aria-invalid"?: true; "aria-describedby"?: string; "aria-required"?: true }

/**
 * Labelled form field whose hint and error are tied to the control (`aria-describedby`) and whose
 * invalid state is exposed (`aria-invalid`). The error lives outside the <label>, so it is read as
 * a description and never becomes part of the field's name.
 */
export function Field({ label, hint, error, required, className = "", render }:
  { label: ReactNode; hint?: ReactNode; error?: string | null; required?: boolean; className?: string; render: (props: FieldControlProps) => ReactNode }) {
  const id = useId();
  const hintId = `${id}-hint`, errId = `${id}-err`;
  const describedBy = [error ? errId : null, hint ? hintId : null].filter(Boolean).join(" ") || undefined;
  return (
    <div className={`field ${className}`.trim()}>
      <label htmlFor={id} className="field-label">{label}</label>
      {render({ id, "aria-invalid": error ? true : undefined, "aria-describedby": describedBy, "aria-required": required ? true : undefined })}
      {hint && <span id={hintId} className="field-hint">{hint}</span>}
      {error && <span id={errId} className="field-error">{error}</span>}
    </div>
  );
}

/** Focus the first control marked invalid inside `root` (after a failed client-side validation). */
export function focusFirstInvalid(root: HTMLElement | null): void {
  root?.querySelector<HTMLElement>("[aria-invalid='true']")?.focus();
}

/** Move keyboard/screen-reader focus to a result heading after an action (headings are not focusable by default). */
export function prefersReducedMotion(): boolean {
  try { return window.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch { return false; }
}

/** Scroll a result into view; instant (no animation) when the user prefers reduced motion. */
export function scrollToResult(el: HTMLElement | null, block: ScrollLogicalPosition = "start"): void {
  if (!el) return;
  try { el.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block }); } catch { /* jsdom */ }
}

export function focusHeading(container: HTMLElement | null): void {
  const h = container?.querySelector<HTMLElement>("h2");
  if (!h) return;
  if (!h.hasAttribute("tabindex")) h.setAttribute("tabindex", "-1");
  h.focus({ preventScroll: true });
}

/* ---------- Page structure ------------------------------------------------ */
/**
 * Page title block. `eyebrow` is optional: in the signed-in shell the top bar already shows where
 * the reader is (group › page), so the eyebrow only appears in the drawer layout (≤ 900 px), where
 * the top bar has no location line. `meta` is a one-line "data as of" note under the description.
 */
export function PageHeader({ eyebrow, icon: Icon, title, description, meta, actions }:
  { eyebrow?: string; icon?: LucideIcon; title: string; description?: ReactNode; meta?: ReactNode; actions?: ReactNode }) {
  return (
    <header className="page-header">
      <div className="page-heading">
        {eyebrow && <div className="eyebrow">{Icon && <Icon size={14} aria-hidden="true" />}{eyebrow}</div>}
        <h1 className="page-title">{title}</h1>
        {description && <p className="page-desc">{description}</p>}
        {meta && <p className="page-meta">{meta}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
  );
}

/**
 * A row of labelled view controls (range, comparison) above the content they change. Each control
 * keeps its own accessible name; the visible label is for sighted readers only.
 */
export function ViewBar({ label, children }: { label: string; children: ReactNode }) {
  return <div className="view-bar" role="group" aria-label={label}>{children}</div>;
}
export function ViewField({ label, children }: { label: string; children: ReactNode }) {
  return <div className="view-field"><span className="view-field-label" aria-hidden="true">{label}</span>{children}</div>;
}

/**
 * Small trend line in the official ICP hue (e.g. the last 12 official months). Decorative: the
 * caller states the values it summarises (low, high, latest) in text next to it. A missing month
 * breaks the line (never interpolated); the latest point is marked.
 */
export function Sparkline({ values }: { values: Array<number | null> }) {
  const nums = values.filter((v): v is number => typeof v === "number");
  const last = values.reduce<number>((acc, v, i) => (typeof v === "number" ? i : acc), -1);
  if (nums.length < 2 || last < 0) return null;
  const W = 240, H = 48, PAD = 5;
  const lo = Math.min(...nums), hi = Math.max(...nums);
  const x = (i: number) => (values.length > 1 ? (i / (values.length - 1)) * W : W / 2);
  const y = (v: number) => (hi === lo ? H / 2 : PAD + (1 - (v - lo) / (hi - lo)) * (H - 2 * PAD));
  const segments: string[][] = [[]];
  values.forEach((v, i) => {
    if (typeof v === "number") segments.at(-1)!.push(`${x(i).toFixed(1)},${y(v).toFixed(1)}`);
    else if (segments.at(-1)!.length) segments.push([]);
  });
  return (
    <span className="spark" aria-hidden="true">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" focusable="false">
        {segments.filter((s) => s.length > 1).map((s, k) => (
          <polyline key={k} points={s.join(" ")} fill="none" stroke="var(--series-icp)" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
        ))}
      </svg>
      <span className="spark-dot" style={{ left: `${(x(last) / W) * 100}%`, top: `${(y(values[last] as number) / H) * 100}%` }} />
    </span>
  );
}

export function Panel({ title, icon: Icon, subtitle, actions, children, footer, flush, className = "", id }:
  { title?: ReactNode; icon?: LucideIcon; subtitle?: ReactNode; actions?: ReactNode; children: ReactNode; footer?: ReactNode; flush?: boolean; className?: string; id?: string }) {
  return (
    <section className={`panel ${className}`} id={id}>
      {(title || actions) && (
        <div className="panel-head">
          <div className="grow">
            {title && <h2 className="panel-title">{Icon && <Icon size={17} aria-hidden="true" />}{title}</h2>}
            {subtitle && <p className="panel-sub">{subtitle}</p>}
          </div>
          {actions && <div className="row">{actions}</div>}
        </div>
      )}
      <div className={`panel-body${flush ? " flush" : ""}`}>{children}</div>
      {footer && <div className="panel-foot">{footer}</div>}
    </section>
  );
}

/**
 * KPI tile. `iconTone` colours the icon by the entity it stands for (series hues, teal for the
 * product). "amber" is a deprecated alias of "idr": amber is reserved for the brand mark and the
 * decree stamp, so a rupiah KPI uses the rupiah series tone instead.
 */
export function Kpi({ label, icon: Icon, iconTone, value, unit, meta, tone, children }:
  { label: string; icon?: LucideIcon; iconTone?: "icp" | "wti" | "brand" | "idr" | "amber"; value: ReactNode; unit?: string; meta?: ReactNode; tone?: "warn" | "bad"; children?: ReactNode }) {
  const iconClass = iconTone === "amber" ? "idr" : iconTone ?? "";
  return (
    <div className={`kpi${tone ? ` tone-${tone}` : ""}`}>
      <div className="kpi-top">
        <span className="kpi-label">{label}</span>
        {Icon && <span className={`kpi-icon ${iconClass}`}><Icon size={16} aria-hidden="true" /></span>}
      </div>
      <div className="kpi-value">{typeof value === "string" || typeof value === "number" ? <span className="kpi-num">{value}</span> : value}{unit && <span className="unit">{unit}</span>}</div>
      {meta && <div className="kpi-meta">{meta}</div>}
      {children}
    </div>
  );
}

/** Changes smaller than this (in %) read as "stabil" / "steady" (documented on /metodologi). */
export const STABLE_PCT = 0.5;

/**
 * Signed change with arrow icon, sign and a direction word (naik / turun / stabil). Neutral ink:
 * a rising price is good news for a seller and bad news for a buyer, so no green/red.
 * `word={false}` keeps the word for screen readers only (dense tables).
 */
export function Delta({ value, suffix = "%", digits = 1, title, word = true, stableBelow }:
  { value: number | null | undefined; suffix?: string; digits?: number; title?: string; word?: boolean; stableBelow?: number }) {
  const { intl, t } = useT();
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  const threshold = stableBelow ?? (suffix.trim() === "%" ? STABLE_PCT : 0.005);
  const dir = Math.abs(value) < threshold ? "flat" : value > 0 ? "up" : "down";
  const Icon = dir === "up" ? TrendingUp : dir === "down" ? TrendingDown : Minus;
  const n = new Intl.NumberFormat(intl, { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(Math.abs(value));
  const w = t(dir === "up" ? "common.dir.up" : dir === "down" ? "common.dir.down" : "common.dir.flat");
  return (
    <span className={`delta ${dir}`} title={title}>
      <Icon size={13} aria-hidden="true" />
      {value > 0 ? "+" : value < 0 ? "−" : ""}{n}{suffix}
      {word ? <span className="delta-word">{w}</span> : <span className="sr-only"> {w}</span>}
    </span>
  );
}

/* ---------- Feedback ------------------------------------------------------ */
export function Callout({ tone = "info", title, children, icon, action }: { tone?: Exclude<Tone, "neutral" | "brand" | "outline" | "cat-estimate" | "cat-forecast">; title?: ReactNode; children?: ReactNode; icon?: LucideIcon; action?: ReactNode }) {
  const Icon = icon ?? (tone === "info" ? Info : tone === "good" ? CircleCheck : tone === "warn" ? TriangleAlert : CircleAlert);
  return (
    <div className={`callout ${tone}`} role={tone === "bad" ? "alert" : undefined}>
      <Icon size={18} aria-hidden="true" />
      <div className="grow">
        {title && <div className="callout-title">{title}</div>}
        {children && <div>{children}</div>}
      </div>
      {action}
    </div>
  );
}

/**
 * Error presentation for end users. Expected (4xx) outcomes show the server's own message when it is
 * already written in the active language (the API answers auth, account and validation errors in
 * Indonesian, others in English); everything else gets a translated explanation. Server faults,
 * network faults and client exceptions never show server text, only a generic explanation plus the
 * request id as a "reference code" so support can trace it without exposing internals.
 */
const UNAVAILABLE_CODES = new Set(["UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "GATEWAY_MISCONFIGURED", "UPSTREAM_REDIRECT", "DATA_PLANE_MISCONFIGURED"]);

/** Seconds to wait before retrying a rate-limited request (server detail or Retry-After header). */
export function retryAfterSeconds(e: ApiError): number | null {
  const v = Number(e.details?.retry_after_seconds);
  return Number.isFinite(v) && v > 0 ? Math.ceil(v) : null;
}

const ID_WORDS = /\b(yang|tidak|dan|atau|anda|sudah|belum|harus|dapat|ini|itu|untuk|dengan|salah|silakan|masih|terlalu|minimal|maksimal|paket|peran|sandi|pengguna|anggota|organisasi|permintaan|hanya|melalui|sendiri|terdaftar|ditemukan|diubah|memerlukan|lebih|tinggi|sebelum|menunggu|keputusan|ditarik|bulan|skenario|laporan|kunci|batas|termasuk|karakter|kosong|wajib)\b/gi;
const EN_WORDS = /\b(the|is|are|was|not|and|or|you|your|must|cannot|can|this|that|for|with|invalid|required|already|only|please|too|many|found|missing|allowed|credentials|request|requests|failed|unknown|should|than|characters|empty)\b/gi;

/** Best guess of the language of a short server message ("id", "en", or null when unclear). */
export function messageLocale(text: string | null | undefined): "id" | "en" | null {
  if (!text) return null;
  const id = text.match(ID_WORDS)?.length ?? 0;
  const en = text.match(EN_WORDS)?.length ?? 0;
  if (id > en) return "id";
  if (en > id) return "en";
  return null;
}

/** A server message that is safe to show: no stack traces or internal dumps, reasonable length. */
export function safeServerText(e: ApiError): string | null {
  const m = (e.message ?? "").trim();
  if (!m || m.length > 300 || /traceback|exception|error:|\bat \S+\(|\.py\b|sqlite|stack/i.test(m)) return null;
  return m;
}

/** 401 caused by wrong credentials typed into a form (not by an expired session). */
export function isCredentialError(e: unknown): boolean {
  return e instanceof ApiError && e.status === 401 && e.code === "AUTH_INVALID" && messageLocale(e.message) === "id";
}

export function errorText(error: unknown, t: ReturnType<typeof useT>["t"], locale: "id" | "en"): { title: string; body: string; expected: boolean } {
  const e = error instanceof ApiError ? error : null;
  const expected = !!e && e.status >= 400 && e.status < 500 && e.code !== "INTERNAL_ERROR";
  const notFound = e?.status === 404;
  const rateLimited = e?.status === 429 || e?.code === "RATE_LIMITED";
  // Gateway answers when the backend is asleep, restarting or offline (self-hosted tunnel down).
  const unavailable = !!e && (UNAVAILABLE_CODES.has(e.code) || [502, 503, 504].includes(e.status) || (e.status >= 520 && e.status <= 530));
  const quota = e?.code === "QUOTA_EXCEEDED";
  // Sign-in throttle: the server says whether this device (client) or the account as a whole is paused.
  const scope = rateLimited ? String(e?.details?.scope ?? "") : "";
  const serverText = expected && e ? safeServerText(e) : null;
  // The server's sentence is shown only when it is already in the reader's language.
  const lang = messageLocale(serverText);
  const serverInLocale = serverText && (lang === locale || (locale === "en" && lang === null)) ? serverText : null;
  const credential = isCredentialError(e);
  // Service switches that are off on purpose (no e-mail, no billing): explained, never "unavailable".
  const switchedOff = e?.code === "EMAIL_DISABLED" || e?.code === "BILLING_DISABLED";
  const title = e?.code === "MODEL_UNAVAILABLE" ? t("error.modelUnavailable")
    : e?.code === "EMAIL_DISABLED" ? t("error.emailDisabled")
    : e?.code === "BILLING_DISABLED" ? t("error.billingDisabled")
    : e?.code === "TEMP_PASSWORD_EXPIRED" ? t("error.tempPasswordExpired")
    : e?.code === "TOKEN_INVALID" ? t("error.tokenInvalid")
    : e?.code === "EMAIL_ALREADY_VERIFIED" ? t("error.emailAlreadyVerified")
    : e?.code === "MEMBER_ALREADY_ACTIVE" ? t("error.memberAlreadyActive")
    : quota ? t("error.quota")
    : rateLimited && (scope === "client" || scope === "account") ? t("error.signinPaused")
    : rateLimited ? t("error.rateLimited")
    : unavailable ? t("error.unavailable")
    : e?.code === "TIMEOUT" ? t("error.timeout")
    : e?.code === "OFFLINE" ? t("error.offline")
    : e?.code === "NETWORK_ERROR" ? t("error.network")
    : e?.code === "EMAIL_TAKEN" ? t("error.emailTaken")
    : e?.code === "EMAIL_UNAVAILABLE" ? t("error.emailUnavailable")
    : e?.code === "IDEMPOTENCY_KEY_REUSED" ? t("error.duplicateRequest")
    : e?.code === "IDEMPOTENCY_IN_PROGRESS" ? t("error.inProgress")
    : e?.code === "SELF_EDIT" ? t("error.selfEdit")
    : e?.code === "PLAN_LIMIT" || e?.code === "SEAT_LIMIT" ? t("error.planLimit")
    : e?.code === "FEATURE_NOT_IN_PLAN" ? t("error.featureNotInPlan")
    : e?.code === "AUTH_REQUIRED" ? t("error.authRequired")
    : credential ? t("error.checkInput")
    : e?.status === 401 ? t("error.unauthorized")
    : notFound ? t("error.notFound")
    : e?.status === 422 ? t("error.validation")
    : expected ? t("error.expected") : t("error.generic");
  const wait = e && rateLimited && !quota ? retryAfterSeconds(e) : null;
  const body = e?.code === "EMAIL_DISABLED" ? t("error.emailDisabledBody")
    : e?.code === "BILLING_DISABLED" ? t("error.billingDisabledBody")
    : e?.code === "TEMP_PASSWORD_EXPIRED" ? (serverInLocale ?? t("error.tempPasswordExpiredBody"))
    : e?.code === "TOKEN_INVALID" ? (serverInLocale ?? t("error.tokenInvalidBody"))
    : e?.code === "EMAIL_ALREADY_VERIFIED" ? (serverInLocale ?? t("error.emailAlreadyVerifiedBody"))
    : e?.code === "MEMBER_ALREADY_ACTIVE" ? (serverInLocale ?? t("error.memberAlreadyActiveBody"))
    : e?.code === "TIMEOUT" ? t("error.timeoutBody", { n: Number(e.details?.timeout_seconds) || 35 })
    : e?.code === "OFFLINE" ? t("error.offlineBody")
    : e?.code === "NETWORK_ERROR" ? t("error.networkBody")
    : quota ? t("error.quotaBody", { reset: String(e?.details?.reset_at ?? "").slice(0, 10) || "-" })
    : rateLimited && scope === "client" ? t("error.signinClientBody", { n: wait ?? "-" })
    : rateLimited && scope === "account" ? t("error.signinAccountBody", { n: wait ?? "-" })
    : rateLimited ? (wait ? t("error.rateLimitedBody", { n: wait }) : t("error.rateLimitedBodyNoTime"))
    : unavailable && e?.code !== "MODEL_UNAVAILABLE" ? t("error.unavailableBody")
    : e?.code === "EMAIL_TAKEN" ? (serverInLocale ?? t("error.emailTakenBody"))
    : e?.code === "EMAIL_UNAVAILABLE" ? (serverInLocale ?? t("error.emailUnavailableBody"))
    : e?.code === "IDEMPOTENCY_KEY_REUSED" ? t("error.duplicateRequestBody")
    : e?.code === "IDEMPOTENCY_IN_PROGRESS" ? t("error.inProgressBody")
    : e?.code === "PLAN_LIMIT" ? t("error.planLimitBody")
    : e?.code === "SEAT_LIMIT" ? t("error.seatLimitBody")
    : e?.code === "FEATURE_NOT_IN_PLAN" ? (serverInLocale ?? t("error.featureNotInPlanBody"))
    : serverInLocale ? serverInLocale
    : notFound ? t("error.notFoundBody")
    : e?.status === 403 ? t("error.forbiddenBody")
    : e?.status === 409 ? t("error.conflictBody")
    : e?.status === 422 ? t("error.validationBody")
    : e?.status === 401 ? t("error.unauthorizedBody")
    : expected ? t("error.expectedBody")
    : t("error.genericBody");
  return { title, body, expected: expected || switchedOff };
}

/**
 * Error callout. `signinLink={false}` on forms whose 401 means "wrong credentials" (sign-in,
 * password change), where a "Sign in" button would point to the page the user is already on.
 * The request id is shown for server faults and for 4xx answers support may need to trace
 * (conflicts, validation, limits); not for 401/404, where it adds nothing for the reader.
 * `titles` / `bodies` let a form give a code a context-specific title or body (by error code);
 * otherwise both come from errorText.
 */
export function ErrorState({ error, onRetry, compact, signinLink = true, titles, bodies, missing }:
  { error: unknown; onRetry?: () => void; compact?: boolean; signinLink?: boolean; titles?: Partial<Record<string, string>>; bodies?: Partial<Record<string, (e: ApiError) => string>>;
    /** Parts of the page that this error leaves out (a page-level error for several failed requests). */
    missing?: string[] }) {
  const { t, locale, intl } = useT();
  const location = useLocation();
  if (!error) return null;
  const e = error instanceof ApiError ? error : null;
  const text = errorText(error, t, locale);
  const { expected } = text;
  // A form may describe an error code more precisely in its own context (e.g. CONFLICT on sign-up,
  // RATE_LIMITED on a password change, whose server text speaks of sign-in attempts).
  const title = (e && titles?.[e.code]) || text.title;
  const body = (e && bodies?.[e.code]?.(e)) || text.body;
  const upgrade = e && (e.code === "PLAN_LIMIT" || e.code === "SEAT_LIMIT" || e.code === "FEATURE_NOT_IN_PLAN");
  const next = encodeURIComponent(location.pathname + location.search);
  const showRid = !!e?.requestId && (!expected || ![401, 404].includes(e.status));
  // Retrying cannot change a permission or plan decision.
  const retry = onRetry && !(e && (e.status === 403 || e.code === "EMAIL_DISABLED" || e.code === "BILLING_DISABLED"));
  const sessionProblem = e?.code === "AUTH_REQUIRED" || (e?.status === 401 && !isCredentialError(e));
  return (
    <div className={`callout ${expected ? "warn" : "bad"}${compact ? " compact" : ""}`} role="alert">
      {expected ? <TriangleAlert size={18} aria-hidden="true" /> : <CircleAlert size={18} aria-hidden="true" />}
      <div className="grow">
        <div className="callout-title">{title}</div>
        <div>{body}</div>
        {!!missing?.length && <div className="callout-missing">{t("common.missingParts", { list: listText(missing, intl) })}</div>}
        {showRid && <span className="rid">{t("common.requestId")}: <span className="mono">{e!.requestId}</span></span>}
      </div>
      {upgrade && <Link className="btn sm" to="/akun#paket">{t("common.upgrade")}</Link>}
      {signinLink && sessionProblem && <Link className="btn sm primary" to={`/masuk?next=${next}`}>{t("shell.signin")}</Link>}
      {retry && <Button size="sm" onClick={onRetry}>{t("common.retry")}</Button>}
    </div>
  );
}

export function EmptyState({ icon: Icon, illustration, title, children, action }: { icon?: LucideIcon; illustration?: ReactNode; title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="empty">
      {illustration ? <div className="empty-ill">{illustration}</div> : <div className="empty-icon">{Icon ? <Icon size={20} aria-hidden="true" /> : <Info size={20} aria-hidden="true" />}</div>}
      <div className="empty-title">{title}</div>
      {children && <div className="small empty-body">{children}</div>}
      {action}
    </div>
  );
}

/** Feature gated by sign-in or plan; always tells the user how to unlock it. */
export function GateNotice({ title, children, signin, upgrade }: { title: string; children?: ReactNode; signin?: boolean; upgrade?: boolean }) {
  const { t } = useT();
  const location = useLocation();
  const next = encodeURIComponent(location.pathname + location.search);
  return (
    <section className="panel locked">
      <IllUpgrade size={120} />
      <div className="grow">
        <div className="strong">{title}</div>
        {children && <div className="small muted mt-2">{children}</div>}
      </div>
      <div className="row">
        {signin && <Link className="btn primary" to={`/masuk?next=${next}`}><Lock size={15} aria-hidden="true" />{t("shell.signin")}</Link>}
        {upgrade && <Link className="btn" to="/akun#paket">{t("common.upgrade")}</Link>}
      </div>
    </section>
  );
}

export function Skeleton({ height = 16, width = "100%", style }: { height?: number | string; width?: number | string; style?: CSSProperties }) {
  return <div className="skeleton" style={{ height, width, ...style }} aria-hidden="true" />;
}

/** True once `active` has stayed true for `ms` milliseconds (a request that is taking unusually long). */
export function useSlow(active: boolean, ms = 8000): boolean {
  const [slow, setSlow] = useState(false);
  useEffect(() => {
    if (!active) { setSlow(false); return; }
    const id = window.setTimeout(() => setSlow(true), ms);
    return () => window.clearTimeout(id);
  }, [active, ms]);
  return active && slow;
}

/**
 * One page-level note while the page's data takes unusually long (8 s): the skeletons stay, and
 * the reader learns that the server is slow and how long the page waits before it explains a
 * failure (the client's default timeout). Renders nothing otherwise.
 */
export function SlowNotice({ active, afterMs = 8000 }: { active: boolean; afterMs?: number }) {
  const { t } = useT();
  const slow = useSlow(active, afterMs);
  return (
    <div role="status" aria-live="polite" className={slow ? undefined : "sr-only"}>
      {slow && <Callout tone="info" icon={LoaderCircle} title={t("common.slowTitle")}>{t("common.slowBody", { n: Math.round(TIMEOUTS.default / 1000) })}</Callout>}
    </div>
  );
}

export function LoadingBlock({ rows = 4 }: { rows?: number }) {
  const { t } = useT();
  return (
    <div className="stack-sm" role="status" aria-label={t("common.loading")}>
      {Array.from({ length: rows }, (_, i) => <Skeleton key={i} height={18} width={`${92 - i * 9}%`} />)}
    </div>
  );
}

/* ---------- Controls ------------------------------------------------------ */
export function Segmented<T extends string | number>({ options, value, onChange, label }:
  { options: Array<{ value: T; label: ReactNode; icon?: LucideIcon }>; value: T; onChange: (v: T) => void; label: string }) {
  return (
    <div className="segmented" role="group" aria-label={label}>
      {options.map((o) => (
        <button key={String(o.value)} type="button" aria-pressed={o.value === value} onClick={() => onChange(o.value)}>
          {o.icon && <o.icon size={14} aria-hidden="true" />}{o.label}
        </button>
      ))}
    </div>
  );
}

/** ids that tie a tab to its panel (`aria-controls` ↔ `aria-labelledby`). */
export const tabId = (idBase: string, value: string) => `${idBase}-tab-${value}`;
export const panelId = (idBase: string, value: string) => `${idBase}-panel-${value}`;
/** Props for the element that shows the selected tab's content. */
export function tabPanelProps(idBase: string, value: string) {
  return { role: "tabpanel" as const, id: panelId(idBase, value), "aria-labelledby": tabId(idBase, value), tabIndex: 0 };
}

/**
 * WAI-ARIA tabs: one tab stop (roving tabindex), Arrow Left/Right/Home/End move and select,
 * every tab controls a panel rendered with `tabPanelProps(idBase, value)`. Callers record the
 * selection in the URL with history *push*, so Back returns to the previous tab.
 */
export function Tabs<T extends string>({ tabs, value, onChange, label, idBase }:
  { tabs: Array<{ value: T; label: string; icon?: LucideIcon; count?: number }>; value: T; onChange: (v: T) => void; label: string; idBase: string }) {
  const listRef = useRef<HTMLDivElement>(null);
  const onKeyDown = (e: React.KeyboardEvent) => {
    const i = tabs.findIndex((tb) => tb.value === value);
    const next = e.key === "ArrowRight" ? (i + 1) % tabs.length : e.key === "ArrowLeft" ? (i - 1 + tabs.length) % tabs.length
      : e.key === "Home" ? 0 : e.key === "End" ? tabs.length - 1 : -1;
    if (next < 0) return;
    e.preventDefault();
    const tb = tabs[next]!;
    onChange(tb.value);
    listRef.current?.querySelector<HTMLElement>(`#${CSS.escape(tabId(idBase, tb.value))}`)?.focus();
  };
  return (
    <div className="tabs" role="tablist" aria-label={label} ref={listRef} onKeyDown={onKeyDown}>
      {tabs.map((tb) => {
        const selected = tb.value === value;
        return (
          <button key={tb.value} id={tabId(idBase, tb.value)} role="tab" type="button" aria-selected={selected} aria-controls={panelId(idBase, tb.value)}
            tabIndex={selected ? 0 : -1} onClick={() => onChange(tb.value)}>
            {tb.icon && <tb.icon size={15} aria-hidden="true" />}{tb.label}
            {tb.count !== undefined && <span className="count">{tb.count}</span>}
          </button>
        );
      })}
    </div>
  );
}

/**
 * Wrapper for a table that may scroll sideways (≥ 561 px). It is a focusable, named region so
 * keyboard users can scroll it; below 561 px tables marked `rc` become record cards instead.
 */
export function TableScroll({ label, children, className = "" }: { label: string; children: ReactNode; className?: string }) {
  return <div className={`table-scroll ${className}`} role="region" aria-label={label} tabIndex={0}>{children}</div>;
}

/**
 * Layout facts of a table and its frame (the element's parent, e.g. the `TableScroll` wrapper):
 * `narrow` while the frame is narrower than `threshold` px (the caller folds columns), `overflows`
 * while the element is wider than its frame (it must scroll inside the frame, never spill out of it).
 * Measured before paint and whenever the frame or the element resizes. Attach the returned callback
 * ref to the table; it also works for a table that only appears once data has loaded. Without layout
 * information (tests, no ResizeObserver) both stay false.
 */
export function useTableFrame<T extends HTMLElement>(threshold = 0): [(node: T | null) => void, { narrow: boolean; overflows: boolean }] {
  const [node, setNode] = useState<T | null>(null);
  const [fit, setFit] = useState({ narrow: false, overflows: false });
  useLayoutEffect(() => {
    const frame = node?.parentElement;
    if (!node || !frame) return;
    const check = () => {
      const w = frame.clientWidth;
      const narrow = w > 0 && w < threshold;
      const overflows = w > 0 && node.getBoundingClientRect().width > w + 1;
      setFit((f) => (f.narrow === narrow && f.overflows === overflows ? f : { narrow, overflows }));
    };
    check();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(check);
    ro.observe(frame);
    ro.observe(node);
    return () => ro.disconnect();
  }, [node, threshold]);
  return [setNode, fit];
}

/** True while the media query matches (false where matchMedia is unavailable, e.g. in tests). */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(() => typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia(query).matches);
  useEffect(() => {
    if (typeof window.matchMedia !== "function") return;
    const mq = window.matchMedia(query);
    const on = () => setMatches(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return matches;
}

export function Pager({ hasPrev, hasNext, onFirst, onNext }: { hasPrev: boolean; hasNext: boolean; onFirst: () => void; onNext: () => void }) {
  const { t } = useT();
  if (!hasPrev && !hasNext) return null;
  return (
    <div className="pager">
      <Button size="sm" variant="ghost" disabled={!hasPrev} onClick={onFirst}>{t("common.firstPage")}</Button>
      <Button size="sm" disabled={!hasNext} onClick={onNext}>{t("common.nextPage")}</Button>
    </div>
  );
}

export function Stat({ label, value, hint }: { label: string; value: ReactNode; hint?: ReactNode }) {
  return (
    <div className="stat">
      <span className="stat-label">{label}</span>
      <span className="stat-value">{value}</span>
      {hint && <span className="xs muted">{hint}</span>}
    </div>
  );
}

/** Value-kind chip: observed / derived / estimate / forecast. Never color-only. */
export function KindChip({ kind }: { kind: "observed" | "derived" | "estimate_awaiting_publication" | "future_forecast" | "legacy_record" | "scenario" | "assumed_hold_last" }) {
  const { t } = useT();
  const map = {
    // A value kind is a category, not a verdict: no "good" green (verification has its own badge).
    observed: { tone: "neutral" as Tone, label: t("common.observed") },
    derived: { tone: "neutral" as Tone, label: t("common.derived") },
    // Categories get their own neutral tokens: status colours (warn/bad) are reserved for problems.
    estimate_awaiting_publication: { tone: "cat-estimate" as Tone, label: t("kind.estimate_awaiting_publication") },
    future_forecast: { tone: "cat-forecast" as Tone, label: t("kind.future_forecast") },
    legacy_record: { tone: "outline" as Tone, label: t("kind.legacy_record") },
    scenario: { tone: "brand" as Tone, label: t("src.scenario") },
    assumed_hold_last: { tone: "warn" as Tone, label: t("src.assumed_hold_last") },
  };
  const m = map[kind];
  return <Badge tone={m.tone}>{m.label}</Badge>;
}

/**
 * Confirmation for consequential actions (archive, delete). Modal: focus moves to the safe
 * choice, Tab stays inside, Escape and the backdrop cancel, and focus returns to the trigger.
 * The confirm button shows progress and the dialog stays open until the server has answered.
 */
export function ConfirmDialog({ open, title, children, confirmLabel, onConfirm, onCancel, busy, danger = true }:
  { open: boolean; title: string; children?: ReactNode; confirmLabel: string; onConfirm: () => void; onCancel: () => void; busy?: boolean; danger?: boolean }) {
  const { t } = useT();
  const titleId = useId();
  const boxRef = useRef<HTMLDivElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);
  useModal(open, boxRef, { onClose: onCancel, initialFocus: cancelRef, locked: busy });
  if (!open) return null;
  // Portal to <body>: an ancestor with a transform/animation would otherwise clip the fixed backdrop.
  return createPortal(
    <div className="dialog-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget && !busy) onCancel(); }}>
      <div className="dialog" role="alertdialog" aria-modal="true" aria-labelledby={titleId} ref={boxRef}>
        <h2 id={titleId} className="dialog-title">{title}</h2>
        {children && <div className="small muted">{children}</div>}
        <div className="row dialog-actions">
          <Button ref={cancelRef} variant="ghost" onClick={onCancel} disabled={busy}>{t("common.cancel")}</Button>
          <Button variant={danger ? "danger-solid" : "primary"} onClick={onConfirm} loading={busy}>{confirmLabel}</Button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
