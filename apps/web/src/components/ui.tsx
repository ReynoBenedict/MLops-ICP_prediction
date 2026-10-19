/**
 * Design-system primitives. Every page composes these; page files contain no ad-hoc styling
 * beyond layout utilities from styles.css. All visible text comes from the i18n dictionary.
 */
import {
  CircleAlert, CircleCheck, CircleX, Info, LoaderCircle, Lock, Minus, TrendingDown, TrendingUp, TriangleAlert,
  type LucideIcon,
} from "lucide-react";
import { useEffect, useId, useRef, type ButtonHTMLAttributes, type CSSProperties, type ReactNode, type Ref } from "react";
import { createPortal } from "react-dom";
import { Link, useLocation } from "react-router-dom";
import { ApiError } from "../api/client";
import { useT } from "../i18n";
import { IllUpgrade } from "./illustrations";

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
type BtnVariant = "primary" | "secondary" | "ghost" | "danger" | "danger-solid" | "amber";
export function Button({ variant = "secondary", size, icon: Icon, loading, children, className = "", ref, ...rest }:
  ButtonHTMLAttributes<HTMLButtonElement> & { variant?: BtnVariant; size?: "sm" | "lg"; icon?: LucideIcon; loading?: boolean; ref?: Ref<HTMLButtonElement> }) {
  const cls = ["btn", variant !== "secondary" ? variant : "", size ?? "", className].filter(Boolean).join(" ");
  return (
    <button ref={ref} className={cls} {...rest} disabled={rest.disabled || loading} aria-busy={loading || undefined}>
      {loading ? <LoaderCircle size={16} className="spin" aria-hidden="true" /> : Icon ? <Icon size={16} aria-hidden="true" /> : null}
      {children}
    </button>
  );
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
export function PageHeader({ eyebrow, icon: Icon, title, description, actions }:
  { eyebrow: string; icon?: LucideIcon; title: string; description?: ReactNode; actions?: ReactNode }) {
  return (
    <header className="page-header">
      <div>
        <div className="eyebrow">{Icon && <Icon size={14} aria-hidden="true" />}{eyebrow}</div>
        <h1 className="page-title">{title}</h1>
        {description && <p className="page-desc">{description}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
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

export function Kpi({ label, icon: Icon, iconTone, value, unit, meta, tone, children }:
  { label: string; icon?: LucideIcon; iconTone?: "icp" | "wti" | "brand" | "amber"; value: ReactNode; unit?: string; meta?: ReactNode; tone?: "warn" | "bad"; children?: ReactNode }) {
  return (
    <div className={`kpi${tone ? ` tone-${tone}` : ""}`}>
      <div className="kpi-top">
        <span className="kpi-label">{label}</span>
        {Icon && <span className={`kpi-icon ${iconTone ?? ""}`}><Icon size={16} aria-hidden="true" /></span>}
      </div>
      <div className="kpi-value">{typeof value === "string" || typeof value === "number" ? <span className="kpi-num">{value}</span> : value}{unit && <span className="unit">{unit}</span>}</div>
      {meta && <div className="kpi-meta">{meta}</div>}
      {children}
    </div>
  );
}

/** Signed change with direction icon; text carries the sign so meaning never relies on color. */
export function Delta({ value, suffix = " %", digits = 1, title }: { value: number | null | undefined; suffix?: string; digits?: number; title?: string }) {
  const { intl } = useT();
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  const dir = Math.abs(value) < 0.05 ? "flat" : value > 0 ? "up" : "down";
  const Icon = dir === "up" ? TrendingUp : dir === "down" ? TrendingDown : Minus;
  const n = new Intl.NumberFormat(intl, { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(Math.abs(value));
  return (
    <span className={`delta ${dir}`} title={title}>
      <Icon size={13} aria-hidden="true" />
      {value > 0 ? "+" : value < 0 ? "−" : ""}{n}{suffix}
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
 * Error presentation for end users. Only authored API messages for expected (4xx) outcomes are
 * shown verbatim; server faults, network faults and client exceptions get a generic explanation
 * plus the request id as a "reference code" so support can trace it without exposing internals.
 */
const UNAVAILABLE_CODES = new Set(["UPSTREAM_TIMEOUT_OR_UNAVAILABLE", "GATEWAY_MISCONFIGURED", "UPSTREAM_REDIRECT", "DATA_PLANE_MISCONFIGURED"]);

export function ErrorState({ error, onRetry, compact }: { error: unknown; onRetry?: () => void; compact?: boolean }) {
  const { t } = useT();
  const location = useLocation();
  if (!error) return null;
  const e = error instanceof ApiError ? error : null;
  const expected = !!e && e.status >= 400 && e.status < 500 && e.code !== "INTERNAL_ERROR";
  const notFound = e?.status === 404;
  // Gateway answers when the backend is asleep, restarting or offline (self-hosted tunnel down).
  const unavailable = !!e && (UNAVAILABLE_CODES.has(e.code) || [502, 503, 504].includes(e.status) || (e.status >= 520 && e.status <= 530));
  const title = e?.code === "MODEL_UNAVAILABLE" ? t("error.modelUnavailable")
    : unavailable ? t("error.unavailable")
    : e?.code === "NETWORK_ERROR" ? t("error.network")
    : e?.code === "PLAN_LIMIT" || e?.code === "SEAT_LIMIT" ? t("error.planLimit")
    : e?.code === "FEATURE_NOT_IN_PLAN" ? t("error.featureNotInPlan")
    : e?.code === "AUTH_REQUIRED" ? t("error.authRequired")
    : e?.status === 401 ? t("error.unauthorized")
    : notFound ? t("error.notFound")
    : e?.status === 422 ? t("error.validation")
    : expected ? t("error.expected") : t("error.generic");
  const body = e?.code === "NETWORK_ERROR" ? t("error.networkBody")
    : unavailable && e?.code !== "MODEL_UNAVAILABLE" ? t("error.unavailableBody")
    : notFound ? t("error.notFoundBody")
    : expected && e.message && !/traceback|exception|error:/i.test(e.message) ? e.message
    : t("error.genericBody");
  const upgrade = e && (e.code === "PLAN_LIMIT" || e.code === "SEAT_LIMIT" || e.code === "FEATURE_NOT_IN_PLAN");
  const next = encodeURIComponent(location.pathname + location.search);
  return (
    <div className={`callout ${expected ? "warn" : "bad"}`} role="alert" style={compact ? { padding: "10px 14px" } : undefined}>
      {expected ? <TriangleAlert size={18} aria-hidden="true" /> : <CircleAlert size={18} aria-hidden="true" />}
      <div className="grow">
        <div className="callout-title">{title}</div>
        <div>{body}</div>
        {e?.requestId && !expected && <span className="rid">{t("common.requestId")}: <span className="mono">{e.requestId}</span></span>}
      </div>
      {upgrade && <Link className="btn sm" to="/akun#paket">{t("common.upgrade")}</Link>}
      {(e?.code === "AUTH_REQUIRED" || e?.status === 401) && <Link className="btn sm primary" to={`/masuk?next=${next}`}>{t("shell.signin")}</Link>}
      {onRetry && <Button size="sm" onClick={onRetry}>{t("common.retry")}</Button>}
    </div>
  );
}

export function EmptyState({ icon: Icon, illustration, title, children, action }: { icon?: LucideIcon; illustration?: ReactNode; title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="empty">
      {illustration ? <div className="empty-ill">{illustration}</div> : <div className="empty-icon">{Icon ? <Icon size={20} aria-hidden="true" /> : <Info size={20} aria-hidden="true" />}</div>}
      <div className="empty-title">{title}</div>
      {children && <div className="small" style={{ maxWidth: 460 }}>{children}</div>}
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

export function Tabs<T extends string>({ tabs, value, onChange, label }:
  { tabs: Array<{ value: T; label: string; icon?: LucideIcon; count?: number }>; value: T; onChange: (v: T) => void; label: string }) {
  return (
    <div className="tabs" role="tablist" aria-label={label}>
      {tabs.map((tb) => (
        <button key={tb.value} role="tab" type="button" aria-selected={tb.value === value} onClick={() => onChange(tb.value)}>
          {tb.icon && <tb.icon size={15} aria-hidden="true" />}{tb.label}
          {tb.count !== undefined && <span className="count">{tb.count}</span>}
        </button>
      ))}
    </div>
  );
}

export function Pager({ hasPrev, hasNext, onFirst, onNext }: { hasPrev: boolean; hasNext: boolean; onFirst: () => void; onNext: () => void }) {
  const { t } = useT();
  if (!hasPrev && !hasNext) return null;
  return (
    <div className="row" style={{ justifyContent: "flex-end", padding: "12px 22px" }}>
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
    observed: { tone: "good" as Tone, label: t("common.observed") },
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
  const returnTo = useRef<HTMLElement | null>(null);
  useEffect(() => {
    if (!open) return;
    returnTo.current = document.activeElement as HTMLElement | null;
    cancelRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) { e.preventDefault(); onCancel(); }
      if (e.key === "Tab" && boxRef.current) {
        const f = [...boxRef.current.querySelectorAll<HTMLElement>("button:not([disabled])")];
        if (!f.length) return;
        const first = f[0]!, last = f[f.length - 1]!;
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
      }
    };
    document.addEventListener("keydown", onKey);
    return () => { document.removeEventListener("keydown", onKey); returnTo.current?.focus?.(); };
  }, [open, busy, onCancel]);
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
