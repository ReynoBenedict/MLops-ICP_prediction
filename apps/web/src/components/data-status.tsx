/**
 * Data status control of the signed-in top bar: "ICP resmi s.d. September 2026" plus the freshness
 * chip of the official ICP. It opens a small non-modal popover that lists every source (official
 * ICP, Brent, Dubai, WTI, JISDOR) with its latest period, provider and freshness, and links to the
 * per-month evidence on the price history page (`/harga#bukti`).
 *
 * Data: GET /data/summary through `sharedGet` (`loadSummary`), so the shell and the overview page
 * share one request. It is read again on every route change (at most once a minute thanks to the
 * shared window) and, after a failure, retried by itself with a growing delay or as soon as a page
 * reads the summary successfully (the overview's retry). Freshness words come
 * from the server status (fresh / lagging / stale / unknown); nothing here claims "real-time" or
 * "verified".
 *
 * Keyboard: the button reports `aria-expanded`; opening moves focus into the popover (a labelled
 * dialog); Escape closes it and returns focus to the button; a click outside, or focus leaving the
 * popover, closes it without moving focus. On phones it is a compact dot + month and the popover
 * spans the width under the top bar.
 */
import type { DataSummary } from "../api/types";
import { ArrowRight, ChevronDown, CircleCheck, CircleHelp, LoaderCircle, TriangleAlert } from "lucide-react";
import { useEffect, useId, useRef, useState, type FocusEvent, type KeyboardEvent } from "react";
import { Link, useLocation } from "react-router-dom";
import { sharedGet } from "../api/client";
import { useResource } from "../api/hooks";
import { dateTime, period } from "../format";
import { useT, type Key } from "../i18n";
import { ErrorState, Skeleton, StatusBadge, statusTone } from "./ui";

/** Sources listed in the data status popover and the overview status strip, in this order. */
export const STATUS_SOURCES = ["icp", "brent", "dubai", "wti", "usd_idr"] as const;
export type StatusSource = (typeof STATUS_SOURCES)[number];

/** Reuse window for the shared /data/summary read (shell + overview page). */
export const SUMMARY_MAX_AGE_MS = 60_000;

/** Called after every successful summary read (so the top bar recovers when a page's retry succeeds). */
const summaryLoaded = new Set<() => void>();

/** One shared read of /data/summary: concurrent callers (top bar, overview) wait for the same request. */
export function loadSummary(signal?: AbortSignal): Promise<DataSummary> {
  const p = sharedGet<DataSummary>("/data/summary", { signal, maxAgeMs: SUMMARY_MAX_AGE_MS });
  p.then(() => { for (const f of summaryLoaded) f(); }, () => undefined);
  return p;
}

/**
 * Freshness label of one dataset: the status word, plus "n months behind" when the server says so.
 * A lagging source says it once ("Tertinggal 1 bulan"), not "Tertinggal · tertinggal 1 bulan".
 */
export function freshnessLabel(f: { status: string; months_behind?: number | null } | null | undefined, t: ReturnType<typeof useT>["t"], td: ReturnType<typeof useT>["td"]): string {
  if (!f) return td("status", "unknown");
  const word = td("status", f.status);
  if (!f.months_behind || f.months_behind <= 0) return word;
  if (f.status === "lagging") return t("shell.status.laggingBy", { n: f.months_behind });
  return `${word} · ${t("shell.status.behind", { n: f.months_behind })}`;
}

/** Re-read delays after a failed status read (ms); the last one repeats while the server stays away. */
export const STATUS_RETRY_MS = [15_000, 30_000, 60_000, 120_000];

function ToneIcon({ status }: { status: string }) {
  const tone = statusTone(status);
  const Icon = tone === "good" ? CircleCheck : tone === "neutral" ? CircleHelp : TriangleAlert;
  return <span className={`ds-dot ${tone}`}><Icon size={14} aria-hidden="true" /></span>;
}

export function DataStatus({ enabled }: { enabled: boolean }) {
  const { t, td, locale, intl } = useT();
  const { pathname } = useLocation();
  // Re-read on every route change (the shared 60 s window makes that one request at most per minute,
  // shared with the page), so a recovered server or a new publication shows up without a reload.
  const summary = useResource((s) => (enabled ? loadSummary(s) : Promise.resolve(null)), [enabled, pathname]);
  // After a failed read, try again by itself with a growing delay until the server answers.
  const attempt = useRef(0);
  const { error: summaryError, reload: reloadSummary } = summary;
  useEffect(() => {
    if (!enabled || !summaryError) { attempt.current = 0; return; }
    const delay = STATUS_RETRY_MS[Math.min(attempt.current, STATUS_RETRY_MS.length - 1)]!;
    const id = window.setTimeout(() => { attempt.current += 1; reloadSummary(); }, delay);
    return () => window.clearTimeout(id);
  }, [enabled, summaryError, reloadSummary]);
  // A page read the summary successfully (e.g. the overview's retry) while this control shows a
  // failure: read it too (served from the shared result, no extra request).
  const failedIdle = useRef(false);
  failedIdle.current = !!summary.error && !summary.loading;
  useEffect(() => {
    if (!enabled) return;
    const onLoaded = () => { if (failedIdle.current) { failedIdle.current = false; reloadSummary(); } };
    summaryLoaded.add(onLoaded);
    return () => { summaryLoaded.delete(onLoaded); };
  }, [enabled, reloadSummary]);
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const uid = useId();
  const popId = `${uid}-pop`, titleId = `${uid}-title`;

  // Close on navigation (the evidence link, the sidebar) and on a click or tap outside.
  useEffect(() => { setOpen(false); }, [pathname]);
  useEffect(() => {
    if (!open) return;
    popRef.current?.focus({ preventScroll: true });
    const onDown = (e: MouseEvent | TouchEvent) => { if (!wrapRef.current?.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("touchstart", onDown);
    return () => { document.removeEventListener("mousedown", onDown); document.removeEventListener("touchstart", onDown); };
  }, [open]);

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key !== "Escape" || !open) return;
    e.preventDefault();
    e.stopPropagation();
    setOpen(false);
    btnRef.current?.focus();
  };
  // Focus moved somewhere outside the control (Tab past the last link): the popover closes.
  const onBlur = (e: FocusEvent) => {
    const next = e.relatedTarget as Node | null;
    if (open && next && !wrapRef.current?.contains(next)) setOpen(false);
  };

  if (!enabled) return null;
  const data = summary.data;
  const icp = data?.datasets.icp;
  const fresh = icp?.freshness ?? null;

  let face;
  if (data && icp?.available && icp.period_end) {
    face = (<>
      <span className="sr-only">{t("shell.status.prefix")}</span>
      <span className="ds-full">{t("shell.status.button", { period: period(icp.period_end, locale, true) })}</span>
      <span className="ds-short"><span className="sr-only">ICP </span>{period(icp.period_end, locale)}</span>
      <span className="ds-full"><StatusBadge status={fresh?.status ?? "unknown"} /></span>
      <span className="ds-short"><ToneIcon status={fresh?.status ?? "unknown"} /><span className="sr-only"> {td("status", fresh?.status ?? "unknown")}</span></span>
    </>);
  } else if (summary.loading && !data && !summary.error) {
    // (A background re-read after a failure keeps the "unavailable" face until it succeeds.)
    // Phones keep the text for screen readers only (.ds-text is visually hidden there).
    face = (<><LoaderCircle size={14} className="spin" aria-hidden="true" /><span className="ds-text">{t("shell.status.loading")}</span></>);
  } else {
    face = (<><span className="ds-dot warn"><TriangleAlert size={14} aria-hidden="true" /></span><span className="ds-text">{t("shell.status.unavailable")}</span></>);
  }

  return (
    <div className="ds" ref={wrapRef} onKeyDown={onKeyDown} onBlur={onBlur}>
      <button ref={btnRef} type="button" className="ds-btn" aria-expanded={open} aria-controls={open ? popId : undefined} aria-haspopup="dialog"
        onClick={() => { setOpen((o) => !o); if (summary.error) summary.reload(); }}>
        {face}
        <ChevronDown size={14} className="ds-chev" aria-hidden="true" />
      </button>
      {open && (
        <div ref={popRef} id={popId} className="ds-pop" role="dialog" aria-labelledby={titleId} tabIndex={-1}>
          <div className="ds-pop-head">
            <h2 id={titleId} className="ds-pop-title">{t("shell.status.title")}</h2>
            <p className="ds-pop-lead">{t("shell.status.lead")}</p>
          </div>
          {data ? (
            <table className="ds-table">
              <caption className="sr-only">{t("shell.status.title")}</caption>
              <thead><tr><th scope="col">{t("shell.status.colSource")}</th><th scope="col" className="num">{t("shell.status.colPeriod")}</th><th scope="col">{t("shell.status.colState")}</th></tr></thead>
              <tbody>
                {STATUS_SOURCES.map((k) => {
                  const d = data.datasets[k];
                  return (
                    <tr key={k}>
                      <th scope="row"><span className="ds-src">{t(`shell.src.${k}` as Key)}</span><span className="ds-prov">{t(`provider.${k}` as Key)}</span></th>
                      <td className="num">{d?.available && d.period_end ? period(d.period_end, locale) : t("shell.status.notLoaded")}</td>
                      <td>{d?.available ? <StatusBadge status={d.freshness?.status ?? "unknown"} label={freshnessLabel(d.freshness, t, td)} /> : <StatusBadge status="unknown" />}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : summary.error ? (
            <div className="ds-pop-body"><ErrorState error={summary.error} compact onRetry={summary.reload} /></div>
          ) : (
            <div className="ds-pop-body stack-sm" role="status" aria-label={t("common.loading")}>
              {STATUS_SOURCES.map((k) => <Skeleton key={k} height={22} />)}
            </div>
          )}
          <div className="ds-pop-foot">
            {icp?.updated_at && <span className="xs muted">{t("shell.status.loaded", { date: dateTime(icp.updated_at, intl) })}</span>}
            <Link className="link-inline small" to="/harga#bukti" onClick={() => setOpen(false)}>{t("shell.status.evidence")} <ArrowRight size={13} aria-hidden="true" /></Link>
          </div>
        </div>
      )}
    </div>
  );
}
