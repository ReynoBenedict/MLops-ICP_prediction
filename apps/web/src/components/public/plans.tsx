/**
 * Plans on the public site: the comparison table built from `GET /plans` (landing), the plan card
 * (landing on phones, account page) and the shared feature list. Every value comes from the API;
 * nothing is priced here (plan changes are requested in the app and approved by an operator).
 */
import type { PlanInfo } from "../../api/types";
import { Check, CloudOff, Minus, RefreshCw, TriangleAlert } from "lucide-react";
import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { ApiError } from "../../api/client";
import { labelFor, type LabelEn } from "../../api/product-types";
import { dateTime, fmtNum } from "../../format";
import { useT } from "../../i18n";
import { Button, errorText, Skeleton, useMediaQuery } from "../ui";
import { usePlans } from "./snapshot";

/** Whether an element's content is wider than the element (follows resizes). */
function useOverflowX<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const [over, setOver] = useState(false);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const check = () => setOver(el.scrollWidth > el.clientWidth + 1);
    check();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(check);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, over] as const;
}

/**
 * Every entitlement of a plan, as one list (landing page and account page share it): nothing is
 * left out, and each line says exactly what the server does (the API quota is counted and enforced).
 * `quotaExplained`: the page explains the quota rule once below the cards, so the line names only the quota.
 */
export function planFeatures(p: PlanInfo, t: ReturnType<typeof useT>["t"], intl: string, quotaExplained = false): Array<[boolean, string]> {
  return [
    [true, t("landing.plan.seats", { n: fmtNum(p.seats, intl, 0) })],
    [true, t("landing.plan.horizon", { n: p.forecast_horizon_max })],
    [true, p.history_years ? t("landing.plan.history", { n: p.history_years }) : t("landing.plan.historyAll")],
    [p.scenarios, p.scenarios ? t("landing.plan.scenarios", { n: fmtNum(p.saved_scenarios_max, intl, 0) }) : t("landing.plan.noScenarios")],
    [p.insights, p.insights ? t("landing.plan.insights") : t("landing.plan.noInsights")],
    [p.reports_pdf, t("landing.plan.reportsPdf")],
    [p.reports_excel, t("landing.plan.reportsExcel")],
    [true, t("landing.plan.reportsCsv")],
    [p.alerts, p.alerts ? t("landing.plan.alerts", { n: fmtNum(p.alert_rules_max, intl, 0) }) : t("landing.plan.noAlerts")],
    [p.api_access, p.api_access ? t(quotaExplained ? "landing.plan.apiQuota" : "landing.plan.apiCounted", { n: fmtNum(p.api_requests_per_month, intl, 0) }) : t("landing.plan.noApi")],
  ];
}

/** Report formats of a plan ("PDF, Excel, CSV"); CSV is part of every plan. */
export function reportFormats(p: PlanInfo): string[] {
  return [p.reports_pdf ? "PDF" : null, p.reports_excel ? "Excel" : null, "CSV"].filter((x): x is string => !!x);
}

/** Plan card: name and note in the active language (Indonesian fallback), complete feature list. */
export function PlanCard({ p, current, currentLabel, quotaExplained, children }: { p: PlanInfo & LabelEn; current?: boolean; currentLabel?: string; quotaExplained?: boolean; children?: ReactNode }) {
  const { t, intl, locale } = useT();
  const note = (locale === "en" ? p.price_note_en : null) ?? p.price_note_id;
  return (
    <div className={`plan${current ? " current" : ""}`}>
      <h3>{labelFor(p, locale)}{current && currentLabel && <> <span className="badge brand">{currentLabel}</span></>}</h3>
      {note && <div className="price-note">{note}</div>}
      {/* The icon is decorative; the "included / not included" state is also given as text (A11Y-03). */}
      <ul>{planFeatures(p, t, intl, quotaExplained).map(([on, label], i) => <li key={i} className={on ? "" : "off"}>{on ? <Check size={15} aria-hidden="true" /> : <Minus size={15} aria-hidden="true" />}{label}{!on && <span className="sr-only"> ({t("landing.plan.notIncluded")})</span>}</li>)}</ul>
      {children}
    </div>
  );
}

function Included({ on, children }: { on: boolean; children?: ReactNode }) {
  const { t } = useT();
  if (!on) return <span className="pt-off"><Minus size={15} aria-hidden="true" />{t("landing.plans.val.notIncluded")}</span>;
  return <span className="pt-on"><Check size={15} aria-hidden="true" />{children ?? t("landing.plans.val.included")}</span>;
}

/**
 * Plans side by side: one row per entitlement, one column per plan (from 641 px). Should the frame
 * ever scroll sideways, it becomes a focusable, named region so keyboard users can scroll it too.
 */
export function PlanTable({ plans }: { plans: Array<PlanInfo & LabelEn> }) {
  const { t, intl, locale } = useT();
  const [frameRef, over] = useOverflowX<HTMLDivElement>();
  const n = (v: number) => <span className="pt-num">{fmtNum(v, intl, 0)}</span>;
  const rows: Array<[string, (p: PlanInfo) => ReactNode]> = [
    [t("landing.plans.row.seats"), (p) => n(p.seats)],
    [t("landing.plans.row.horizon"), (p) => t("landing.plans.val.months", { n: p.forecast_horizon_max })],
    [t("landing.plans.row.history"), (p) => (p.history_years ? t("landing.plans.val.years", { n: p.history_years }) : t("landing.plans.val.full"))],
    [t("landing.plans.row.scenarios"), (p) => (p.scenarios ? n(p.saved_scenarios_max) : <Included on={false} />)],
    [t("landing.plans.row.insights"), (p) => <Included on={p.insights} />],
    [t("landing.plans.row.reports"), (p) => reportFormats(p).join(", ")],
    [t("landing.plans.row.alerts"), (p) => (p.alerts ? n(p.alert_rules_max) : <Included on={false} />)],
    [t("landing.plans.row.api"), (p) => (p.api_access ? <Included on>{t("landing.plans.val.apiQuota", { n: fmtNum(p.api_requests_per_month, intl, 0) })}</Included> : <Included on={false} />)],
  ];
  return (
    <div className="pt-frame" ref={frameRef} {...(over ? { tabIndex: 0, role: "region", "aria-label": t("landing.plans.caption") } : {})}>
      <table className="pt">
        <caption className="sr-only">{t("landing.plans.caption")}</caption>
        <thead>
          <tr>
            <th scope="col" className="pt-corner">{t("landing.plans.feature")}</th>
            {plans.map((p) => {
              const note = (locale === "en" ? p.price_note_en : null) || p.price_note_id;
              return (
                <th scope="col" key={p.code} className={p.code === "trial" ? "pt-plan self" : "pt-plan"}>
                  <span className="pt-name">{labelFor(p, locale)}</span>
                  {note && <span className="pt-note">{note}</span>}
                  {p.code === "trial" && <span className="badge brand pt-badge">{t("landing.plans.selfServe")}</span>}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map(([label, cell]) => (
            <tr key={label}>
              <th scope="row">{label}</th>
              {plans.map((p) => <td key={p.code} className={p.code === "trial" ? "self" : undefined}>{cell(p)}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * The plans block of the landing page with its loading, empty, stale (the gateway's stored copy,
 * contract C2: said with the time it was stored) and error states.
 */
export function PlansBlock() {
  const { t, intl, locale } = useT();
  const res = usePlans();
  const phone = useMediaQuery("(max-width: 640px)");
  const plans = res.data?.data ?? null;
  if (plans && plans.length === 0) return <p className="pt-empty">{t("landing.plans.empty")}</p>;
  if (plans) {
    const cachedAt = res.data?.cachedAt ?? null;
    return (
      <>
        {res.data?.stale && (
          <p className="pt-stale"><CloudOff size={16} aria-hidden="true" />
            <span>{cachedAt ? t("landing.plans.stale", { time: dateTime(cachedAt, intl) }) : t("landing.plans.staleNoTime")}</span></p>
        )}
        {phone
          ? <div className="plan-grid">{plans.map((p) => <PlanCard key={p.code} p={p} quotaExplained />)}</div>
          : <PlanTable plans={plans} />}
      </>
    );
  }
  if (res.loading) {
    return <div className="pt-loading" role="status" aria-label={t("landing.plans.loading")}><Skeleton height={44} /><Skeleton height={280} /></div>;
  }
  const e = res.error instanceof ApiError ? res.error : null;
  const body = e && e.status >= 500 ? t("landing.panel.errorUnavailable") : errorText(res.error, t, locale).body;
  return (
    <div className="pt-error">
      <TriangleAlert size={18} aria-hidden="true" />
      <div className="grow">
        <p className="strong">{t("landing.plans.errorTitle")}</p>
        <p className="small">{body}</p>
      </div>
      <Button size="sm" icon={RefreshCw} onClick={res.reload}>{t("common.retry")}</Button>
    </div>
  );
}
