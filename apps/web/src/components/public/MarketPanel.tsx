/**
 * The live market panel of the landing hero: the latest official ICP with its verification, the
 * month-on-month change, Brent and the rupiah equivalent, the 24-month chart (or its table), the
 * locked forecast window and the source documents. Every state is designed and keeps the frame:
 * - loading: a labelled skeleton shaped like the content, with a "still waiting" line after 3 s;
 * - stale (gateway's stored copy, contract C2): values plus an info notice saying when the copy was
 *   stored; never a "fresh"/Mutakhir badge (FE-02);
 * - error without data: what happened, a retry, and the official ESDM page (no skeleton survives);
 *   on phones the error drops the kept height so it is read on the first screen;
 * - no `recent` (older backend): the facts without the chart;
 * - a latest month whose official documents disagree (`icp_evidence.basis` "conflict"): the server
 *   withholds its ICP (and so the rupiah equivalent); the headline, the rupiah fact, the legend, the
 *   table, the tooltip and the summary call it withheld, not "no official value" (values exist; they
 *   disagree).
 * Guests never see forecast values: only the teaser's horizon and issue date are used.
 */
import type { MarketSnapshot } from "../../api/types";
import { CircleAlert, CloudOff, Lock, RefreshCw } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { ApiError } from "../../api/client";
import { loc } from "../../api/product-types";
import { useAuth } from "../../auth";
import { dateOnly, dateTime, fmtIdr, fmtNum, fmtSigned, fmtUsd, period, shiftPeriod } from "../../format";
import { useT } from "../../i18n";
import { EvidenceBadge } from "../../lib/verification";
import { Button, Delta, errorText, Segmented, Skeleton, StatusBadge } from "../ui";
import { ExternalA } from "./ExternalA";
import { chartSummary, gapRuns, MarketChart, noIcpText } from "./MarketChart";
import { docLabel, ESDM_ICP_URL, forecastTeaser, impliedRate, PUBLIC_WAIT_SECONDS, recentMonths, useSlow, type EvidenceDoc, type PublicMonth, type SnapshotResource } from "./snapshot";

/** Months after `a` up to and including `b` (YYYY-MM); 0 when b is not after a. */
function monthsAfter(a: string, b: string): number {
  const [ya, ma] = a.split("-").map(Number) as [number, number];
  const [yb, mb] = b.split("-").map(Number) as [number, number];
  return Math.max(0, (yb * 12 + mb) - (ya * 12 + ma));
}

export function MarketPanel({ res }: { res: SnapshotResource }) {
  const { t } = useT();
  const data = res.data?.data ?? null;
  const waiting = res.loading && !data;
  const slow = useSlow(waiting);
  return (
    <section className={`mkt${data ? "" : waiting ? " mkt-pending" : " mkt-pending mkt-failed"}`} aria-label={t("landing.panel.region")} aria-busy={waiting || undefined}>
      {data ? <PanelData d={data} stale={res.data?.stale ?? false} cachedAt={res.data?.cachedAt ?? null} />
        : waiting ? <PanelLoading slow={slow} />
        : <PanelError error={res.error} onRetry={res.reload} />}
    </section>
  );
}

/** Loading: the panel's own layout in skeleton form (head, number, facts, chart, lock line, footer). */
function PanelLoading({ slow }: { slow: boolean }) {
  const { t } = useT();
  return (
    <div className="mkt-state" role="status" aria-label={t("landing.panel.loading")}>
      <div className="mkt-body">
        <div className="mkt-head"><Skeleton height={18} width="36%" /><Skeleton height={24} width={168} /></div>
        <div className="mkt-figure"><Skeleton height={50} width={210} /><Skeleton height={20} width={260} /></div>
        <div className="mkt-facts mkt-facts-skel" aria-hidden="true">
          <div><Skeleton height={12} width="52%" /><Skeleton height={20} width="40%" /></div>
          <div><Skeleton height={12} width="60%" /><Skeleton height={20} width="46%" /><Skeleton height={12} width="70%" /></div>
        </div>
        <div className="mkt-chart-skel"><Skeleton height={14} width="32%" /><Skeleton height={12} width="24%" /><Skeleton height={248} /></div>
        <Skeleton height={40} />
      </div>
      <div className="mkt-foot">
        <p className="mkt-wait-text">{slow ? t("landing.panel.slow", { n: PUBLIC_WAIT_SECONDS }) : t("landing.panel.loadingText")}</p>
      </div>
    </div>
  );
}

/**
 * Error without data: the frame keeps its place and shape; one alert says what happened, offers a
 * retry and the official ESDM page, where the figure can still be read.
 */
function PanelError({ error, onRetry }: { error: unknown; onRetry: () => void }) {
  const { t, locale } = useT();
  const e = error instanceof ApiError ? error : null;
  const serverDown = !!e && (e.status >= 500 || e.code === "UPSTREAM_TIMEOUT_OR_UNAVAILABLE");
  const body = serverDown ? t("landing.panel.errorUnavailable") : errorText(error, t, locale).body;
  return (
    <div className="mkt-state">
      <div className="mkt-body">
        <div className="mkt-head"><p className="mkt-title"><span className="mkt-label">{t("landing.panel.labelLatest")}</span></p></div>
        <div className="mkt-error" role="alert">
          <span className="mkt-error-icon" aria-hidden="true"><CircleAlert size={20} /></span>
          <p className="mkt-error-title">{t("landing.panel.errorTitle")}</p>
          <p className="mkt-error-text">{body}</p>
          <div className="mkt-error-actions">
            <Button icon={RefreshCw} onClick={onRetry}>{t("common.retry")}</Button>
            <ExternalA href={ESDM_ICP_URL} className="btn ghost">{t("landing.panel.esdmLink")}</ExternalA>
          </div>
        </div>
      </div>
      <div className="mkt-foot"><p>{t("landing.panel.esdmHint")}</p></div>
    </div>
  );
}

function PanelData({ d, stale, cachedAt }: { d: MarketSnapshot; stale: boolean; cachedAt: string | null }) {
  const { t, intl, locale } = useT();
  const { principal } = useAuth();
  const [view, setView] = useState<"chart" | "table">("chart");
  // The chart draws in once: on the first chart of this page view, not after a table/chart toggle.
  const drawn = useRef(false);
  const animate = !drawn.current;
  useEffect(() => { drawn.current = true; }, []);

  const months = recentMonths(d);
  const teaser = forecastTeaser(d.latest_forecast);
  const last = months[months.length - 1]?.period ?? d.icp_period;
  const windowKnown = !!(teaser?.horizon && teaser.origin && last);
  const zone = teaser?.horizon ? (teaser.origin && last ? monthsAfter(last, shiftPeriod(teaser.origin, teaser.horizon)) : teaser.horizon) : 0;
  // A forecast whose months are all official by now has nothing left to lock: no lock line for it.
  const windowPassed = windowKnown && zone === 0;
  const evidence = d.icp_evidence ?? null;
  const freshness = String((d.freshness as { status?: unknown } | null)?.status ?? "");
  const rate = impliedRate(d);
  // The latest month's ICP is withheld (not missing) when its official documents disagree.
  const withheld = evidence?.basis === "conflict" ? evidence.period : null;
  const heldShown = !!withheld && months.some((m) => m.period === withheld && m.icp === null);
  const missing = gapRuns(months.map((m) => (m.period === withheld ? 0 : m.icp))).length > 0;
  const gapLegend = missing && heldShown ? "landing.chart.gapBoth" : heldShown ? "landing.chart.gapWithheld" : missing ? "landing.chart.gap" : null;
  const signedIn = principal.kind !== "public";
  const lockText = teaser && !windowPassed
    ? teaser.horizon && teaser.issueDate
      ? t(signedIn ? "landing.panel.lockUser" : "landing.panel.lock", { n: teaser.horizon, date: dateOnly(teaser.issueDate, intl) })
      : t(signedIn ? "landing.panel.lockShortUser" : "landing.panel.lockShort")
    : null;
  const announcement = evidence?.documents.find((x) => x.kind === "announcement") ?? null;
  const decree = evidence?.documents.find((x) => x.kind === "decree") ?? null;

  return (
    <>
      <div className="mkt-body">
        {stale && (
          <div className="mkt-stale">
            <CloudOff size={16} aria-hidden="true" />
            <p>{cachedAt ? t("landing.panel.stale", { time: dateTime(cachedAt, intl) }) : t("landing.panel.staleNoTime")}</p>
          </div>
        )}
        <div className="mkt-head">
          <p className="mkt-title"><span className="mkt-label">{t("landing.panel.label")}</span> <span className="mkt-period">{period(d.icp_period, locale, true)}</span></p>
          <div className="mkt-chips">
            {evidence && <span className="mkt-chip"><EvidenceBadge v={{ basis: evidence.basis, label: loc(evidence.label, locale) ?? evidence.basis, explanation: null }} /></span>}
            {stale ? (!evidence && <span className="badge neutral">{t("landing.panel.storedCopy")}</span>)
              : freshness && (!evidence || freshness !== "fresh") && <StatusBadge status={freshness} />}
          </div>
        </div>

        <div className="mkt-figure">
          <p className="mkt-value">
            {d.icp_usd == null ? <span className="mkt-na">{t(evidence?.basis === "conflict" ? "landing.panel.withheld" : "landing.chart.noValue")}</span>
              : <><span className="mkt-num">{fmtNum(d.icp_usd, intl, 2)}</span><span className="mkt-unit">{t("landing.panel.unit")}</span></>}
          </p>
          {d.mom_change_pct != null && (
            <p className="mkt-change"><Delta value={d.mom_change_pct} />
              {d.previous_usd != null && <span>{t("landing.panel.from", { value: fmtUsd(d.previous_usd, intl), period: period(d.previous_period, locale, true) })}</span>}
            </p>
          )}
        </div>

        <dl className="mkt-facts">
          <div>
            <dt>{t("landing.panel.brent", { period: period(d.brent_period, locale, true) })}</dt>
            <dd>{d.brent_usd == null ? <span className="mkt-fact-na">{t("landing.chart.noBrent")}</span>
              : <><span className="mkt-fact-num">{fmtUsd(d.brent_usd, intl)}</span> <span className="mkt-fact-unit">{t("landing.panel.perBbl")}</span></>}</dd>
          </div>
          <div>
            <dt>{t("landing.panel.idr", { period: period(d.icp_period, locale, true) })}</dt>
            <dd>{d.icp_idr == null ? <span className="mkt-fact-na">{t(d.icp_usd == null && evidence?.basis === "conflict" ? "landing.panel.idrWithheld" : "landing.chart.noBrent")}</span>
              : <><span className="mkt-fact-num">{fmtIdr(d.icp_idr, intl)}</span> <span className="mkt-fact-unit">{t("landing.panel.perBbl")}</span>
                {rate !== null && <span className="mkt-fact-sub">{t("landing.panel.idrRate", { rate: fmtIdr(rate, intl) })}</span>}</>}</dd>
          </div>
        </dl>

        {months.length > 1 ? (
          <div className="mkt-chart-block">
            <div className="mkt-chart-head">
              <div className="mkt-chart-meta">
                <p className="mkt-chart-title">{t("landing.chart.title", { n: months.length })}</p>
                <ul className="mkt-legend">
                  <li><span className="mkt-key icp" />{t("landing.chart.icp")}</li>
                  <li><span className="mkt-key brent" />{t("landing.chart.brent")}</li>
                  {gapLegend && <li><span className="mkt-key gap" />{t(gapLegend)}</li>}
                </ul>
              </div>
              <Segmented label={t("landing.chart.view")} value={view} onChange={setView}
                options={[{ value: "chart", label: t("landing.chart.viewChart") }, { value: "table", label: t("landing.chart.viewTable") }]} />
            </div>
            <p className="sr-only">{chartSummary(months, t, intl, locale, withheld)}</p>
            {view === "chart" ? <MarketChart months={months} zone={zone} animate={animate} withheld={withheld} /> : <MarketTable months={months} withheld={withheld} />}
          </div>
        ) : (
          <p className="mkt-nochart">{t("landing.panel.noChart")}</p>
        )}

        {lockText ? (
          <Link className="mkt-lock" to={signedIn ? "/proyeksi" : "/masuk?next=%2Fproyeksi"}>
            {/* The hatch key ties this line to the hatched window, so it is shown only while the chart is. */}
            {months.length > 1 && zone > 0 && view === "chart" && <span className="mkt-key hatch" aria-hidden="true" />}
            <Lock size={14} aria-hidden="true" />{lockText}
          </Link>
        ) : !teaser && <p className="mkt-noforecast">{t("landing.panel.noForecast")}</p>}
      </div>

      <footer className="mkt-foot">
        <div className="mkt-docs">
          <span className="mkt-foot-label">{t(announcement || decree ? "landing.panel.sources" : "landing.panel.sourcesPlain")}</span>
          {announcement || decree ? (
            <>
              {announcement && <DocLink doc={announcement} />}
              {decree && <DocLink doc={decree} />}
            </>
          ) : <span className="mkt-doc">{t("landing.panel.sourceFallback")}</span>}
        </div>
        {d.updated_at && <p className="mkt-updated">{t("landing.panel.updated", { time: dateTime(d.updated_at, intl) })}</p>}
      </footer>
    </>
  );
}

/** A source document of the latest month: a link to the government page when stored (new tab), else its name. */
function DocLink({ doc }: { doc: EvidenceDoc }) {
  const { t, intl } = useT();
  const cls = doc.kind === "decree" ? "mkt-doc stamp" : "mkt-doc";
  const label = docLabel(doc, t, intl);
  return doc.url ? <ExternalA href={doc.url} className={cls}>{label}</ExternalA> : <span className={cls}>{label}</span>;
}

/** The chart's values as a table (newest month first), same months and units; `withheld` as in MarketChart. */
function MarketTable({ months, withheld }: { months: PublicMonth[]; withheld: string | null }) {
  const { t, intl, locale } = useT();
  const rows = [...months].reverse();
  const first = months[0]?.period, last = months[months.length - 1]?.period;
  const none = (s: string) => <span className="mkt-none">{s}</span>;
  return (
    <div className="mkt-table-wrap" role="region" aria-label={t("landing.chart.tableRegion")} tabIndex={0}>
      <table className="table mkt-table">
        <caption className="sr-only">{t("landing.chart.tableCaption", { from: period(first, locale, true), to: period(last, locale, true) })}</caption>
        <thead>
          <tr>
            <th scope="col">{t("landing.chart.month")}</th>
            <th scope="col" className="num">{t("landing.chart.icp")}</th>
            <th scope="col" className="num">{t("landing.chart.brent")}</th>
            <th scope="col" className="num">{t("landing.chart.spread")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((m) => (
            <tr key={m.period}>
              <th scope="row">{period(m.period, locale)}</th>
              <td className="num">{m.icp === null ? none(noIcpText(m.period, withheld, t)) : fmtNum(m.icp, intl, 2)}</td>
              <td className="num">{m.brent === null ? none(t("landing.chart.noBrent")) : fmtNum(m.brent, intl, 2)}</td>
              <td className="num">{m.icp !== null && m.brent !== null ? fmtSigned(m.icp - m.brent, intl, 2) : none("-")}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
