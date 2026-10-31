/**
 * Ringkasan pasar (/ringkasan). Reading order:
 * 1. Lead card: the official ICP of the latest month with its verification chip (/data/verification),
 *    the headline number, the change against the previous month and a year ago (with the values
 *    compared), the rupiah equivalent with the JISDOR rate it used (derived), the 12-month range and
 *    the source line (provider, decree number only when the server records one, load time).
 * 2. Benchmarks card: Brent, Dubai, WTI (latest month, ICP − benchmark spread and its mean) and JISDOR.
 * 3. "Langkah pertama" for new organisations (compact, below the headline number on every width).
 * 4. Chart: official ICP vs Brent with the latest issued forecast segment and its 80% band, range
 *    1/3/5 years in the URL (`?rentang=`), table view.
 * 5. Latest forecast (origin, issue date, method, assumption, three months) and the insight brief.
 * 6. Data status strip: freshness per source and evidence counts, linking to /harga#bukti.
 * Every block owns its loading, empty and error state; /data/summary is read once (shared with the
 * top bar's data status).
 */
import type { ForecastRun, HistoryResponse, OrganizationDetail, ReportList } from "../api/types";
import { ArrowRight, CircleCheck, CircleDashed, FileText, Play, X } from "lucide-react";
import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { api, nullOn404 } from "../api/client";
import { useResource } from "../api/hooks";
import { CONFIRMED_BASES } from "../api/product-types";
import { useAuth } from "../auth";
import { loadSummary, STATUS_SOURCES, freshnessLabel } from "../components/data-status";
import { AssumptionNote, ForecastChart, ForecastTable, methodLabel } from "../components/forecast";
import { IllNoForecast } from "../components/illustrations";
import { Callout, Delta, EmptyState, ErrorState, KindChip, PageHeader, Panel, Segmented, Skeleton, SlowNotice, Sparkline, StatusBadge, useMediaQuery } from "../components/ui";
import { dateOnly, dateTime, fmtIdr, fmtNum, fmtSigned, fmtUsd, period, rangeText, shiftPeriod } from "../format";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/market.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { seriesContext } from "../lib/market";
import { loadInsights, NOT_IN_PLAN } from "../lib/plan";
import { EvidenceBadge, useVerification } from "../lib/verification";

const BENCHES = ["brent", "dubai", "wti"] as const;
const BENCH_NAME: Record<(typeof BENCHES)[number], string> = { brent: "Brent", dubai: "Dubai", wti: "WTI" };

/** localStorage key for the dismissed onboarding card (per organisation and member, this browser only). */
export const onboardingKey = (orgId: string | null, name: string | null) => `icp.onboarding.dismissed.${orgId ?? "none"}:${name ?? ""}`;
function readFlag(key: string): boolean { try { return localStorage.getItem(key) === "1"; } catch { return false; } }
function writeFlag(key: string): void { try { localStorage.setItem(key, "1"); } catch { /* per-browser convenience only */ } }

/** Chart ranges in years; `?rentang=` in the URL. The plan's history caps them. The default is 3 years,
 *  1 year on phones (560 px and below), where 3 years leave the forecast months a few pixels wide. */
const RANGES = [1, 3, 5] as const;

/** "Langkah pertama": dismissible, never blocking; steps tick when the matching data exists. */
function FirstSteps() {
  const { t } = useT();
  const { principal, isUser } = useAuth();
  const key = onboardingKey(principal.orgId, principal.name);
  const [dismissed, setDismissed] = useState(() => readFlag(key));
  const org = useResource((s) => isUser && !dismissed ? api<OrganizationDetail>("/org", { signal: s }).catch(() => null) : Promise.resolve(null), [isUser, dismissed]);
  const reports = useResource((s) => isUser && !dismissed ? api<ReportList>("/reports", { query: { limit: 1 }, signal: s }).catch(() => null) : Promise.resolve(null), [isUser, dismissed]);
  if (!isUser || dismissed || !org.data) return null;
  const steps: Array<{ to: string; label: Key; done: boolean | null }> = [
    { to: "/harga", label: "onboard.s1", done: null },
    { to: "/skenario", label: "onboard.s2", done: org.data.saved_scenarios > 0 },
    { to: "/laporan?jenis=bulanan", label: "onboard.s3", done: (reports.data?.items.length ?? 0) > 0 },
    { to: "/pemberitahuan", label: "onboard.s4", done: org.data.alert_rules > 0 },
  ];
  // Established organisations (every data step done) do not see the card.
  if (steps.every((s) => s.done !== false)) return null;
  return (
    <section className="panel onboard" aria-labelledby="onboard-title">
      <div className="onboard-head">
        <div className="grow">
          <h2 className="panel-title" id="onboard-title">{t("onboard.title")}</h2>
          <p className="panel-sub">{t("onboard.lead")}</p>
        </div>
        <button type="button" className="icon-close" aria-label={t("onboard.dismiss")} title={t("onboard.dismiss")} onClick={() => { writeFlag(key); setDismissed(true); }}><X size={16} aria-hidden="true" /></button>
      </div>
      <ol className="onboard-steps">
        {steps.map((s, i) => (
          <li key={s.to} className={s.done ? "done" : undefined}>
            <span className="onboard-n" aria-hidden="true">{s.done ? <CircleCheck size={16} /> : i + 1}</span>
            <span className="onboard-text">
              <Link to={s.to}>{t(s.label)}</Link>
              {s.done !== null && <span className="xs muted">{s.done ? t("onboard.done") : t("onboard.todo")}</span>}
            </span>
          </li>
        ))}
      </ol>
      <p className="xs muted onboard-help">{t("onboard.help")}</p>
    </section>
  );
}

export default function OverviewPage() {
  const { t, td, locale, intl } = useT();
  const { principal, isUser } = useAuth();
  const [params, setParams] = useSearchParams();
  const phone = useMediaQuery("(max-width: 560px)");
  const summary = useResource((s) => loadSummary(s), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => nullOn404(api<ForecastRun>("/forecasts/latest", { signal: s })), []);
  const insights = useResource((s) => loadInsights(principal, s, locale), [principal.plan, principal.kind, locale]);
  const insightData = insights.data && insights.data !== NOT_IN_PLAN ? insights.data : null;
  const verification = useVerification("icp");

  const s = summary.data;
  const icp = s?.datasets.icp;
  const icpPeriod = icp?.available ? icp.period_end ?? null : null;
  const points = useMemo(() => history.data?.points ?? [], [history.data]);
  // Chart labels and the year-ago comparison only; narratives come from the insight engine.
  const ctx = useMemo(() => seriesContext(points, "icp_price"), [points]);
  // The last 12 calendar months of the official ICP (missing months stay null: a break, never filled).
  const year = useMemo(() => {
    if (!ctx.latestPeriod) return null;
    const byPeriod = new Map(points.map((p) => [p.period, p.icp_price ?? null]));
    const months = Array.from({ length: 12 }, (_, i) => shiftPeriod(ctx.latestPeriod!, i - 11));
    const values = months.map((m) => byPeriod.get(m) ?? null);
    const observed = months.map((m, i) => ({ m, v: values[i] })).filter((x): x is { m: string; v: number } => typeof x.v === "number");
    if (observed.length < 2) return null;
    const low = observed.reduce((a, b) => (b.v < a.v ? b : a));
    const high = observed.reduce((a, b) => (b.v > a.v ? b : a));
    return { from: months[0]!, to: months[11]!, values, low, high };
  }, [points, ctx.latestPeriod]);
  const topInsights = (insightData?.insights ?? []).slice(0, 3);

  // Range options: 1/3/5 years, capped by the plan's history (a 2-year plan also gets "2 thn").
  const planYears = history.data?.history_years ?? null;
  const options: number[] = RANGES.filter((n) => planYears === null || n <= planYears);
  if (planYears !== null && planYears > 0 && planYears < 5 && !options.includes(planYears)) options.push(planYears);
  if (!options.length) options.push(1);
  const defaultRange = phone ? 1 : 3;
  const asked = Number(params.get("rentang"));
  const wanted = Number.isInteger(asked) && asked > 0 ? asked : defaultRange;
  const range = options.includes(wanted) ? wanted : wanted > options.at(-1)! ? options.at(-1)! : options[0]!;
  const setRange = (n: number) => {
    const next = new URLSearchParams(params);
    if (n === defaultRange) next.delete("rentang"); else next.set("rentang", String(n));
    setParams(next, { replace: true });
  };

  // Evidence counts in the plan range (server labels; only two_documents / decree_text count as confirmed).
  const icpMonths = points.filter((p) => p.icp_price != null);
  const confirmed = icpMonths.filter((p) => CONFIRMED_BASES.has(verification.forPeriod(p.period).basis));
  const notConfirmed = icpMonths.filter((p) => !CONFIRMED_BASES.has(verification.forPeriod(p.period).basis));
  const noEvidence = notConfirmed.filter((p) => verification.forPeriod(p.period).basis === "no_evidence");
  const weaker = notConfirmed.filter((p) => verification.forPeriod(p.period).basis !== "no_evidence");
  const list = (ps: typeof points) => ps.slice(-8).map((p) => period(p.period, locale)).join(", ") + (ps.length > 8 ? "…" : "");
  const leadVerification = icpPeriod && verification.ready ? verification.forPeriod(icpPeriod) : null;

  const run = latest.data?.result ? latest.data : null;
  const loadingBlock = (h: number) => <div role="status" aria-label={t("common.loading")}><Skeleton height={h} /></div>;

  // One failing request is that block's problem: it explains itself and offers its own retry. When
  // several fail, the data service is down: one page-level message with one retry, and the blocks
  // only say that their data is not available (no wall of identical alerts).
  const sources = [summary, history, latest, insights, verification];
  const failed = sources.filter((r) => !!r.error);
  const partNames: Key[] = ["overview.part.summary", "overview.part.history", "overview.part.latest", "overview.part.insights", "overview.part.verification"];
  const missingParts = sources.flatMap((r, i) => (r.error ? [t(partNames[i]!)] : []));
  const outage = failed.length >= 2;
  const retryFailed = () => { for (const r of sources) if (r.error) r.reload(); };
  const blockError = (r: { error: unknown; reload: () => void }, compact = true) => (outage
    ? <p className="muted small ov-unavailable">{t("common.dataUnavailable")}</p>
    : <ErrorState error={r.error} onRetry={r.reload} compact={compact} />);

  return (
    <div className="page">
      <PageHeader title={t("overview.title")} description={t("overview.desc")}
        meta={icpPeriod ? <>{t("overview.asOf", { period: period(icpPeriod, locale, true) })}{icp?.updated_at && <> · {t("overview.asOfLoaded", { date: dateTime(icp.updated_at, intl) })}</>}</> : undefined}
        actions={<>
          <Link className="btn primary" to="/proyeksi"><Play size={16} aria-hidden="true" />{t("forecast.run")}</Link>
          {isUser && <Link className="btn" to="/laporan?jenis=bulanan"><FileText size={16} aria-hidden="true" />{t("overview.actionReport")}</Link>}
        </>} />
      <SlowNotice active={(summary.loading && !s) || (history.loading && !history.data) || (latest.loading && !latest.data && !latest.error)} />
      {outage && <ErrorState error={failed[0]!.error} onRetry={retryFailed} missing={missingParts} />}

      <div className="ov-top">
        {/* Lead card: the official number, in context. */}
        <section className="panel ov-lead" aria-labelledby="ov-lead-title">
          <div className="ov-lead-head">
            <h2 id="ov-lead-title" className="ov-lead-label">{icpPeriod ? t("overview.leadLabel", { period: period(icpPeriod, locale, true) }) : t("overview.leadLabelShort")}</h2>
            {leadVerification ? <EvidenceBadge v={leadVerification} />
              : verification.error && icpPeriod ? <span className="badge neutral">{t("overview.evidenceUnavailable")}</span> : null}
          </div>
          {s && icp?.available ? (<>
            <div className="ov-lead-body">
              <div className="ov-lead-main">
                <p className="ov-hero"><span className="ov-hero-num">{fmtNum(icp.latest_value, intl)}</span><span className="ov-hero-unit">{t("common.usdPerBbl")}</span></p>
                <dl className="ov-changes">
                  <div className="ov-change">
                    <dt>{t("overview.changeMonth", { period: period(icpPeriod ? shiftPeriod(icpPeriod, -1) : null, locale, true) })}</dt>
                    <dd>{icp.mom_change_pct != null ? <><Delta value={icp.mom_change_pct} /> <span className="ov-change-from">{t("overview.fromValue", { v: fmtUsd(icp.previous_value, intl) })}</span></>
                      : <span className="muted">{t("overview.notConsecutive")}</span>}</dd>
                  </div>
                  {ctx.yoyPct != null && ctx.latestPeriod === icpPeriod && (
                    <div className="ov-change">
                      <dt>{t("overview.changeMonth", { period: period(ctx.yoyPeriod, locale, true) })}</dt>
                      <dd><Delta value={ctx.yoyPct} /> <span className="ov-change-from">{t("overview.fromValue", { v: fmtUsd(ctx.yoy, intl) })}</span></dd>
                    </div>
                  )}
                </dl>
              </div>
              <div className="ov-lead-side">
                <div className="ov-fact">
                  <div className="ov-fact-label">{t("overview.idrEquiv")} <KindChip kind="derived" /></div>
                  {s.icp_idr ? (<>
                    <div className="ov-fact-value"><span className="num">{fmtIdr(s.icp_idr.icp_idr, intl)}</span> <span className="ov-fact-unit">{t("common.perBarrel")}</span></div>
                    <div className="ov-fact-note">{t("overview.idrBasis", { period: period(s.icp_idr.period, locale, true), rate: fmtIdr(s.icp_idr.usd_idr, intl) })}</div>
                  </>) : <div className="ov-fact-note">{t("explorer.noFx")}</div>}
                </div>
                {year && ctx.latestPeriod === icpPeriod && (
                  <div className="ov-fact">
                    <div className="ov-fact-label">{t("overview.range12", { from: period(year.from, locale), to: period(year.to, locale) })}</div>
                    <Sparkline values={year.values} />
                    <dl className="ov-range-ends">
                      <div><dt>{t("overview.range12LowAt", { period: period(year.low.m, locale) })}</dt><dd className="num">{fmtUsd(year.low.v, intl)}</dd></div>
                      <div className="ov-range-high"><dt>{t("overview.range12HighAt", { period: period(year.high.m, locale) })}</dt><dd className="num">{fmtUsd(year.high.v, intl)}</dd></div>
                    </dl>
                  </div>
                )}
              </div>
            </div>
            <p className="ov-lead-foot">
              {t("overview.sourceProvider", { provider: t("provider.icp") })}
              {leadVerification?.reference && <> · {t("overview.decree", { ref: leadVerification.reference })}</>}
              {icp.updated_at && <> · {t("overview.asOfLoaded", { date: dateTime(icp.updated_at, intl) })}</>}
            </p>
          </>) : summary.loading && !s ? (
            <div className="ov-lead-body" role="status" aria-label={t("common.loading")}><Skeleton height={56} width="55%" /><Skeleton height={88} /></div>
          ) : summary.error ? (
            <div className="ov-lead-body is-state">{blockError(summary, false)}</div>
          ) : (
            <div className="ov-lead-body is-state"><p className="muted">{t("overview.leadUnavailable")}</p></div>
          )}
        </section>

        {/* Benchmarks and FX, same month. */}
        <Panel title={t("overview.benchTitle")} className="ov-bench" subtitle={icpPeriod ? t("overview.benchSub", { period: period(icpPeriod, locale, true) }) : undefined}
          footer={<Link className="link-inline" to="/harga">{t("overview.toHistory")} <ArrowRight size={13} aria-hidden="true" /></Link>}>
          {s ? (
            <ul className="ov-bench-list">
              {BENCHES.map((b) => {
                const d = s.datasets[b];
                const rel = s.relationships[b];
                return (
                  <li key={b}>
                    <span className={`ov-swatch ${b}`} aria-hidden="true" />
                    <span className="ov-bench-name">{BENCH_NAME[b]}<span className="ov-bench-meta">{d?.period_end ? period(d.period_end, locale) : ""}</span></span>
                    <span className="ov-bench-value num">{d?.available ? fmtUsd(d.latest_value, intl) : t("common.notAvailable")}</span>
                    {rel && <span className="ov-bench-spread">{t("overview.benchSpread", { b: BENCH_NAME[b], v: fmtSigned(rel.spread_latest, intl) })} · {t("overview.benchSpreadMean", { n: rel.months, m: fmtSigned(rel.spread_mean, intl) })}</span>}
                  </li>
                );
              })}
              <li>
                <span className="ov-swatch fx" aria-hidden="true" />
                <span className="ov-bench-name">{t("overview.fxName")}<span className="ov-bench-meta">{s.datasets.usd_idr?.period_end ? period(s.datasets.usd_idr.period_end, locale) : ""}</span></span>
                <span className="ov-bench-value num">{s.datasets.usd_idr?.available ? fmtIdr(s.datasets.usd_idr.latest_value, intl) : t("common.notAvailable")}</span>
                <span className="ov-bench-spread">{t("overview.perUsd")} · {t("provider.usd_idr")}</span>
              </li>
            </ul>
          ) : summary.loading ? loadingBlock(180) : <p className="muted small">{outage ? t("common.dataUnavailable") : t("overview.benchUnavailable")}</p>}
        </Panel>
      </div>

      <FirstSteps />

      <Panel title={range === 1 ? t("overview.chartTitleYear") : t("overview.chartTitleYears", { n: range })} className="ov-chart"
        subtitle={run ? t("overview.chartSub", { date: dateOnly(run.issue_date ?? null, intl) }) : t("overview.chartSubNone")}
        actions={history.data ? <Segmented label={t("overview.chartRange")} value={range} onChange={setRange} options={options.map((n) => ({ value: n, label: t("explorer.years", { n }) }))} /> : undefined}>
        {history.loading && !history.data ? loadingBlock(340)
          : history.error ? blockError(history, false)
          : <ForecastChart history={points} run={run} origin={ctx.latestPeriod} level="0.8" showBrent months={12 * range} />}
      </Panel>

      <div className="grid cols-2 align-start">
        <Panel title={t("overview.forecastTitle")}
          subtitle={run ? t("overview.forecastOrigin", { origin: period(run.forecast_origin, locale), date: dateOnly(run.issue_date ?? null, intl), method: methodLabel(run.method, t) }) : undefined}
          footer={run ? <Link className="link-inline" to={`/proyeksi?proyeksi=${encodeURIComponent(run.id)}`}>{t("overview.forecastOpen")} <ArrowRight size={13} aria-hidden="true" /></Link> : undefined}>
          {latest.loading && latest.data === null ? loadingBlock(160) : latest.error ? blockError(latest) : run?.result ? (
            <div className="stack-sm">
              <p className="small muted">{t("overview.forecastHonest")}</p>
              <AssumptionNote points={run.result.points} history={points} compact />
              <ForecastTable points={run.result.points.slice(0, 3)} level="0.8" compact history={points} />
            </div>
          ) : <EmptyState illustration={<IllNoForecast size={140} />} title={t("overview.noForecast")}>{t("overview.noForecastBody")}</EmptyState>}
        </Panel>

        <Panel title={t("overview.summaryTitle")}
          footer={<Link className="link-inline" to="/insight">{t("overview.summaryMore")} <ArrowRight size={13} aria-hidden="true" /></Link>}>
          {insights.loading && !insights.data ? loadingBlock(160) : insights.error ? blockError(insights) : insights.data === NOT_IN_PLAN ? (
            <Callout tone="info" title={t("ins.notInPlanTitle")} action={<Link className="btn sm" to="/akun#paket">{t("forecast.plansLink")}</Link>}>{t("ins.notInPlanBody")}</Callout>
          ) : topInsights.length ? (
            <ul className="insight-brief">
              {topInsights.map((i) => (
                <li key={i.id}>
                  <Link className="insight-brief-title" to={`/insight?insight=${encodeURIComponent(i.id)}`}>{i.title}</Link>
                  <p className="small muted insight-brief-body">{i.body}</p>
                </li>
              ))}
            </ul>
          ) : <EmptyState title={t("ins.empty")}>{t("ins.emptyBody")}</EmptyState>}
        </Panel>
      </div>

      {/* Data status strip: freshness per source and the evidence behind the official ICP. */}
      <Panel title={t("overview.statusTitle")} subtitle={t("overview.statusSub")} className="ov-status"
        footer={<Link className="link-inline" to="/harga#bukti">{t("overview.qualityLink")} <ArrowRight size={13} aria-hidden="true" /></Link>}>
        <div className="stack-sm">
          {s ? (
            <ul className="ov-status-list">
              {STATUS_SOURCES.map((k) => {
                const d = s.datasets[k];
                return (
                  <li key={k}>
                    <span className="ov-status-name">{t(`shell.src.${k}` as Key)}</span>
                    <span className="ov-status-period num">{d?.available && d.period_end ? period(d.period_end, locale) : t("shell.status.notLoaded")}</span>
                    <StatusBadge status={d?.available ? d.freshness?.status ?? "unknown" : "unknown"} label={d?.available ? freshnessLabel(d.freshness, t, td) : undefined} />
                  </li>
                );
              })}
              <li className="ov-status-evidence">
                <span className="ov-status-name">{t("overview.evidenceLabel")}</span>
                {verification.error || history.error ? <span className="muted small">{t("common.notAvailable")}</span>
                  : verification.ready && history.data ? <span className="ov-status-period num">{t("overview.evidenceCount", { c: confirmed.length, n: icpMonths.length, range: icpMonths.length ? rangeText(period(icpMonths[0]!.period, locale), period(icpMonths.at(-1)!.period, locale), locale) : "-" })}</span>
                  : <span role="status" aria-label={t("common.loading")}><Skeleton height={18} width={160} /></span>}
              </li>
            </ul>
          ) : summary.loading ? loadingBlock(64) : <p className="muted small">{outage ? t("common.dataUnavailable") : t("common.notAvailable")}</p>}
          {verification.error ? (outage ? null : <ErrorState error={verification.error} compact onRetry={verification.reload} />) : verification.ready && history.data ? (<>
            {icp?.missing_periods?.length ? <Callout tone="warn">{t("overview.qualityMissing", { n: icp.missing_periods.length, list: icp.missing_periods.map((p) => period(p, locale)).join(", ") })}</Callout> : null}
            {noEvidence.length ? <p className="note-line"><CircleDashed size={15} aria-hidden="true" />{t("overview.qualityNoEvidence", { n: noEvidence.length, list: list(noEvidence) })}</p> : null}
            {weaker.length ? <p className="note-line"><CircleDashed size={15} aria-hidden="true" />{t("overview.qualityPending", { n: weaker.length, list: list(weaker) })}</p> : null}
            {!icp?.missing_periods?.length && !notConfirmed.length && icpMonths.length > 0 && <p className="note-line"><CircleCheck size={15} aria-hidden="true" />{t("overview.qualityOk")}</p>}
          </>) : null}
        </div>
      </Panel>
    </div>
  );
}
