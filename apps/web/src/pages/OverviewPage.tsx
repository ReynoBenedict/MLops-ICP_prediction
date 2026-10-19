/** Ringkasan Pasar: latest official ICP, benchmarks, FX, rupiah equivalent, freshness, chart, summary, data-quality notes. */
import type { DataSummary, EvidenceResponse, ForecastRun, HistoryResponse, InsightsResponse } from "../api/types";
import { ArrowRight, Calendar, Database, Droplets, Landmark, LayoutDashboard, Lightbulb, ShieldCheck, Wallet } from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api, nullOn404 } from "../api/client";
import { useResource } from "../api/hooks";
import { ForecastChart, ForecastTable, type Level, methodLabel } from "../components/forecast";
import { IllNoForecast, MarkBarrel, MarkDecree, MarkFx } from "../components/illustrations";
import { Callout, Delta, EmptyState, ErrorState, Kpi, PageHeader, Panel, Skeleton, StatusBadge } from "../components/ui";
import { dateOnly, dateTime, fmtIdr, fmtNum, fmtUsd, period, shiftPeriod } from "../format";

const SOURCE_MARK = { icp: MarkDecree, brent: MarkBarrel, dubai: MarkBarrel, wti: MarkBarrel, usd_idr: MarkFx } as const;
const SOURCE_KIND = { icp: "overview.sourceKind.decree", brent: "overview.sourceKind.market", dubai: "overview.sourceKind.market", wti: "overview.sourceKind.market", usd_idr: "overview.sourceKind.fx" } as const;
import { useT, type Key } from "../i18n";
import { seriesContext } from "../lib/market";
import { loadInsights, NOT_IN_PLAN } from "../lib/plan";
import { useAuth } from "../auth";

export default function OverviewPage() {
  const { t, locale, intl } = useT();
  const summary = useResource((s) => api<DataSummary>("/data/summary", { signal: s }), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => nullOn404(api<ForecastRun>("/forecasts/latest", { signal: s })), []);
  const { principal } = useAuth();
  const insights = useResource((s) => loadInsights(principal, s), [principal.plan, principal.kind]);
  const insightData: InsightsResponse | null = insights.data && insights.data !== NOT_IN_PLAN ? insights.data : null;
  const evidence = useResource((s) => api<EvidenceResponse>("/data/evidence", { query: { dataset: "icp" }, signal: s }), []);
  const [level] = useState<Level>("0.8");

  const s = summary.data;
  const icp = s?.datasets.icp;
  const points = useMemo(() => history.data?.points ?? [], [history.data]);
  const ctx = useMemo(() => seriesContext(points, "icp_price"), [points]);
  const pendingMonths = useMemo(() => {
    const byPeriod = new Map<string, string>();
    for (const e of evidence.data?.items ?? []) {
      const cur = byPeriod.get(e.period);
      if (!cur || e.verification_status === "verified") byPeriod.set(e.period, e.verification_status);
    }
    return [...byPeriod.entries()].filter(([, st]) => ["single_source", "machine_extracted", "conflict", "pending"].includes(st)).map(([p]) => p).sort();
  }, [evidence.data]);
  const topInsights = (insightData?.insights ?? []).slice(0, 3);
  const chartHistory = useMemo(() => points.filter((p) => p.period >= shiftPeriod(ctx.latestPeriod ?? "2026-01", -59)), [points, ctx.latestPeriod]);
  // The plan may clip history (e.g. 3 years): then the title says where the chart really starts.
  const clipped = (history.data?.history_years ?? null) !== null && (history.data?.history_years ?? 99) < 5;
  const firstPlotted = chartHistory.find((p) => p.icp_price != null)?.period ?? history.data?.earliest_period ?? null;
  const fresh = icp?.freshness;

  return (
    <div className="page">
      <PageHeader eyebrow={t("overview.eyebrow")} icon={LayoutDashboard} title={t("overview.title")} description={t("overview.desc")}
        actions={<Link className="btn primary" to="/proyeksi">{t("nav.forecast")} <ArrowRight size={16} aria-hidden="true" /></Link>} />

      {summary.error && <ErrorState error={summary.error} onRetry={summary.reload} />}

      <div className="grid cols-4">
        <Kpi label={t("overview.latestIcp")} icon={Droplets} iconTone="icp" unit={t("common.usdPerBbl")}
          value={icp?.available ? fmtNum(icp.latest_value, intl) : summary.loading ? <Skeleton height={34} width={120} /> : "-"}
          meta={icp?.period_end ? <>{period(icp.period_end, locale, true)} · {fresh && <StatusBadge status={fresh.status} />}</> : undefined} />
        <Kpi label={t("overview.mom")} icon={Calendar} iconTone="brand"
          value={icp ? (icp.mom_change_pct != null ? <Delta value={icp.mom_change_pct} /> : <span className="muted">-</span>) : summary.loading ? <Skeleton height={34} width={90} /> : "-"}
          meta={icp?.previous_value != null ? `${t("overview.previous")} ${period(ctx.prevPeriod, locale)}: ${fmtUsd(icp.previous_value, intl)}` : icp ? t("overview.notConsecutive") : undefined}>
          {ctx.yoyPct != null && <div className="kpi-meta">{t("overview.yoy")}: <Delta value={ctx.yoyPct} /> <span className="xs">{t("overview.vsYearAgo", { period: period(ctx.yoyPeriod, locale) })}</span></div>}
        </Kpi>
        <Kpi label={t("overview.idrEquiv")} icon={Wallet} iconTone="amber" unit={t("common.idrPerBbl")}
          value={s?.icp_idr ? fmtNum(s.icp_idr.icp_idr, intl, 0) : summary.loading ? <Skeleton height={34} width={150} /> : "-"}
          meta={s?.icp_idr ? <span title={s.icp_idr.note}>{t("overview.idrDerivation")}</span> : s ? t("explorer.noFx") : undefined} />
        <Kpi label={t("overview.fx")} icon={Landmark} iconTone="brand"
          value={s?.datasets.usd_idr?.available ? fmtIdr(s.datasets.usd_idr.latest_value, intl) : "-"}
          meta={s?.datasets.usd_idr?.period_end ? t("overview.fxMeta", { period: period(s.datasets.usd_idr.period_end, locale) }) : undefined} />
      </div>

      <div className="grid split-main align-start">
        <Panel title={clipped && firstPlotted ? t("overview.chartTitleSince", { period: period(firstPlotted, locale) }) : t("overview.chartTitle")} icon={Droplets}
          subtitle={latest.data?.result ? t("overview.chartSub", { date: dateOnly(latest.data.issue_date ?? null, intl) }) : undefined}>
          {history.loading && !history.data ? <Skeleton height={340} /> : history.error ? <ErrorState error={history.error} onRetry={history.reload} /> :
            latest.data?.result ? <ForecastChart history={chartHistory} run={latest.data} level={level} showBrent months={60} /> : (
              <ForecastChart history={chartHistory} level={level} showBrent months={60} run={{ id: "none", created_at: "", request_id: null, requested_by: "", status: "succeeded", horizon: 0,
                model_name: null, model_version: null, model_run_id: null, icp_dataset_version_id: null, wti_dataset_version_id: null,
                last_observed_period: ctx.latestPeriod, forecast_origin: ctx.latestPeriod, params: {}, result: null, error_code: null, error_message: null, duration_ms: null, export_available: false }} />
            )}
        </Panel>
        <div className="stack">
          <Panel title={t("overview.benchmarks")} icon={Database}>
            <div className="stack-sm">
              {(["brent", "dubai", "wti"] as const).map((b) => {
                const d = s?.datasets[b];
                const rel = s?.relationships[b];
                return (
                  <div key={b} className="row-between" style={{ borderBottom: "1px solid var(--border)", paddingBottom: 8 }}>
                    <div><strong>{b === "wti" ? "WTI" : b[0]!.toUpperCase() + b.slice(1)}</strong><div className="xs muted">{d?.period_end ? period(d.period_end, locale) : "-"}</div></div>
                    <div style={{ textAlign: "right" }}><strong>{d?.available ? fmtUsd(d.latest_value, intl) : "-"}</strong>
                      {rel && <div className="xs muted">{t("overview.spread", { b: b === "wti" ? "WTI" : b[0]!.toUpperCase() + b.slice(1) })}: {rel.spread_latest > 0 ? "+" : ""}{fmtNum(rel.spread_latest, intl)} · {t("overview.spreadMean", { n: rel.months })}: {fmtNum(rel.spread_mean, intl)}</div>}
                    </div>
                  </div>
                );
              })}
              <Link className="small link-inline" to="/harga">{t("nav.explorer")} <ArrowRight size={13} aria-hidden="true" /></Link>
            </div>
          </Panel>
          <Panel title={t("overview.freshness")} icon={ShieldCheck}>
            {fresh ? (
              <div className="stack-sm">
                <div className="row"><StatusBadge status={fresh.status} /><span className="small">{fresh.months_behind ? t("overview.freshnessBehind", { n: fresh.months_behind, period: period(fresh.expected_period, locale) }) : t("overview.freshnessOk")}</span></div>
                <div className="xs muted">{t("overview.publishedAt", { period: period(icp?.period_end, locale, true), date: dateTime(icp?.updated_at ?? null, intl) })}</div>
                <ul className="source-list">
                  {(["icp", "brent", "dubai", "wti", "usd_idr"] as const).map((k) => {
                    const d = s?.datasets[k];
                    const Mark = SOURCE_MARK[k];
                    return d?.available ? <li key={k}><span className="src-icon"><Mark size={15} /></span><span><strong>{d.label_id}</strong> · {t(`provider.${k}` as Key)} · {period(d.period_end, locale)}<span className="xs muted" style={{ display: "block" }}>{t(SOURCE_KIND[k])}</span></span></li> : null;
                  })}
                </ul>
              </div>
            ) : summary.loading ? <Skeleton height={80} /> : <p className="muted small">{t("common.dataUnavailable")}</p>}
          </Panel>
        </div>
      </div>

      <div className="grid split-main">
        <Panel title={t("overview.summaryTitle")} icon={Lightbulb} actions={<Link className="btn sm ghost" to="/insight">{t("overview.summaryMore")} <ArrowRight size={14} aria-hidden="true" /></Link>}>
          {insights.loading && !insights.data ? <Skeleton height={120} /> : insights.error ? <ErrorState error={insights.error} compact /> : insights.data === NOT_IN_PLAN ? (
            <Callout tone="info" title={t("ins.notInPlanTitle")} action={<Link className="btn sm" to="/akun#paket">{t("forecast.plansLink")}</Link>}>{t("ins.notInPlanBody")}</Callout>
          ) : topInsights.length ? (
            <div className="stack-sm">
              {topInsights.map((i) => <div key={i.id}><strong>{i.title}</strong><p className="small muted">{i.body}</p></div>)}
              <p className="xs muted">{insightData?.disclaimer}</p>
            </div>
          ) : <EmptyState title={t("ins.empty")}>{t("ins.emptyBody")}</EmptyState>}
        </Panel>
        <Panel title={t("overview.qualityTitle")} icon={ShieldCheck}>
          <div className="stack-sm">
            {icp?.missing_periods?.length ? <Callout tone="warn">{t("overview.qualityMissing", { n: icp.missing_periods.length, list: icp.missing_periods.map((p) => period(p, locale)).join(", ") })}</Callout> : null}
            {pendingMonths.length ? <Callout tone="info">{t("overview.qualityPending", { n: pendingMonths.length, list: pendingMonths.slice(0, 8).map((p) => period(p, locale)).join(", ") + (pendingMonths.length > 8 ? "…" : "") })}</Callout> : null}
            {!icp?.missing_periods?.length && !pendingMonths.length && icp && <Callout tone="good">{t("overview.qualityOk")}</Callout>}
          </div>
        </Panel>
      </div>

      <Panel title={t("overview.forecastTitle")} icon={ArrowRight}
        subtitle={latest.data?.result ? t("overview.forecastOrigin", { origin: period(latest.data.forecast_origin, locale), date: dateOnly(latest.data.issue_date ?? null, intl), method: methodLabel(latest.data.method, t) }) : undefined}
        actions={latest.data?.result ? <Link className="btn sm primary" to="/proyeksi">{t("overview.forecastOpen")}</Link> : undefined}>
        {latest.loading && latest.data === null ? <Skeleton height={80} /> : latest.error ? <ErrorState error={latest.error} onRetry={latest.reload} compact /> : latest.data?.result ? (
          <div className="reveal"><ForecastTable points={latest.data.result.points.slice(0, 6)} level="0.8" compact /></div>
        ) : <EmptyState illustration={<IllNoForecast size={150} />} title={t("overview.noForecast")}>{t("overview.noForecastBody")}</EmptyState>}
      </Panel>
    </div>
  );
}
