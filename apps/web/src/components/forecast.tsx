/**
 * Shared forecast presentation: chart with history + forecast segment + 80 % band, assumption
 * table with per-benchmark sources, point cards, method/accuracy panel in business language.
 * Internal identifiers (run ids, method ids, dataset version ids) never reach the screen; they
 * stay in the CSV export for traceability.
 */
import type { ForecastPoint, ForecastRun, HistoryPoint, ModelVersion } from "../api/types";
import { CircleHelp, Download } from "lucide-react";
import { useMemo } from "react";
import { Link } from "react-router-dom";
import { downloadUrl } from "../api/client";
import { dateOnly, fmtIdr, fmtNum, fmtUsd, monthRange, NA, period, rangeText, toCsv, downloadText } from "../format";
import { useT, type Key } from "../i18n";
import { SERIES_COLORS, TimeSeriesChart, type Zone } from "./charts";
import { IllIcpProcess } from "./illustrations";
import { Badge, Button, Callout, KindChip, Panel, Segmented, Stat } from "./ui";
import { MetricBars } from "./charts";

export type Level = "0.5" | "0.8" | "0.95";
const BENCH = ["brent", "dubai", "wti"] as const;
const DATASET_KEYS = new Set(["icp", "brent", "dubai", "wti", "usd_idr", "usd_idr_eom"]);

export function methodPlain(family: string | null | undefined, t: ReturnType<typeof useT>["t"]): string {
  const f = family ?? "";
  if (f === "spread_rule") return t("forecast.methodPlain.spread_rule");
  if (f === "naive") return t("forecast.methodPlain.naive");
  if (f === "blend") return t("forecast.methodPlain.blend");
  if (f === "ridge") return t("forecast.methodPlain.ridge");
  if (f === "linear") return t("forecast.methodPlain.linear");
  if (f === "gradient_boosting" || f === "random_forest") return t("forecast.methodPlain.tree");
  return t("forecast.methodPlain.other");
}

/** Business-facing name for a method id. The id itself is never rendered (it stays in exports). */
export function methodLabel(name: string | null | undefined, t: ReturnType<typeof useT>["t"]): string {
  const n = name ?? "";
  const benchName = (b: string) => (b === "wti" ? "WTI" : b.charAt(0).toUpperCase() + b.slice(1));
  const spread = /^spread_(wti|brent|dubai)_(\d+)m$/.exec(n);
  if (spread) return t("forecast.methodName.spread", { bench: benchName(spread[1] ?? ""), window: spread[2] ?? "" });
  const spreadAll = /^spread_(wti|brent|dubai)_all$/.exec(n);
  if (spreadAll) return t("forecast.methodName.spreadAll", { bench: benchName(spreadAll[1] ?? "") });
  if (n.startsWith("blend")) return t("forecast.methodName.blend");
  if (n.startsWith("naive")) return t("forecast.methodName.naive");
  if (n.startsWith("ridge") || n.startsWith("linear")) return t("forecast.methodName.ridge");
  if (n.startsWith("gbm") || n.startsWith("gradient") || n.startsWith("random_forest")) return t("forecast.methodName.tree");
  if (n === "legacy") return t("forecast.methodName.legacy");
  return n ? t("forecast.methodName.other") : NA;
}

/** Readable list of the datasets a forecast used (labels only, never version ids). */
export function datasetList(versions: Record<string, string> | null | undefined, t: ReturnType<typeof useT>["t"]): string {
  if (!versions) return "";
  return Object.keys(versions).filter((k) => DATASET_KEYS.has(k)).map((k) => t(`ds.${k}` as Key)).join(", ");
}

/** Chart rows: official history up to the origin, then forecast points with the chosen band. */
export function buildForecastRows(history: HistoryPoint[], run: ForecastRun, level: Level, months = 30): { rows: Array<Record<string, unknown>>; zones: Zone[] } {
  const points = run.result?.points ?? [];
  const origin = run.forecast_origin ?? run.last_observed_period ?? "";
  const actual = history.filter((p) => !origin || p.period <= origin).slice(-months);
  const byPeriod = new Map<string, Record<string, unknown>>();
  for (const p of actual) byPeriod.set(p.period, { period: p.period, actual: p.icp_price ?? undefined, brent: p.brent_price ?? undefined });
  const originRow = byPeriod.get(origin);
  if (originRow && typeof originRow.actual === "number") originRow.forecast = originRow.actual;
  for (const p of points) {
    const iv = p.intervals[level];
    byPeriod.set(p.period, { period: p.period, forecast: p.prediction, band: iv ? [iv[0], iv[1]] : undefined, kind: p.kind, brent: p.benchmarks.brent?.value ?? undefined });
  }
  const periods = [...byPeriod.keys()].sort();
  const rows = periods.length ? monthRange(periods[0]!, periods.at(-1)!).map((p) => byPeriod.get(p) ?? { period: p }) : [];
  const zones: Zone[] = points.length ? [{ from: points[0]!.period, to: points.at(-1)!.period, kind: "forecast" }] : [];
  return { rows, zones };
}

export function ForecastChart({ history, run, level, height = "md", showBrent }: { history: HistoryPoint[]; run: ForecastRun; level: Level; height?: "sm" | "md" | "lg"; showBrent?: boolean }) {
  const { t, locale } = useT();
  const { rows, zones } = useMemo(() => buildForecastRows(history, run, level), [history, run, level]);
  const series = [
    { key: "actual", label: t("forecast.chartActual"), color: SERIES_COLORS.icp, width: 2.25 },
    ...(showBrent ? [{ key: "brent", label: "Brent", color: SERIES_COLORS.brent, width: 1.5 }] : []),
    { key: "forecast", label: t("forecast.chartForecast"), color: SERIES_COLORS.forecast, dashed: true },
  ];
  return (
    <TimeSeriesChart ariaLabel={t("forecast.title")} data={rows} height={height} zones={zones} series={series}
      band={{ key: "band", label: t("forecast.range", { level: `${Math.round(Number(level) * 100)} %` }) }}
      marker={run.forecast_origin ? { at: run.forecast_origin, label: period(run.forecast_origin, locale) } : undefined}
      tooltipExtra={(row) => row.kind ? <div className="t-note">{t(`kind.${String(row.kind)}` as Key)}</div> : null} />
  );
}

export function LevelPicker({ level, onChange }: { level: Level; onChange: (l: Level) => void }) {
  const { t } = useT();
  return <Segmented label={t("forecast.intervals")} value={level} onChange={onChange}
    options={[{ value: "0.5", label: "50 %" }, { value: "0.8", label: "80 %" }, { value: "0.95", label: "95 %" }]} />;
}

export function PointCards({ points, level }: { points: ForecastPoint[]; level: Level }) {
  const { t, locale, intl } = useT();
  return (
    <div className="points-grid">
      {points.map((p) => {
        const iv = p.intervals[level];
        return (
          <div className="point-card" key={p.period}>
            <div className="row-between"><strong>{period(p.period, locale, true)}</strong><span className="xs muted">{t("forecast.step")} {p.step}</span></div>
            <KindChip kind={p.kind} />
            <div className="pc-val">{fmtUsd(p.prediction, intl)}<span className="xs muted" style={{ marginLeft: 6 }}>/bbl</span></div>
            {iv && <div className="small muted">{t("forecast.range", { level: `${Math.round(Number(level) * 100)} %` })}: {rangeText(fmtNum(iv[0], intl), fmtNum(iv[1], intl), locale)}</div>}
            {p.prediction_idr != null && <div className="small">{t("forecast.idr")}: <strong>{fmtIdr(p.prediction_idr, intl)}</strong> <span className="xs muted">({p.fx_source ? t(`fx.${p.fx_source}` as Key) : NA})</span></div>}
            {p.conditional_on_scenario && <Badge tone="brand">{t("forecast.conditional")}</Badge>}
          </div>
        );
      })}
    </div>
  );
}

export function AssumptionTable({ points }: { points: ForecastPoint[] }) {
  const { t, locale, intl } = useT();
  return (
    <div className="table-scroll">
      <table className="table">
        <thead><tr><th>{t("common.month")}</th><th>{t("forecast.kindLegend")}</th>
          {BENCH.map((b) => <th key={b} className="num">{b === "wti" ? "WTI" : b[0]!.toUpperCase() + b.slice(1)}</th>)}
          <th className="num hide-sm">{t("overview.fx")}</th></tr></thead>
        <tbody>{points.map((p) => (
          <tr key={p.period}>
            <td className="primary-cell nowrap">{period(p.period, locale)}</td>
            <td><KindChip kind={p.kind} /></td>
            {BENCH.map((b) => {
              const bi = p.benchmarks[b];
              if (!bi) return <td key={b} className="num muted">{NA}</td>;
              return (
                <td key={b} className="num">
                  <span className={bi.used_by_model ? "strong" : "muted"}>{bi.value == null ? NA : fmtNum(bi.value, intl)}</span>
                  <span className="sub">{t(`src.${bi.source}` as Key)}{bi.used_by_model ? "" : ` · ${t("forecast.notUsed")}`}</span>
                </td>
              );
            })}
            <td className="num hide-sm">{p.fx_rate ? fmtNum(p.fx_rate, intl, 0) : NA}<span className="sub">{p.fx_source ? t(`fx.${p.fx_source}` as Key) : ""}</span></td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
}

/** CSV export keeps the technical identifiers (forecast id, method id) for traceability. */
export function exportForecastCsv(run: ForecastRun): void {
  const pts = run.result?.points ?? [];
  const rows = pts.map((p) => [run.id, run.issue_date ?? "", run.forecast_origin ?? "", run.method ?? "", p.period, p.step, p.kind, p.prediction,
    p.intervals["0.8"]?.[0] ?? null, p.intervals["0.8"]?.[1] ?? null, p.benchmarks.brent?.value ?? null, p.benchmarks.brent?.source ?? "",
    p.benchmarks.dubai?.value ?? null, p.benchmarks.dubai?.source ?? "", p.benchmarks.wti?.value ?? null, p.benchmarks.wti?.source ?? "",
    p.fx_rate ?? null, p.fx_source ?? "", p.prediction_idr ?? null]);
  const stamp = `${run.issue_date ?? run.created_at.slice(0, 10)}_${run.forecast_origin ?? "origin"}`;
  downloadText(`icp-proyeksi_${stamp}.csv`, toCsv(["forecast_id", "issue_date", "forecast_origin", "method", "period", "step", "kind", "prediction_usd",
    "p80_low", "p80_high", "brent_usd", "brent_source", "dubai_usd", "dubai_source", "wti_usd", "wti_source", "usd_idr", "usd_idr_source", "prediction_idr"], rows));
}

/** Business-language description of the active method and its backtest error. */
export function MethodPanel({ prod }: { prod: ModelVersion | null | undefined }) {
  const { t, intl } = useT();
  if (!prod) return null;
  const m = prod.metrics;
  const usd = (v: number | null | undefined) => fmtUsd(v, intl);
  return (
    <Panel title={t("forecast.methodTitle")} icon={CircleHelp} subtitle={<>{methodLabel(prod.method, t)} · {prod.rule_based ? t("forecast.ruleBased") : t("forecast.statistical")} · {t("forecast.modelVersion", { v: prod.version })}</>}>
      <div className="grid cols-2" style={{ alignItems: "start" }}>
        <div className="stack-sm">
          <p>{prod.description || methodPlain(prod.method_family, t)}</p>
          <div className="section-title" style={{ marginTop: 6 }}>{t("forecast.accuracyTitle")}</div>
          <p className="small">{t("forecast.accuracyBody", { mae: usd(m.mae), naive: usd(m.baseline_naive_last_mae) })}</p>
          {m.hold_last_h1_mae != null && <p className="small muted">{t("forecast.accuracyHold", { mae: usd(m.hold_last_h1_mae), naive: usd(m.baseline_naive_hold_last_h1_mae) })}</p>}
          <p className="xs muted">{t("forecast.evalPeriod", { period: prod.backtest_origins ?? NA, fit: prod.fit_period?.split("..").at(-1) ?? NA })}</p>
        </div>
        <div className="stack-sm">
          <div className="section-title">{t("forecast.baselineTitle")}</div>
          <div className="xs muted">{t("forecast.baselineObserved")}</div>
          <MetricBars unit="" rows={[
            { label: <strong>{t("forecast.baselineActive")}</strong>, value: m.mae, kind: "model" },
            { label: t("forecast.baselineNaive"), value: m.baseline_naive_last_mae, kind: "baseline" },
          ]} />
          {m.hold_last_h1_mae != null && (<>
            <div className="xs muted" style={{ marginTop: 6 }}>{t("forecast.baselineHold")}</div>
            <MetricBars unit="" rows={[
              { label: <strong>{t("forecast.baselineActive")}</strong>, value: m.hold_last_h1_mae, kind: "model" },
              { label: t("forecast.baselineNaive"), value: m.baseline_naive_hold_last_h1_mae, kind: "baseline" },
            ]} />
          </>)}
          <div className="xs muted">USD/bbl · {t("forecast.accuracyTitle").toLowerCase()}</div>
        </div>
      </div>
    </Panel>
  );
}

/** Three-step explainer of how an official ICP comes about (illustration + text). */
export function ProcessExplainer() {
  const { t } = useT();
  return (
    <div className="process">
      <div className="process-ill"><IllIcpProcess size={220} /></div>
      <ol className="process-steps">
        <li>{t("forecast.process1")}</li>
        <li>{t("forecast.process2")}</li>
        <li>{t("forecast.process3")}</li>
      </ol>
    </div>
  );
}

/** Complete view of one forecast run (used by the forecast page and scenario runs). */
export function ForecastRunView({ run, history, level, onLevel, prod }: { run: ForecastRun; history: HistoryPoint[]; level: Level; onLevel: (l: Level) => void; prod?: ModelVersion | null }) {
  const { t, locale, intl } = useT();
  const res = run.result;
  if (!res) return <Callout tone="bad" title={run.error_code === "MODEL_UNAVAILABLE" ? t("error.modelUnavailable") : t("error.generic")}>{t("forecast.runFailed")}</Callout>;
  const legacy = res.points.some((p) => p.kind === "legacy_record");
  const datasets = datasetList(run.dataset_versions, t);
  return (
    <div className="stack">
      <div className="stat-grid">
        <Stat label={t("forecast.origin")} value={period(res.forecast_origin, locale)} />
        <Stat label={t("forecast.issueDate")} value={dateOnly(res.issue_date, intl)} />
        <Stat label={t("forecast.horizon")} value={t("forecast.horizonMonths", { n: res.horizon })} hint={rangeText(period(res.points[0]?.period, locale), period(res.points.at(-1)?.period, locale), locale)} />
        <Stat label={t("common.method")} value={methodLabel(res.method, t)} hint={run.model_version ? t("forecast.modelVersion", { v: run.model_version }) : undefined} />
      </div>
      {legacy && <Callout tone="warn">{t("forecast.legacyNote")}</Callout>}
      {res.warnings.length > 0 && <Callout tone="info" title={t("forecast.warnings")}>{res.warnings.join(" ")}</Callout>}
      <div className="row-between">
        <div className="kind-legend">
          <span className="row" style={{ gap: 6 }} title={t("kind.estimate.help")}><KindChip kind="estimate_awaiting_publication" /><span className="xs muted hide-sm">{t("kind.estimate.short")}</span></span>
          <span className="row" style={{ gap: 6 }} title={t("kind.forecast.help")}><KindChip kind="future_forecast" /><span className="xs muted hide-sm">{t("kind.forecast.short")}</span></span>
        </div>
        <div className="row"><LevelPicker level={level} onChange={onLevel} />
          {run.export_available
            ? <a className="btn sm" href={downloadUrl(`/forecasts/${run.id}/export.csv`)} download><Download size={16} aria-hidden="true" />{t("forecast.exportCsv")}</a>
            : <Button size="sm" icon={Download} onClick={() => exportForecastCsv(run)}>{t("forecast.exportCsv")}</Button>}
        </div>
      </div>
      <ForecastChart history={history} run={run} level={level} />
      <PointCards points={res.points} level={level} />
      <div>
        <div className="section-title">{t("forecast.assumptionsTitle")}</div>
        <p className="small muted" style={{ marginBottom: 8 }}>{t("forecast.assumptionsSub")}</p>
        <div style={{ border: "1px solid var(--border)", borderRadius: "var(--r-card)", overflow: "hidden" }}><AssumptionTable points={res.points} /></div>
      </div>
      <p className="xs muted">{t("forecast.intervalNote")} {datasets && t("forecast.dataVersionsNote", { list: datasets })}</p>
      {prod && <MethodPanel prod={prod} />}
      {!prod && <p className="small"><Link to="/proyeksi">{t("forecast.methodTitle")}</Link></p>}
    </div>
  );
}
