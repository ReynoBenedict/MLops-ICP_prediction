/**
 * Shared forecast presentation: chart with history + forecast segment + band, a compact forecast
 * table, the assumption table and the method/accuracy panel, all in business language.
 *
 * Honesty rules:
 * - Benchmark values that are assumed (held at the last value, or from a scenario) are drawn as a
 *   separate thin dotted "asumsi" series, never as a continuation of the observed line.
 * - A flat hold-last forecast is explained in one sentence next to it.
 * - Scenario-conditional ranges are labelled "jika asumsi tepat".
 * - Accuracy shown first is the one for the situation of the displayed points (horizon + mode),
 *   from the backend `evaluation` block when present, else from the model's stored by-horizon
 *   backtest metrics; nothing is computed here beyond lookup and formatting.
 * - Months that became official since the issue show the official value and the difference.
 * Internal identifiers (run ids, method ids, dataset version ids) never reach the screen.
 */
import type { ForecastPoint, ForecastRun, HistoryPoint, ModelVersion } from "../api/types";
import { ArrowRight, CircleHelp, Download, FileText, Info, SlidersHorizontal, TriangleAlert } from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { downloadWithAuth } from "../api/client";
import { loc, type PointEvaluation, type PointReadFields, type ResultReadFields, type RunPlanClip } from "../api/product-types";
import { dateOnly, fmtIdr, fmtNum, fmtSigned, fmtUsd, fmtUsdRange, localizePeriods, monthRange, NA, period, rangeText, toCsv, downloadText } from "../format";
import { useT, type Key } from "../i18n";
import { SERIES_COLORS, TimeSeriesChart, type SeriesSpec, type Zone } from "./charts";
import { Badge, Button, Callout, ErrorState, KindChip, Panel, Segmented, Stat, TableScroll, useMediaQuery, useTableFrame } from "./ui";
import { MetricBars } from "./charts";

export type Level = "0.5" | "0.8" | "0.95";
const BENCH = ["brent", "dubai", "wti"] as const;
const DATASET_KEYS = new Set(["icp", "brent", "dubai", "wti", "usd_idr", "usd_idr_eom"]);
const benchName = (b: string) => (b === "wti" ? "WTI" : b.charAt(0).toUpperCase() + b.slice(1));
/** Widths (px) below which the full forecast and assumption tables fold the value-kind column under the month. */
const FOLD_BELOW = 820;
const FOLD_BELOW_ASSUMPTIONS = 860;

/** "80%": percentages are written without a space in both languages, like the server's insight text. */
const lvlText = (level: Level) => `${Math.round(Number(level) * 100)}%`;

type P = ForecastPoint & PointReadFields;

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

/** The official ICP for a forecast month, from the backend `actual` field or the official history. */
export function officialFor(p: P, history: HistoryPoint[]): number | null {
  if (p.actual && typeof p.actual.value === "number") return p.actual.value;
  const h = history.find((x) => x.period === p.period);
  return typeof h?.icp_price === "number" ? h.icp_price : null;
}

/**
 * Chart rows: official history (up to the origin, and after it when official values have been
 * published since), the forecast with the chosen band, observed Brent (solid) and assumed Brent
 * (separate dotted series starting at the last observed value).
 */
export function buildForecastRows(history: HistoryPoint[], run: ForecastRun, level: Level, months = 30): { rows: Array<Record<string, unknown>>; zones: Zone[] } {
  const points = (run.result?.points ?? []) as P[];
  const origin = run.forecast_origin ?? run.last_observed_period ?? "";
  const lastPoint = points.at(-1)?.period ?? origin;
  const actual = history.filter((p) => !origin || p.period <= origin).slice(-months);
  const byPeriod = new Map<string, Record<string, unknown>>();
  for (const p of actual) byPeriod.set(p.period, { period: p.period, actual: p.icp_price ?? undefined, brent: p.brent_price ?? undefined });
  const originRow = byPeriod.get(origin);
  if (originRow && typeof originRow.actual === "number") originRow.forecast = originRow.actual;
  let assumedStarted = false;
  for (const p of points) {
    const iv = p.intervals[level];
    const b = p.benchmarks.brent;
    const row: Record<string, unknown> = { period: p.period, forecast: p.prediction, band: iv ? [iv[0], iv[1]] : undefined, kind: p.kind, conditional: p.conditional_on_scenario };
    if (b?.value != null) {
      if (b.source === "observed") row.brent = b.value;
      else {
        row.brent_assumed = b.value;
        if (!assumedStarted && originRow && typeof originRow.brent === "number") originRow.brent_assumed = originRow.brent;
        // Continuity from the last observed Brent month inside the window.
        if (!assumedStarted) { const prev = byPeriod.get(points[points.indexOf(p) - 1]?.period ?? ""); if (prev && typeof prev.brent === "number") prev.brent_assumed = prev.brent; }
        assumedStarted = true;
      }
    }
    const official = officialFor(p, history);
    if (official != null) row.actual = official;
    byPeriod.set(p.period, row);
  }
  // Official values published after the window's start keep the actual line continuous.
  for (const h of history) if (h.period > origin && h.period <= lastPoint && !byPeriod.has(h.period) && h.icp_price != null) byPeriod.set(h.period, { period: h.period, actual: h.icp_price });
  const periods = [...byPeriod.keys()].sort();
  const rows = periods.length ? monthRange(periods[0]!, periods.at(-1)!).map((p) => byPeriod.get(p) ?? { period: p }) : [];
  const zones: Zone[] = points.length ? [{ from: points[0]!.period, to: points.at(-1)!.period, kind: "forecast" }] : [];
  return { rows, zones };
}

/** A run with no forecast points: the chart then shows official history only, up to `origin`. */
export function historyOnlyRun(origin: string | null): ForecastRun {
  return {
    id: "none", created_at: "", request_id: null, requested_by: "", status: "succeeded", horizon: 0, model_name: null, model_version: null,
    model_run_id: null, icp_dataset_version_id: null, wti_dataset_version_id: null, last_observed_period: origin, forecast_origin: origin,
    params: {}, result: null, error_code: null, error_message: null, duration_ms: null, export_available: false,
  };
}

/**
 * Official ICP history with the forecast segment (dashed ICP hue), its band and the forecast zone.
 * `run` may be null (no forecast published yet): the chart then shows history up to `origin`.
 * Without `months`, it shows 30 months, or 12 on phones (560 px and below), where a longer window
 * would leave the forecast months a few pixels wide.
 */
export function ForecastChart({ history, run, level, height = "md", showBrent, months, origin = null }:
  { history: HistoryPoint[]; run: ForecastRun | null; level: Level; height?: "sm" | "md" | "lg"; showBrent?: boolean; months?: number; origin?: string | null }) {
  const phone = useMediaQuery("(max-width: 560px)");
  const shown = useMemo(() => run ?? historyOnlyRun(origin), [run, origin]);
  return <ForecastChartInner history={history} run={shown} level={level} height={height} showBrent={showBrent} months={months ?? (phone ? 12 : 30)} />;
}

function ForecastChartInner({ history, run, level, height, showBrent, months }: { history: HistoryPoint[]; run: ForecastRun; level: Level; height: "sm" | "md" | "lg"; showBrent?: boolean; months: number }) {
  const { t, locale, intl } = useT();
  const { rows, zones } = useMemo(() => buildForecastRows(history, run, level, months), [history, run, level, months]);
  const hasAssumed = rows.some((r) => typeof r.brent_assumed === "number");
  const conditional = rows.some((r) => r.conditional === true);
  const series: SeriesSpec[] = [
    { key: "actual", label: t("forecast.chartActual"), color: SERIES_COLORS.icp, width: 2.25 },
    ...(showBrent ? [{ key: "brent", label: hasAssumed ? t("forecast.chartBrentObserved") : "Brent", color: SERIES_COLORS.brent, width: 1.5 }] : []),
    ...(showBrent && hasAssumed ? [{ key: "brent_assumed", label: t("forecast.chartBrentAssumed"), color: SERIES_COLORS.brent, assumed: true }] : []),
    { key: "forecast", label: t("forecast.chartForecast"), color: SERIES_COLORS.forecast, dashed: true },
  ];
  const first = (run.result?.points ?? [])[0];
  const lastOfficial = [...history].reverse().find((h) => h.icp_price != null && (!run.forecast_origin || h.period <= run.forecast_origin));
  const iv = first?.intervals[level];
  const summary = first && lastOfficial && iv ? t("forecast.summary", {
    v: fmtUsd(lastOfficial.icp_price, intl), p: period(lastOfficial.period, locale), kind: t(`kind.${first.kind}` as Key).toLowerCase(),
    p1: period(first.period, locale), f1: fmtUsd(first.prediction, intl), level: lvlText(level), lo: fmtNum(iv[0], intl), hi: fmtNum(iv[1], intl),
  }) : undefined;
  return (
    <TimeSeriesChart ariaLabel={t("forecast.chartLabel")} data={rows} height={height} zones={zones} series={series} summary={summary}
      band={{ key: "band", label: `${t("forecast.range", { level: lvlText(level) })}${conditional ? ` (${t("forecast.rangeIfRight")})` : ""}` }}
      marker={run.forecast_origin ? { at: run.forecast_origin, label: t("forecast.lastOfficialMarker", { period: period(run.forecast_origin, locale).replace(" ", "\u00a0") }) } : undefined}
      tooltipExtra={(row) => row.kind ? <div className="t-note">{t(`kind.${String(row.kind)}` as Key)}{row.conditional ? ` · ${t("forecast.conditional")}` : ""}</div> : null} />
  );
}

export function LevelPicker({ level, onChange }: { level: Level; onChange: (l: Level) => void }) {
  const { t } = useT();
  return <Segmented label={t("forecast.intervals")} value={level} onChange={onChange}
    options={[{ value: "0.5", label: "50%" }, { value: "0.8", label: "80%" }, { value: "0.95", label: "95%" }]} />;
}

/** "Brent held at USD 114,16 (Sep 2026) for n months" + scenario note, with links to test it. */
export function AssumptionNote({ points, history, runId, compact }: { points: ForecastPoint[]; history: HistoryPoint[]; runId?: string; compact?: boolean }) {
  const { t, locale, intl } = useT();
  const held = points.filter((p) => p.benchmarks.brent?.source === "assumed_hold_last" && p.benchmarks.brent?.value != null);
  const scen = points.some((p) => p.conditional_on_scenario);
  if (!held.length && !scen) return null;
  const v = held[0]?.benchmarks.brent?.value ?? null;
  // The month whose observed Brent is held: the latest observed month in the history when it carries
  // that value, else a month of this run with an observed Brent of that value (an estimate month).
  // When neither establishes it (history unavailable), no month is named rather than a wrong one.
  const lastBrent = [...history].reverse().find((h) => h.brent_price != null);
  const observedRun = [...points].reverse().find((p) => p.benchmarks.brent?.source === "observed" && p.benchmarks.brent?.value === v);
  const heldFrom = lastBrent && lastBrent.brent_price === v ? lastBrent.period : observedRun?.period ?? null;
  const holdKey = heldFrom ? (scen ? "forecast.assumeHoldMixed" : "forecast.assumeHold") : (scen ? "forecast.assumeHoldMixedNoPeriod" : "forecast.assumeHoldNoPeriod");
  return (
    <div className="callout info" role="note">
      <Info size={18} aria-hidden="true" />
      <div className="grow stack-sm">
        <div className="callout-title">{t("forecast.assumeHoldTitle")}</div>
        {/* With scenario months the plotted path moves, so the "almost flat" sentence would be false (COPY-04). */}
        {held.length > 0 && <p>{t(holdKey, { v: fmtNum(v, intl), period: heldFrom ? period(heldFrom, locale) : "", n: held.length })}</p>}
        {scen && <p>{t("forecast.assumeScenario")}</p>}
        <div className="row">
          <Link className="btn sm" to="/skenario"><SlidersHorizontal size={14} aria-hidden="true" />{t("forecast.testAssumption")}</Link>
          {!compact && runId && <Link className="btn sm ghost" to={`/laporan?jenis=proyeksi&proyeksi=${encodeURIComponent(runId)}`}><FileText size={14} aria-hidden="true" />{t("forecast.reportThis")}</Link>}
        </div>
      </div>
    </div>
  );
}

/**
 * Compact forecast table (month, kind, central value, range, rupiah). At ≤ 560 px every row is a
 * record card with the central value first. Months that are official by now show the official
 * value and the difference to the estimate. In a container narrower than FOLD_BELOW (the forecast
 * page between 901 and about 1200 px, a scenario results pane, phones) the value-kind column folds
 * under the month, so the rupiah column is never cut off without a cue (B-P3-4).
 */
export function ForecastTable({ points, level, compact, history = [] }: { points: ForecastPoint[]; level: Level; compact?: boolean; history?: HistoryPoint[] }) {
  const { t, locale, intl } = useT();
  const lvl = lvlText(level);
  const [tableRef, { narrow }] = useTableFrame<HTMLTableElement>(FOLD_BELOW);
  const folded = !compact && narrow;
  const kindColumn = !compact && !narrow;
  return (
    <TableScroll label={t("forecast.resultTable")} className="forecast-table-wrap">
      <table ref={tableRef} className="table forecast-table rc">
        <caption className="sr-only">{t("forecast.resultTitle")} (USD/bbl)</caption>
        <thead><tr>
          <th>{t("common.month")}</th>{kindColumn && <th>{t("forecast.kindLegend")}</th>}
          <th className="num">{t("forecast.colPoint")} <span className="th-unit">USD/bbl</span></th>
          <th className="num">{t("forecast.range", { level: lvl })} <span className="th-unit">USD/bbl</span></th>
          {!compact && <th className="num">{t("forecast.idr")}</th>}
        </tr></thead>
        <tbody>{(points as P[]).map((p) => {
          const iv = p.intervals[level];
          const official = officialFor(p, history);
          const assumed = p.benchmarks.brent?.source === "assumed_hold_last";
          const chips = (
            <span className="chip-row">
              {official != null ? <Badge tone="outline">{t("kind.official_published")}</Badge> : <KindChip kind={p.kind} />}
              {p.conditional_on_scenario && <Badge tone="brand">{t("forecast.conditional")}</Badge>}
              {assumed && official == null && !compact && <span className="tag-assumed">{t("forecast.assumedBadge")}</span>}
            </span>
          );
          const officialNote = official != null ? <span className="sub">{t("forecast.officialNote", { kind: t(`kind.${p.kind}` as Key) })}</span> : null;
          return (
            <tr key={p.period}>
              {/* Compact tables (overview) and narrow containers carry the value kind under the month
                  instead of in its own column. */}
              <td className="primary-cell nowrap rc-title">{period(p.period, locale, true)}
                {compact ? <span className="sub kind-sub">{chips}</span> : folded ? <span className="sub kind-sub">{chips}{officialNote}</span> : null}</td>
              {kindColumn && <td data-label={t("forecast.kindLegend")}>{chips}{officialNote}</td>}
              <td className="num strong nowrap rc-main" data-label={`${t("forecast.colPoint")} (USD/bbl)`}><span>{fmtUsd(p.prediction, intl)}</span>
                {official != null && <span className="sub">{t("forecast.officialNow", { v: fmtUsd(official, intl) })} · {t("forecast.officialDiff", { d: fmtSigned(p.prediction - official, intl) })}</span>}</td>
              <td className="num nowrap" data-label={t("forecast.range", { level: lvl })}>{iv ? fmtUsdRange(iv[0], iv[1], intl, locale) : NA}
                {iv && p.conditional_on_scenario && <span className="sub">{t("forecast.rangeIfRight")}</span>}</td>
              {!compact && <td className="num nowrap" data-label={t("forecast.idr")}>{p.prediction_idr != null ? <><span className="strong">{fmtIdr(p.prediction_idr, intl)}</span><span className="sub">{p.fx_source ? t(`fx.${p.fx_source}` as Key) : NA}</span></> : NA}</td>}
            </tr>
          );
        })}</tbody>
      </table>
    </TableScroll>
  );
}

/**
 * Benchmark assumptions per month. A note that is the same in every cell of a column ("held at
 * last value", "not used") is shown once in the column header instead of in every cell. In a
 * narrow container the value kind folds under the month (as in ForecastTable).
 */
export function AssumptionTable({ points }: { points: ForecastPoint[] }) {
  const { t, locale, intl } = useT();
  const [tableRef, { narrow }] = useTableFrame<HTMLTableElement>(FOLD_BELOW_ASSUMPTIONS);
  const cols = BENCH.map((b) => {
    const cells = points.map((p) => p.benchmarks[b]).filter(Boolean);
    const used = cells.some((c) => c?.used_by_model);
    const sources = new Set(cells.map((c) => c?.source));
    return { b, used, sameSource: sources.size === 1 ? [...sources][0] ?? null : null };
  });
  const fxSources = new Set(points.map((p) => p.fx_source).filter(Boolean));
  const fxSame = fxSources.size === 1 ? [...fxSources][0]! : null;
  return (
    <TableScroll label={t("forecast.assumptionsTitle")}>
      <table ref={tableRef} className="table rc">
        <caption className="sr-only">{t("forecast.assumptionsTitle")} (USD/bbl)</caption>
        <thead><tr><th>{t("common.month")}</th>{!narrow && <th>{t("forecast.kindLegend")}</th>}
          {cols.map(({ b, used, sameSource }) => (
            <th key={b} className="num">{benchName(b)} <span className="th-unit">USD/bbl</span>
              <span className="sub th-note">{used ? t("forecast.usedByModel") : t("forecast.colNotUsed")}{sameSource ? ` · ${t(`src.${sameSource}` as Key)}` : ""}</span></th>
          ))}
          <th className="num">{t("overview.fx")}<span className="sub th-note">{fxSame ? t(`fx.${fxSame}` as Key) : ""}</span></th></tr></thead>
        <tbody>{points.map((p) => (
          <tr key={p.period}>
            <td className="primary-cell nowrap rc-title">{period(p.period, locale)}{narrow && <span className="sub kind-sub"><KindChip kind={p.kind} /></span>}</td>
            {!narrow && <td data-label={t("forecast.kindLegend")}><KindChip kind={p.kind} /></td>}
            {cols.map(({ b, used, sameSource }, i) => {
              const bi = p.benchmarks[b];
              const label = `${benchName(b)} (${used ? t("forecast.usedByModel") : t("forecast.colNotUsed")})`;
              if (!bi) return <td key={b} className="num muted" data-label={label}>{NA}</td>;
              return (
                <td key={b} className={`num${i === 0 && used ? " rc-main" : ""}`} data-label={sameSource ? `${label} · ${t(`src.${sameSource}` as Key)}` : label}>
                  <span className={used ? "strong" : "muted"}>{bi.value == null ? NA : fmtNum(bi.value, intl)}</span>
                  {!sameSource && <span className="sub">{t(`src.${bi.source}` as Key)}</span>}
                </td>
              );
            })}
            <td className="num" data-label={t("overview.fx")}>{p.fx_rate ? fmtNum(p.fx_rate, intl, 0) : NA}{!fxSame && p.fx_source && <span className="sub">{t(`fx.${p.fx_source}` as Key)}</span>}</td>
          </tr>
        ))}</tbody>
      </table>
    </TableScroll>
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

/** Server CSV export (sent with the session credential) or a client-side CSV when no export exists. */
export function ExportButton({ run }: { run: ForecastRun }) {
  const { t } = useT();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const stamp = `${run.issue_date ?? run.created_at.slice(0, 10)}_${run.forecast_origin ?? "origin"}`;
  const onClick = async () => {
    if (!run.export_available) { exportForecastCsv(run); return; }
    setBusy(true); setError(null);
    try { await downloadWithAuth(`/forecasts/${run.id}/export.csv`, undefined, `icp-proyeksi_${stamp}.csv`); }
    catch (e) { setError(e); }
    finally { setBusy(false); }
  };
  return (
    <>
      <Button size="sm" icon={Download} loading={busy} onClick={() => void onClick()}>{t("forecast.exportCsv")}</Button>
      {error ? <div className="full-row"><ErrorState error={error} compact /></div> : null}
    </>
  );
}

/** "2020-12..2026-08" -> number of months tested and the bounds. */
function evalWindow(origins: string | null | undefined): { n: number; from: string; to: string } | null {
  const m = /(\d{4}-\d{2})\.\.(\d{4}-\d{2})/.exec(origins ?? "");
  if (!m || !m[1] || !m[2]) return null;
  return { n: monthRange(m[1], m[2]).length, from: m[1], to: m[2] };
}

/** Backtest accuracy for one point: backend `evaluation` first, else the model's stored by-horizon metrics. */
export function accuracyFor(p: ForecastPoint & PointReadFields, prod: ModelVersion | null | undefined): PointEvaluation | null {
  if (p.evaluation) return p.evaluation;
  const mode = (p.interval_basis ?? (p.kind === "estimate_awaiting_publication" ? "observed" : "hold_last")) as "observed" | "hold_last";
  const h = prod?.metrics.by_horizon?.find((x) => x.mode === mode && x.horizon === p.step);
  if (!h) return null;
  return { mode, horizon: p.step, n: h.n, mae: h.mae ?? null, mase: h.mase ?? null, dir_acc: h.dir_acc ?? null, coverage: null, coverage_n: null };
}

/** Accuracy for exactly the situation of the points on screen (per horizon and mode). */
export function PointAccuracy({ points, prod, level }: { points: ForecastPoint[]; prod: ModelVersion | null | undefined; level: Level }) {
  const { t, intl, locale } = useT();
  const rows = (points as P[]).map((p) => ({ p, ev: accuracyFor(p, prod) }));
  const pct = (v: number) => `${fmtNum(v * 100, intl, 0)}%`;
  return (
    <div className="stack-sm">
      <div className="section-title flush">{t("forecast.accFor")}</div>
      <p className="xs muted">{t("forecast.accForSub")}</p>
      <ul className="acc-list">
        {rows.map(({ p, ev }) => (
          <li key={p.period}>
            <strong>{period(p.period, locale)}</strong>
            {ev && loc(ev.summary, locale) ? <AccuracyDisclosure ev={ev} level={level} /> : ev ? <span>{t("forecast.accRow", { h: ev.horizon, mode: t(`forecast.mode.${ev.mode}` as Key) })}: {t("forecast.accMae", { mae: fmtUsd(ev.mae, intl) })}
              {ev.coverage?.[level] != null && <> · {t("forecast.accCoverage", { level: lvlText(level), pct: pct(ev.coverage[level]!) })}</>}
              {ev.dir_acc != null && <> · {t("forecast.accDir", { pct: pct(ev.dir_acc) })}</>}
              <span className="muted"> ({t("forecast.accN", { n: ev.coverage_n ?? ev.n })})</span></span>
              : <span className="muted">{t("forecast.accNone")}</span>}
          </li>
        ))}
      </ul>
    </div>
  );
}

/**
 * One point's accuracy as the server states it (test period, n, horizon, mode, error versus the
 * "repeat last month" baseline on the same cases), then the measured coverage of the selected range
 * with its own n and period, and the server's under-coverage warning. No arithmetic here.
 */
function AccuracyDisclosure({ ev, level }: { ev: PointEvaluation; level: Level }) {
  const { t, intl, locale } = useT();
  const cov = ev.coverage?.[level];
  const warn = loc(ev.coverage_warning, locale);
  return (
    <span className="acc-disclosure">
      <span>{loc(ev.summary, locale)}</span>
      {ev.beats_baseline != null && <span className={`acc-verdict ${ev.beats_baseline ? "good" : "flat"}`}>{ev.beats_baseline ? t("forecast.beatsBaseline") : t("forecast.notBeatsBaseline")}</span>}
      {cov != null && <span className="muted">{t("forecast.accCoverage", { level: lvlText(level), pct: `${fmtNum(cov * 100, intl, 0)}%` })}{" "}
        ({t("forecast.accN", { n: ev.coverage_n ?? "-" })}{ev.coverage_period ? `, ${rangeText(period(ev.coverage_period.origin_start, locale), period(ev.coverage_period.origin_end, locale), locale)}` : ""})</span>}
      {warn && <span className="acc-warn" role="note"><TriangleAlert size={14} aria-hidden="true" />{warn}</span>}
    </span>
  );
}

/** Business-language description of the active method and its tested error. */
export function MethodPanel({ prod, points, level = "0.8", notes }: { prod: ModelVersion | null | undefined; points?: ForecastPoint[]; level?: Level; notes?: ResultReadFields["notes"] }) {
  const { t, intl, locale } = useT();
  if (!prod) return null;
  const m = prod.metrics;
  const usd = (v: number | null | undefined) => fmtUsd(v, intl);
  const win = evalWindow(prod.backtest_origins);
  const fit = prod.fit_period?.split("..").at(-1) ?? null;
  // Same-case comparison from the server (1 month ahead) wins over the stored model metrics, so the
  // bars and the sentences never show two different baseline numbers for the same situation.
  // "Same as last month" repeats the last official ICP whatever the benchmarks do, so on the model's
  // own backtest cases it has one error for both situations; the registry's baseline_naive_last_mae
  // is persistence on the smaller case set common to all candidate methods (FORECASTING_METHODOLOGY.md
  // §5), so it is only the fallback when no point carries the same-case figure.
  const evals = ((points as P[] | undefined) ?? []).map((p) => p.evaluation);
  const ev1 = evals.find((e) => e?.mode === "hold_last" && e.horizon === 1 && e.baseline?.mae != null);
  const obs1 = evals.find((e) => e?.mode === "observed" && e.horizon === 1 && e.baseline?.mae != null);
  const holdMae = ev1?.mae ?? m.hold_last_h1_mae;
  const holdBase = ev1?.baseline?.mae ?? m.baseline_naive_hold_last_h1_mae;
  const obsBase = obs1?.baseline?.mae ?? ev1?.baseline?.mae ?? m.baseline_naive_last_mae;
  return (
    <Panel title={t("forecast.methodTitle")} icon={CircleHelp} subtitle={<>{methodLabel(prod.method, t)} · {prod.rule_based ? t("forecast.ruleBased") : t("forecast.statistical")}</>}
      actions={<Link className="btn sm ghost" to="/metodologi">{t("forecast.methodologyLink")} <ArrowRight size={14} aria-hidden="true" /></Link>}>
      <div className="grid cols-2 align-start">
        <div className="stack-sm">
          <p>{loc(notes?.method, locale) ?? (prod.description || methodPlain(prod.method_family, t))}</p>
          {points?.length ? <PointAccuracy points={points} prod={prod} level={level} /> : null}
          {loc(notes?.evaluation, locale) && <p className="xs muted">{loc(notes?.evaluation, locale)}</p>}
          {m.hold_last_h1_mae != null && !points?.length && <p className="small">{t("forecast.accuracyHold", { mae: usd(m.hold_last_h1_mae), naive: usd(m.baseline_naive_hold_last_h1_mae) })}</p>}
          <p className="small muted">{t("forecast.accObservedSecondary", { mae: usd(m.mae), naive: usd(obsBase) })}</p>
          {win && <p className="xs muted">{t("forecast.evalPeriod", { n: win.n, from: period(win.from, locale), to: period(win.to, locale), fit: period(fit, locale) })}</p>}
        </div>
        <div className="stack-sm">
          <div className="section-title">{t("forecast.baselineTitle")}</div>
          {m.hold_last_h1_mae != null && (<>
            <div className="xs muted">{t("forecast.baselineHold")}</div>
            <MetricBars unit="" rows={[
              { label: <strong>{t("forecast.baselineActive")}</strong>, value: holdMae, kind: "model" },
              { label: t("forecast.baselineNaive"), value: holdBase, kind: "baseline" },
            ]} />
          </>)}
          <div className="xs muted mt-2">{t("forecast.baselineObserved")}</div>
          <MetricBars unit="" rows={[
            { label: <strong>{t("forecast.baselineActive")}</strong>, value: m.mae, kind: "model" },
            { label: t("forecast.baselineNaive"), value: obsBase, kind: "baseline" },
          ]} />
          <div className="xs muted">USD/bbl · {t("forecast.accuracyTitle").toLowerCase()}</div>
        </div>
      </div>
    </Panel>
  );
}

type RunResult = NonNullable<ForecastRun["result"]> & ResultReadFields;
const resultOf = (run: ForecastRun): RunResult | null => (run.result as RunResult | null | undefined) ?? null;

/** A failed run is never presented as a result: it says what happened and what to do. */
export function RunFailed({ run }: { run: ForecastRun }) {
  const { t } = useT();
  return <Callout tone="bad" title={run.error_code === "MODEL_UNAVAILABLE" ? t("error.modelUnavailable") : t("error.generic")}>{t("forecast.runFailed")}</Callout>;
}

/** Key facts of a run (origin, issue date, horizon with its months, method) and the notes that qualify it. */
export function ForecastStats({ run }: { run: ForecastRun }) {
  const { t, locale, intl } = useT();
  const res = resultOf(run);
  if (!res) return null;
  const legacy = res.points.some((p) => p.kind === "legacy_record");
  const clip = (run as ForecastRun & RunPlanClip).plan_clip;
  return (<>
    <div className="stat-grid">
      <Stat label={t("forecast.origin")} value={period(res.forecast_origin, locale)} />
      <Stat label={t("forecast.issueDate")} value={dateOnly(res.issue_date, intl)} />
      <Stat label={t("forecast.horizon")} value={t("forecast.horizonMonths", { n: res.points.length })} hint={rangeText(period(res.points[0]?.period, locale), period(res.points.at(-1)?.period, locale), locale)} />
      <Stat label={t("common.method")} value={methodLabel(res.method, t)} />
    </div>
    {clip?.clipped && <p className="small muted">{t("forecast.planClipped", { n: clip.horizon_max, m: clip.stored_horizon })}</p>}
    {legacy && <Callout tone="warn">{t("forecast.legacyNote")}</Callout>}
    {res.warnings.length > 0 && <Callout tone="info" title={t("forecast.warnings")}>{localizePeriods(res.warnings.join(" "), locale)}</Callout>}
  </>);
}

/** Inputs the active method did not use (server notes), one line each. */
export function InputUsageNotes({ run }: { run: ForecastRun }) {
  const { locale } = useT();
  const res = resultOf(run);
  if (!res) return null;
  return (<>
    {res.input_usage?.filter((u) => !u.used_by_method).map((u) => <p key={u.input} className="note-line"><Info size={15} aria-hidden="true" />{loc(u.note, locale)}</p>)}
    {!res.input_usage?.some((u) => !u.used_by_method) && loc(res.notes?.ignored_inputs, locale) && <p className="note-line"><Info size={15} aria-hidden="true" />{loc(res.notes?.ignored_inputs, locale)}</p>}
  </>);
}

/**
 * Value-kind legend, the interval picker and the CSV export. The legend names only the kinds this
 * result contains (both when a stored run does not say), each with its one-line meaning.
 */
export function ForecastToolbar({ run, level, onLevel }: { run: ForecastRun; level: Level; onLevel: (l: Level) => void }) {
  const { t } = useT();
  const kinds = new Set((resultOf(run)?.points ?? []).map((p) => p.kind));
  const known = kinds.has("estimate_awaiting_publication") || kinds.has("future_forecast");
  return (
    <div className="row-between fc-toolbar">
      <div className="kind-legend">
        {(!known || kinds.has("estimate_awaiting_publication")) && <span className="chip-row" title={t("kind.estimate.help")}><KindChip kind="estimate_awaiting_publication" /><span className="xs muted">{t("kind.estimate.short")}</span></span>}
        {(!known || kinds.has("future_forecast")) && <span className="chip-row" title={t("kind.forecast.help")}><KindChip kind="future_forecast" /><span className="xs muted">{t("kind.forecast.short")}</span></span>}
      </div>
      <div className="row"><LevelPicker level={level} onChange={onLevel} /><ExportButton run={run} /></div>
    </div>
  );
}

/** The interval explanation, the server's coverage warning and the datasets used. */
export function ResultFootnotes({ run }: { run: ForecastRun }) {
  const { t, locale } = useT();
  const res = resultOf(run);
  if (!res) return null;
  const datasets = datasetList(run.dataset_versions, t);
  const conditional = res.points.some((p) => p.conditional_on_scenario);
  return (<>
    {loc(res.notes?.coverage_warning, locale) && <Callout tone="warn" title={t("forecast.coverageWarnTitle")}>{loc(res.notes?.coverage_warning, locale)}</Callout>}
    <p className="xs muted">{loc(res.notes?.interval, locale) ?? t("meth.accuracy.intervalNote")} {conditional && t("forecast.rangeIfRightNote")} {datasets && t("forecast.dataVersionsNote", { list: datasets })}</p>
  </>);
}

/** The per-month input assumptions (sources, used / not used by the active method). */
export function ForecastAssumptionsView({ run }: { run: ForecastRun }) {
  const { t } = useT();
  const res = resultOf(run);
  if (!res) return <RunFailed run={run} />;
  return (
    <div className="stack-sm">
      <div className="section-title flush">{t("forecast.assumptionsTitle")}</div>
      <p className="small muted">{t("forecast.assumptionsSub")}</p>
      <div className="table-frame"><AssumptionTable points={res.points} /></div>
    </div>
  );
}

/** The "result" part of a run: facts, assumption note, legend and interval, chart, table, footnotes. */
export function ForecastResultView({ run, history, level, onLevel }: { run: ForecastRun; history: HistoryPoint[]; level: Level; onLevel: (l: Level) => void }) {
  const res = resultOf(run);
  if (!res) return <RunFailed run={run} />;
  return (
    <div className="stack">
      <ForecastStats run={run} />
      <AssumptionNote points={res.points} history={history} runId={run.id} />
      <InputUsageNotes run={run} />
      <ForecastToolbar run={run} level={level} onLevel={onLevel} />
      <ForecastChart history={history} run={run} level={level} showBrent />
      <ForecastTable points={res.points} level={level} history={history} />
      <ResultFootnotes run={run} />
    </div>
  );
}

/** Complete view of one forecast run in a single column (scenario results; the forecast page uses the sections in tabs). */
export function ForecastRunView({ run, history, level, onLevel, prod }: { run: ForecastRun; history: HistoryPoint[]; level: Level; onLevel: (l: Level) => void; prod?: ModelVersion | null }) {
  const { t } = useT();
  const res = resultOf(run);
  if (!res) return <RunFailed run={run} />;
  return (
    <div className="stack">
      <ForecastStats run={run} />
      <AssumptionNote points={res.points} history={history} runId={run.id} />
      <InputUsageNotes run={run} />
      <ForecastToolbar run={run} level={level} onLevel={onLevel} />
      <ForecastChart history={history} run={run} level={level} showBrent />
      <ForecastTable points={res.points} level={level} history={history} />
      <ForecastAssumptionsView run={run} />
      <ResultFootnotes run={run} />
      {prod && <MethodPanel prod={prod} points={res.points} level={level} notes={res.notes} />}
      {!prod && <p className="small"><Link to="/metodologi">{t("forecast.methodologyLink")}</Link></p>}
    </div>
  );
}
