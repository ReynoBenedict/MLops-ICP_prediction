/**
 * Chart components: one y-axis per chart with its unit, "nice" evenly spaced ticks, recessive
 * grid, 2px lines, fixed identity colours per series, a legend for >= 2 series, and calendar
 * gaps rendered as breaks (never interpolated). Every chart has a one-sentence summary
 * (`aria-describedby`) and a keyboard-reachable "Tabel" view with the same values and units.
 * Estimates/forecasts use the entity's hue dashed; assumed values (hold-last, scenario) are
 * dashed, thinner and labelled "asumsi" in legend, tooltip and table. Scenario comparisons add
 * distinct marker shapes so identity never relies on colour alone.
 * Axis text follows the reader's text size (tokens in rem): the x-axis height, the y-axis width and
 * the marker label's room are derived from the px size of --fs-meta (lib/text-size.ts).
 */
import { useCallback, useId, useMemo, useRef, useState, type ReactNode } from "react";
import {
  Area, CartesianGrid, ComposedChart, Line, ReferenceArea, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
  type TooltipContentProps,
} from "recharts";
import { BarChart3, Table2 } from "lucide-react";
import { fmtNum, period, rangeText } from "../format";
import { useT } from "../i18n";
import { META_PX, useMetaPx } from "../lib/text-size";
import { Segmented, TableScroll } from "./ui";

/**
 * Series colours, validated with the dataviz palette validator on the light surface (lightness,
 * chroma, CVD and normal-vision separation all PASS for icp/brent/dubai/wti and for
 * icp + s1..s4). Status red is never a series colour. Forecasts use the ICP hue dashed.
 * Same values as the --series-* tokens in styles/tokens.css (Recharts needs literal colours for
 * gradient stops and marker fills, so they are kept here as constants).
 */
export const SERIES_COLORS = {
  icp: "#1d6fb8", brent: "#e8692f", dubai: "#6250d6", wti: "#8a5a0b", forecast: "#1d6fb8", spread: "#0f6b73", idr: "#8a5a0b",
  s1: "#e8692f", s2: "#6250d6", s3: "#8a5a0b", s4: "#0f8f6b",
};
export const MARKERS = ["circle", "square", "triangle", "diamond"] as const;

export interface SeriesSpec {
  key: string;
  label: string;
  color: string;
  dashed?: boolean;
  /** Assumed values (hold-last, scenario): dashed, thinner, "asumsi" tag in legend/tooltip/table. */
  assumed?: boolean;
  fill?: boolean;
  width?: number;
  marker?: (typeof MARKERS)[number];
}

export interface Zone {
  from: string;
  to: string;
  label?: string;
  kind: "forecast" | "gap";
}

export type ChartUnit = "usd" | "idr" | "pct" | "spread";

interface Props {
  data: Array<Record<string, unknown>>;
  series: SeriesSpec[];
  band?: { key: string; label: string };
  zones?: Zone[];
  marker?: { at: string; label: string };
  height?: "sm" | "md" | "lg";
  /** Custom table view; by default a table of every plotted value is generated. */
  table?: ReactNode;
  ariaLabel: string;
  /** One-sentence description of what the chart shows (default: first and last value per series). */
  summary?: string;
  unit?: ChartUnit;
  tooltipExtra?: (row: Record<string, unknown>) => ReactNode;
  yDomainZero?: boolean;
}

export function fmtVal(v: number, unit: ChartUnit | undefined, intl: string): string {
  if (unit === "idr") return `Rp ${fmtNum(v, intl, 0)}`;
  if (unit === "pct") return `${fmtNum(v, intl, 1)}%`;
  if (unit === "spread") return `${v > 0 ? "+" : v < 0 ? "−" : ""}${fmtNum(Math.abs(v), intl, 2)}`;
  return `USD ${fmtNum(v, intl, 2)}`;
}

/** Evenly spaced "nice" ticks (step 1/2/2.5/5 × 10ⁿ), 4 to 6 of them, covering [min, max]. */
export function niceTicks(min: number, max: number, opts: { zero?: boolean; includeZero?: boolean } = {}): number[] {
  let lo = opts.zero ? Math.min(0, min) : min;
  let hi = max;
  if (opts.includeZero) { lo = Math.min(lo, 0); hi = Math.max(hi, 0); }
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) return [0, 1];
  if (hi === lo) { const pad = Math.abs(hi) * 0.1 || 1; lo -= pad; hi += pad; }
  const span = hi - lo;
  const mags = [Math.floor(Math.log10(span / 5)), Math.floor(Math.log10(span / 5)) + 1];
  for (const mag of mags) {
    for (const m of [1, 2, 2.5, 5]) {
      const step = m * 10 ** mag;
      const start = Math.floor(lo / step + 1e-9) * step;
      const end = Math.ceil(hi / step - 1e-9) * step;
      const n = Math.round((end - start) / step) + 1;
      if (n <= 6 && n >= 3) {
        const ticks = Array.from({ length: n }, (_, i) => Number((start + i * step).toFixed(10)));
        if (!opts.zero && !opts.includeZero && ticks[0]! < 0 && min >= 0) continue;
        return ticks;
      }
    }
  }
  return [lo, hi];
}

/** Axis tick text: "Rp 1,5 jt" / "IDR 1.5 m" for rupiah, plain numbers otherwise. */
export function tickText(v: number, unit: ChartUnit | undefined, intl: string, locale: "id" | "en"): string {
  if (unit === "idr") {
    const m = v / 1_000_000;
    const digits = Number.isInteger(Math.round(m * 100) / 100) ? 0 : Math.abs(m * 10 - Math.round(m * 10)) < 1e-6 ? 1 : 2;
    return locale === "en" ? `IDR ${fmtNum(m, intl, digits)} m` : `Rp ${fmtNum(m, intl, digits)} jt`;
  }
  const digits = Number.isInteger(v) ? 0 : Math.abs(v * 10 - Math.round(v * 10)) < 1e-6 ? 1 : 2;
  if (unit === "pct") return `${fmtNum(v, intl, digits)}%`;
  if (unit === "spread") return `${v > 0 ? "+" : v < 0 ? "−" : ""}${fmtNum(Math.abs(v), intl, digits)}`;
  return fmtNum(v, intl, digits);
}

function numbersIn(data: Array<Record<string, unknown>>, keys: string[], bandKey?: string): number[] {
  const out: number[] = [];
  for (const row of data) {
    for (const k of keys) { const v = row[k]; if (typeof v === "number" && Number.isFinite(v)) out.push(v); }
    const b = bandKey ? row[bandKey] : undefined;
    if (Array.isArray(b)) for (const v of b) if (typeof v === "number" && Number.isFinite(v)) out.push(v);
  }
  return out;
}

function ChartTooltip({ active, payload, label, series, extra, unit, band }:
  TooltipContentProps<number, string> & { series: SeriesSpec[]; extra?: (row: Record<string, unknown>) => ReactNode; unit: ChartUnit | undefined; band?: { key: string; label: string } }) {
  const { t, locale, intl } = useT();
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload as Record<string, unknown>;
  const values = series.filter((s) => typeof row[s.key] === "number");
  const bandVal = band ? row[band.key] : undefined;
  return (
    <div className="chart-tooltip">
      <div className="t-title">{period(String(label), locale)}</div>
      {values.length === 0 && <div className="t-note">{t("forecast.noObs")}</div>}
      {values.map((s) => (
        <div key={s.key} className="t-row">
          <span className="legend-key">
            <span className={s.dashed || s.assumed ? "swatch-dash" : "swatch-line"} style={s.dashed || s.assumed ? { borderColor: s.color } : { background: s.color }} />
            <span>{s.label}{s.assumed && <span className="assumed"> ({t("chart.assumed")})</span>}</span>
          </span>
          <strong>{fmtVal(row[s.key] as number, unit, intl)}</strong>
        </div>
      ))}
      {Array.isArray(bandVal) && (
        <div className="t-note">{band?.label}: {rangeText(fmtVal(bandVal[0] as number, unit, intl), fmtVal(bandVal[1] as number, unit, intl), locale)}</div>
      )}
      {extra?.(row)}
    </div>
  );
}

function MarkerSwatch({ marker, color }: { marker?: SeriesSpec["marker"]; color: string }) {
  if (!marker) return null;
  const style = { color };
  if (marker === "square") return <svg width="10" height="10" style={style} aria-hidden="true"><rect width="10" height="10" fill="currentColor" /></svg>;
  if (marker === "triangle") return <svg width="10" height="10" style={style} aria-hidden="true"><path d="M5 0 L10 10 L0 10Z" fill="currentColor" /></svg>;
  if (marker === "diamond") return <svg width="10" height="10" style={style} aria-hidden="true"><path d="M5 0 L10 5 L5 10 L0 5Z" fill="currentColor" /></svg>;
  return <svg width="10" height="10" style={style} aria-hidden="true"><circle cx="5" cy="5" r="5" fill="currentColor" /></svg>;
}

export function ChartLegend({ series, band, zones }: { series: SeriesSpec[]; band?: { label: string }; zones?: Zone[] }) {
  const { t } = useT();
  const kinds = new Set((zones ?? []).map((z) => z.kind));
  const gapLabel = (zones ?? []).find((z) => z.kind === "gap" && z.label)?.label ?? t("forecast.gap");
  return (
    <div className="legend" aria-hidden="true">
      {series.map((s) => (
        <span className="legend-key" key={s.key}>
          <span className={s.dashed || s.assumed ? `swatch-dash${s.assumed ? " thin" : ""}` : "swatch-line"} style={s.dashed || s.assumed ? { borderColor: s.color } : { background: s.color }} />
          <MarkerSwatch marker={s.marker} color={s.color} />
          <span>{s.label}{s.assumed && <span className="assumed"> ({t("chart.assumed")})</span>}</span>
        </span>
      ))}
      {band && <span className="legend-key"><span className="swatch-area sw-band" />{band.label}</span>}
      {kinds.has("forecast") && <span className="legend-key"><span className="swatch-area sw-forecast" />{t("forecast.chartZone")}</span>}
      {kinds.has("gap") && <span className="legend-key"><span className="swatch-area sw-gap" />{gapLabel}</span>}
    </div>
  );
}

function dotFor(marker: SeriesSpec["marker"], color: string) {
  if (!marker) return false;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (props: any) => {
    const { cx, cy, value } = props as { cx?: number; cy?: number; value?: number };
    if (cx === undefined || cy === undefined || value === undefined || value === null) return <g key={`${cx}-${cy}`} />;
    const s = 4.5;
    if (marker === "square") return <rect key={`${cx}-${cy}`} x={cx - s} y={cy - s} width={s * 2} height={s * 2} fill={color} stroke="var(--surface)" strokeWidth={1.5} />;
    if (marker === "triangle") return <path key={`${cx}-${cy}`} d={`M${cx} ${cy - s - 1} L${cx + s + 1} ${cy + s} L${cx - s - 1} ${cy + s}Z`} fill={color} stroke="var(--surface)" strokeWidth={1.5} />;
    if (marker === "diamond") return <path key={`${cx}-${cy}`} d={`M${cx} ${cy - s - 1} L${cx + s + 1} ${cy} L${cx} ${cy + s + 1} L${cx - s - 1} ${cy}Z`} fill={color} stroke="var(--surface)" strokeWidth={1.5} />;
    return <circle key={`${cx}-${cy}`} cx={cx} cy={cy} r={s} fill={color} stroke="var(--surface)" strokeWidth={1.5} />;
  };
}

/** Default text alternative: each series' first and last plotted value with its month. */
function useAutoSummary(data: Array<Record<string, unknown>>, series: SeriesSpec[], unit: ChartUnit | undefined): string {
  const { t, locale, intl } = useT();
  return useMemo(() => {
    const parts: string[] = [];
    for (const s of series.slice(0, 4)) {
      const pts = data.filter((r) => typeof r[s.key] === "number");
      const a = pts[0], z = pts.at(-1);
      if (!a || !z) continue;
      const label = s.assumed ? `${s.label} (${t("chart.assumed")})` : s.label;
      parts.push(a === z
        ? t("chart.summaryOne", { series: label, v: fmtVal(a[s.key] as number, unit, intl), p: period(String(a.period), locale) })
        : t("chart.summarySeries", { series: label, v1: fmtVal(a[s.key] as number, unit, intl), p1: period(String(a.period), locale), v2: fmtVal(z[s.key] as number, unit, intl), p2: period(String(z.period), locale) }));
    }
    return parts.length ? `${parts.join("; ")}.` : t("chart.summaryEmpty");
  }, [data, series, unit, t, locale, intl]);
}

/** The values behind a chart, as a table (same values and units as the plot). */
function ChartDataTable({ data, series, band, unit, label }: { data: Array<Record<string, unknown>>; series: SeriesSpec[]; band?: { key: string; label: string }; unit: ChartUnit | undefined; label: string }) {
  const { t, locale, intl } = useT();
  const rows = data.filter((r) => series.some((s) => typeof r[s.key] === "number")).slice().reverse();
  const unitText = unitLabel(unit, t);
  return (
    <TableScroll label={label} className="chart-table">
      <table className="table rc">
        <caption className="sr-only">{label} ({unitText})</caption>
        <thead><tr><th>{t("common.month")}</th>{series.map((s) => <th key={s.key} className="num">{s.label}{s.assumed ? ` (${t("chart.assumed")})` : ""} <span className="th-unit">{unitText}</span></th>)}
          {band && <th className="num">{band.label}</th>}</tr></thead>
        <tbody>{rows.map((r) => (
          <tr key={String(r.period)}>
            <td className="primary-cell nowrap rc-title">{period(String(r.period), locale)}</td>
            {series.map((s, i) => <td key={s.key} className={`num${i === 0 ? " rc-main" : ""}`} data-label={s.label}>{typeof r[s.key] === "number" ? fmtVal(r[s.key] as number, unit, intl) : "-"}</td>)}
            {band && <td className="num" data-label={band.label}>{Array.isArray(r[band.key]) ? rangeText(fmtVal((r[band.key] as number[])[0]!, unit, intl), fmtVal((r[band.key] as number[])[1]!, unit, intl), locale) : "-"}</td>}
          </tr>
        ))}</tbody>
      </table>
    </TableScroll>
  );
}

export function unitLabel(unit: ChartUnit | undefined, t: ReturnType<typeof useT>["t"]): string {
  return unit === "idr" ? t("common.idrPerBbl") : unit === "pct" ? "%" : unit === "spread" ? t("chart.unitSpread") : t("common.usdPerBbl");
}

/** Approximate advance of one character of chart label text, as a share of its font size (label room). */
const CHAR_EM = 0.52;
/** Approximate advance of a tabular digit, as a share of the font size (y-axis room). */
const DIGIT_EM = 0.6;

/** Width of the chart box, kept current while it is mounted (0 until measured). */
function useBoxWidth(): [(el: HTMLDivElement | null) => void, number] {
  const [width, setWidth] = useState(0);
  const ro = useRef<ResizeObserver | null>(null);
  const ref = useCallback((el: HTMLDivElement | null) => {
    ro.current?.disconnect();
    ro.current = null;
    if (!el) return;
    const read = () => setWidth(el.clientWidth);
    read();
    if (typeof ResizeObserver === "undefined") return;
    ro.current = new ResizeObserver(read);
    ro.current.observe(el);
  }, []);
  return [ref, width];
}

/**
 * The marker's label ("Data resmi terakhir: Sep 2026") at the top of its reference line: left of the
 * line when it fits there, else right of it; when one line fits on neither side (phones, large text)
 * it wraps to the roomier side, so it is never cut at the chart's edge.
 */
function MarkerLabel({ viewBox, text, plotLeft, plotRight, fontPx }: { viewBox?: { x?: number; y?: number }; text: string; plotLeft: number; plotRight: number; fontPx: number }) {
  const x = viewBox?.x ?? 0, y = viewBox?.y ?? 0;
  const charW = fontPx * CHAR_EM, gap = 6;
  const leftRoom = x - plotLeft - gap, rightRoom = plotRight - x - gap;
  const oneLine = text.length * charW;
  const left = oneLine <= leftRoom || (oneLine > rightRoom && leftRoom >= rightRoom);
  const room = Math.max(charW * 8, left ? leftRoom : rightRoom);
  const lines: string[] = [];
  // Split at plain spaces only: a non-breaking space keeps a month name with its year ("Sep 2026").
  for (const word of text.split(" ")) {
    const last = lines[lines.length - 1];
    if (last !== undefined && (last.length + 1 + word.length) * charW <= room) lines[lines.length - 1] = `${last} ${word}`;
    else lines.push(word);
  }
  const tx = left ? x - gap : x + gap;
  return (
    <text x={tx} y={y + fontPx + 2} textAnchor={left ? "end" : "start"} className="recharts-label" fill="var(--muted)">
      {lines.map((l, i) => <tspan key={i} x={tx} dy={i === 0 ? 0 : fontPx * 1.25}>{l}</tspan>)}
    </text>
  );
}

export function TimeSeriesChart({ data, series, band, zones = [], marker, height = "md", table, ariaLabel, summary, unit = "usd", tooltipExtra, yDomainZero }: Props) {
  const { t, locale, intl } = useT();
  const metaPx = useMetaPx();
  const [boxRef, boxW] = useBoxWidth();
  const [view, setView] = useState<"chart" | "table">("chart");
  const summaryId = useId();
  const auto = useAutoSummary(data, series, unit);
  const sizeClass = height === "md" ? "" : ` ${height}`;
  const revealKey = `${data.length}|${String(data[0]?.period ?? "")}|${String(data.at(-1)?.period ?? "")}|${series.map((s) => s.key).join(",")}|${band?.key ?? ""}`;
  const ticks = useMemo(() => {
    const vals = numbersIn(data, series.map((s) => s.key), band?.key);
    if (!vals.length) return [0, 1];
    return niceTicks(Math.min(...vals), Math.max(...vals), { zero: yDomainZero, includeZero: unit === "spread" });
  }, [data, series, band, yDomainZero, unit]);
  const domain: [number, number] = [ticks[0]!, ticks.at(-1)!];
  // Axis room for the tick text at the reader's text size: the default 48/74 px y-axis grows by what
  // its longest tick grows with larger text, the x-axis is tall enough for one line of ticks; both
  // are the defaults at the default text size.
  const longestTick = Math.max(...ticks.map((v) => tickText(v, unit, intl, locale).length));
  const yWidth = Math.ceil((unit === "idr" ? 74 : 48) + longestTick * Math.max(0, metaPx - META_PX) * DIGIT_EM);
  const xHeight = Math.max(30, Math.ceil(10 + metaPx * 1.3));
  // Room above the top tick (its label is centred on the top grid line): 10 px at the default size.
  const margin = { top: Math.max(10, Math.ceil(metaPx * 0.8)), right: 12, bottom: 0, left: 0 };
  return (
    <figure className="chart-figure">
      <div className="chart-head">
        <ChartLegend series={series} band={band} zones={zones} />
        <Segmented label={`${t("common.view")}: ${ariaLabel}`} value={view} onChange={setView}
          options={[{ value: "chart", label: t("common.chart"), icon: BarChart3 }, { value: "table", label: t("common.table"), icon: Table2 }]} />
      </div>
      <p className="chart-summary" id={summaryId}>{summary ?? auto}</p>
      {view === "chart" ? (
        <>
          <div className="chart-unit" aria-hidden="true">{unitLabel(unit, t)}</div>
          {/* The key changes only when the plotted dataset changes (range, series set, band), so the
              one-time reveal replays for a new dataset but never on hover or unrelated re-renders. */}
          <div key={revealKey} ref={boxRef} className={`chart-box chart-anim${sizeClass}`} role="img" aria-label={`${ariaLabel} (${unitLabel(unit, t)})`} aria-describedby={summaryId}>
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={data} margin={margin}>
                <defs>
                  {series.filter((s) => s.fill).map((s) => (
                    <linearGradient key={s.key} id={`fill-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={s.color} stopOpacity={0.16} />
                      <stop offset="100%" stopColor={s.color} stopOpacity={0} />
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid stroke="var(--line-subtle)" vertical={false} />
                {zones.filter((z) => z.from === z.to).map((z) => (
                  <ReferenceLine key={`${z.kind}-${z.from}`} x={z.from} stroke={z.kind === "forecast" ? "var(--forecast-zone)" : "var(--gap-mark)"} strokeWidth={7} />
                ))}
                {zones.filter((z) => z.from !== z.to).map((z) => (
                  <ReferenceArea key={`${z.kind}-${z.from}`} x1={z.from} x2={z.to} ifOverflow="hidden"
                    fill={z.kind === "forecast" ? "var(--forecast-zone)" : "var(--gap-zone)"} fillOpacity={1}
                    stroke={z.kind === "forecast" ? "var(--series-icp)" : undefined} strokeDasharray={z.kind === "forecast" ? "3 4" : undefined} strokeOpacity={z.kind === "forecast" ? 0.5 : 0}
                    label={z.label ? { value: z.label, position: "insideTopLeft", fill: "var(--muted)" } : undefined} />
                ))}
                {unit === "spread" && <ReferenceLine y={0} stroke="var(--faint)" />}
                {marker && (
                  <ReferenceLine x={marker.at} stroke="var(--muted)" strokeDasharray="3 3"
                    label={<MarkerLabel text={marker.label} plotLeft={margin.left + yWidth} plotRight={(boxW || 10_000) - margin.right} fontPx={metaPx} />} />
                )}
                <XAxis dataKey="period" tickFormatter={(p: string) => period(p, locale)} stroke="var(--line-strong)" tickLine={false}
                  tick={{ fill: "var(--muted)" }} minTickGap={Math.round(40 * (metaPx / META_PX))} dy={6} height={xHeight} interval="preserveStartEnd" />
                <YAxis stroke="var(--line-strong)" tickLine={false} axisLine={false} tick={{ fill: "var(--muted)" }} width={yWidth}
                  domain={domain} ticks={ticks} interval={0} allowDataOverflow={false} tickFormatter={(v: number) => tickText(v, unit, intl, locale)} />
                <Tooltip content={(p) => <ChartTooltip {...(p as TooltipContentProps<number, string>)} series={series} extra={tooltipExtra} unit={unit} band={band} />}
                  cursor={{ stroke: "var(--faint)", strokeWidth: 1, strokeDasharray: "3 3" }} isAnimationActive={false} />
                {band && <Area dataKey={band.key} stroke="none" fill="var(--band)" isAnimationActive={false} connectNulls={false} />}
                {series.filter((s) => s.fill).map((s) => (
                  <Area key={`a-${s.key}`} dataKey={s.key} stroke="none" fill={`url(#fill-${s.key})`} isAnimationActive={false}
                    connectNulls={false} legendType="none" tooltipType="none" />
                ))}
                {series.map((s) => (
                  <Line key={s.key} dataKey={s.key} name={s.label} stroke={s.color} strokeWidth={s.width ?? (s.assumed ? 1.5 : 2)}
                    strokeDasharray={s.assumed ? "2 4" : s.dashed ? "6 4" : undefined} dot={dotFor(s.marker, s.color)} strokeOpacity={s.assumed ? 0.85 : 1}
                    activeDot={{ r: 4.5, strokeWidth: 2, stroke: "var(--surface)" }} connectNulls={false} isAnimationActive={false} />
                ))}
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </>
      ) : (
        table ?? <ChartDataTable data={data} series={series} band={band} unit={unit} label={ariaLabel} />
      )}
    </figure>
  );
}

/** Horizontal comparison bars (lower is better for error metrics). */
export function MetricBars({ rows, unit = "" }: { rows: Array<{ label: ReactNode; value: number | null | undefined; kind: "model" | "baseline" }>; unit?: string }) {
  const { intl } = useT();
  const max = Math.max(...rows.map((r) => r.value ?? 0), 1e-9);
  return (
    <div className="bars">
      {rows.map((r, i) => (
        <div className="bar-row" key={i}>
          <span className="bar-label">{r.label}</span>
          <span className="bar-track" aria-hidden="true">
            <span className={`bar-fill ${r.kind}`} style={{ width: `${r.value ? Math.max(3, (r.value / max) * 100) : 0}%` }} />
          </span>
          <span className="bar-value">{r.value === null || r.value === undefined ? "-" : `${fmtNum(r.value, intl)}${unit}`}</span>
        </div>
      ))}
    </div>
  );
}

const MONTH_INITIALS = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"];

/** Year x month presence grid. `present` = observed, `missing` = gap inside the covered range. */
export function CoverageGrid({ points, startYear, endPeriod, labels }: { points: Map<string, boolean>; startYear: number; endPeriod: string; labels: { observed: string; missing: string; outside: string; title: string } }) {
  const { locale } = useT();
  const endYear = Number(endPeriod.slice(0, 4));
  const years = Array.from({ length: endYear - startYear + 1 }, (_, i) => startYear + i);
  const firstPresent = [...points.entries()].filter(([, v]) => v).map(([k]) => k).sort()[0] ?? "";
  return (
    <div className="coverage" role="table" aria-label={labels.title}>
      <span />
      {MONTH_INITIALS.map((m, i) => <span key={i} className="mh" aria-hidden="true">{m}</span>)}
      {years.map((y) => (
        <div key={y} className="coverage-row" role="row">
          <span className="yr" role="rowheader">{y}</span>
          {Array.from({ length: 12 }, (_, m) => {
            const p = `${y}-${String(m + 1).padStart(2, "0")}`;
            const state = p > endPeriod ? "future" : p < firstPresent ? "none" : points.get(p) ? "present" : "missing";
            const label = state === "present" ? labels.observed : state === "missing" ? labels.missing : labels.outside;
            return <span key={p} role="cell" className={`cell ${state}`} title={`${period(p, locale)}: ${label}`} aria-label={`${period(p, locale)}: ${label}`} />;
          })}
        </div>
      ))}
    </div>
  );
}
