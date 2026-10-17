/**
 * Chart components: one y-axis per chart, recessive grid, 2px lines, hover tooltip by default,
 * fixed identity colors per series, a legend for >= 2 series, and calendar gaps rendered as
 * breaks - never interpolated. Scenario comparisons add distinct marker shapes so identity never
 * relies on colour alone.
 */
import { useState, type ReactNode } from "react";
import {
  Area, CartesianGrid, ComposedChart, Line, ReferenceArea, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
  type TooltipContentProps,
} from "recharts";
import { BarChart3, Table2 } from "lucide-react";
import { fmtNum, period, rangeText } from "../format";
import { useT } from "../i18n";
import { Segmented } from "./ui";

export const SERIES_COLORS = {
  icp: "var(--icp)", brent: "var(--wti)", dubai: "var(--cmp)", wti: "var(--amber-ink)", forecast: "var(--brand)",
  s1: "#d99a2b", s2: "#6250d6", s3: "#0f8f6b", s4: "#b42318",
};
export const MARKERS = ["circle", "square", "triangle", "diamond"] as const;

export interface SeriesSpec {
  key: string;
  label: string;
  color: string;
  dashed?: boolean;
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

interface Props {
  data: Array<Record<string, unknown>>;
  series: SeriesSpec[];
  band?: { key: string; label: string };
  zones?: Zone[];
  marker?: { at: string; label: string };
  height?: "sm" | "md" | "lg";
  table?: ReactNode;
  ariaLabel: string;
  unit?: "usd" | "idr" | "pct" | "spread";
  tooltipExtra?: (row: Record<string, unknown>) => ReactNode;
  yDomainZero?: boolean;
}

function fmtVal(v: number, unit: Props["unit"], intl: string): string {
  if (unit === "idr") return `Rp ${fmtNum(v, intl, 0)}`;
  if (unit === "pct") return `${fmtNum(v, intl, 1)} %`;
  if (unit === "spread") return `${v > 0 ? "+" : ""}${fmtNum(v, intl, 2)}`;
  return `USD ${fmtNum(v, intl, 2)}`;
}

function ChartTooltip({ active, payload, label, series, extra, unit, band }:
  TooltipContentProps<number, string> & { series: SeriesSpec[]; extra?: (row: Record<string, unknown>) => ReactNode; unit: Props["unit"]; band?: { key: string; label: string } }) {
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
            <span className={s.dashed ? "swatch-dash" : "swatch-line"} style={s.dashed ? { borderColor: s.color } : { background: s.color }} />
            {s.label}
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
          <span className={s.dashed ? "swatch-dash" : "swatch-line"} style={s.dashed ? { borderColor: s.color } : { background: s.color }} />
          <MarkerSwatch marker={s.marker} color={s.color} />
          {s.label}
        </span>
      ))}
      {band && <span className="legend-key"><span className="swatch-area" style={{ background: "var(--band)" }} />{band.label}</span>}
      {kinds.has("forecast") && <span className="legend-key"><span className="swatch-area" style={{ background: "var(--forecast-zone)", border: "1px dashed var(--brand)" }} />{t("forecast.chartZone")}</span>}
      {kinds.has("gap") && <span className="legend-key"><span className="swatch-area" style={{ background: "var(--gap-zone)", border: "1px dashed #e19a3c" }} />{gapLabel}</span>}
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
    if (marker === "square") return <rect key={`${cx}-${cy}`} x={cx - s} y={cy - s} width={s * 2} height={s * 2} fill={color} stroke="#fff" strokeWidth={1.5} />;
    if (marker === "triangle") return <path key={`${cx}-${cy}`} d={`M${cx} ${cy - s - 1} L${cx + s + 1} ${cy + s} L${cx - s - 1} ${cy + s}Z`} fill={color} stroke="#fff" strokeWidth={1.5} />;
    if (marker === "diamond") return <path key={`${cx}-${cy}`} d={`M${cx} ${cy - s - 1} L${cx + s + 1} ${cy} L${cx} ${cy + s + 1} L${cx - s - 1} ${cy}Z`} fill={color} stroke="#fff" strokeWidth={1.5} />;
    return <circle key={`${cx}-${cy}`} cx={cx} cy={cy} r={s} fill={color} stroke="#fff" strokeWidth={1.5} />;
  };
}

export function TimeSeriesChart({ data, series, band, zones = [], marker, height = "md", table, ariaLabel, unit = "usd", tooltipExtra, yDomainZero }: Props) {
  const { t, locale, intl } = useT();
  const [view, setView] = useState<"chart" | "table">("chart");
  const sizeClass = height === "md" ? "" : ` ${height}`;
  const revealKey = `${data.length}|${String(data[0]?.period ?? "")}|${String(data.at(-1)?.period ?? "")}|${series.map((s) => s.key).join(",")}|${band?.key ?? ""}`;
  const tickFmt = (v: number) => unit === "idr" ? `${Math.round(v / 1000)}k` : unit === "pct" ? `${Math.round(v)}%` : unit === "spread" ? fmtNum(v, intl, 0) : `${Math.round(v)}`;
  const pad = unit === "idr" ? 50000 : 4;
  const domain: [(min: number) => number, (max: number) => number] = unit === "spread"
    ? [(min) => Math.floor((min - 2) / 2) * 2, (max) => Math.ceil((max + 2) / 2) * 2]
    : [(min) => (yDomainZero ? 0 : Math.max(0, Math.floor((min - pad) / 5) * 5)), (max) => Math.ceil((max + pad) / 5) * 5];
  return (
    <figure style={{ margin: 0 }} className="stack-sm">
      <div className="row-between">
        <ChartLegend series={series} band={band} zones={zones} />
        {table && (
          <Segmented label={t("common.view")} value={view} onChange={setView}
            options={[{ value: "chart", label: t("common.chart"), icon: BarChart3 }, { value: "table", label: t("common.table"), icon: Table2 }]} />
        )}
      </div>
      {view === "chart" ? (
        // The key changes only when the plotted dataset changes (range, series set, band), so the
        // one-time reveal replays for a new dataset but never on hover or unrelated re-renders.
        <div key={revealKey} className={`chart-box chart-anim${sizeClass}`} role="img" aria-label={ariaLabel}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data} margin={{ top: 10, right: 12, bottom: 0, left: 0 }}>
              <defs>
                {series.filter((s) => s.fill).map((s) => (
                  <linearGradient key={s.key} id={`fill-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={s.color} stopOpacity={0.16} />
                    <stop offset="100%" stopColor={s.color} stopOpacity={0} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid stroke="#ecebe5" vertical={false} />
              {zones.filter((z) => z.from === z.to).map((z) => (
                <ReferenceLine key={`${z.kind}-${z.from}`} x={z.from} stroke={z.kind === "forecast" ? "var(--forecast-zone)" : "rgba(181, 71, 8, 0.16)"} strokeWidth={7} />
              ))}
              {zones.filter((z) => z.from !== z.to).map((z) => (
                <ReferenceArea key={`${z.kind}-${z.from}`} x1={z.from} x2={z.to} ifOverflow="hidden"
                  fill={z.kind === "forecast" ? "var(--forecast-zone)" : "var(--gap-zone)"} fillOpacity={1}
                  strokeOpacity={0} label={z.label ? { value: z.label, position: "insideTopLeft", fill: "#5d6779", fontSize: 12.5, fontWeight: 600 } : undefined} />
              ))}
              {unit === "spread" && <ReferenceLine y={0} stroke="#9aa2ae" />}
              {marker && (
                <ReferenceLine x={marker.at} stroke="#9aa2ae" strokeDasharray="3 3"
                  label={{ value: marker.label, position: "insideTopRight", fill: "#5d6779", fontSize: 12.5 }} />
              )}
              <XAxis dataKey="period" tickFormatter={(p: string) => period(p, locale)} stroke="#d4d1c7" tickLine={false}
                tick={{ fill: "#5d6779", fontSize: 12.5 }} minTickGap={36} dy={6} />
              <YAxis stroke="#d4d1c7" tickLine={false} axisLine={false} tick={{ fill: "#5d6779", fontSize: 12.5 }} width={unit === "idr" ? 52 : 46}
                domain={domain} tickFormatter={tickFmt} />
              <Tooltip content={(p) => <ChartTooltip {...(p as TooltipContentProps<number, string>)} series={series} extra={tooltipExtra} unit={unit} band={band} />}
                cursor={{ stroke: "#9aa2ae", strokeWidth: 1, strokeDasharray: "3 3" }} isAnimationActive={false} />
              {band && <Area dataKey={band.key} stroke="none" fill="var(--band)" isAnimationActive={false} connectNulls={false} />}
              {series.filter((s) => s.fill).map((s) => (
                <Area key={`a-${s.key}`} dataKey={s.key} stroke="none" fill={`url(#fill-${s.key})`} isAnimationActive={false}
                  connectNulls={false} legendType="none" tooltipType="none" />
              ))}
              {series.map((s) => (
                <Line key={s.key} dataKey={s.key} name={s.label} stroke={s.color} strokeWidth={s.width ?? 2}
                  strokeDasharray={s.dashed ? "6 4" : undefined} dot={dotFor(s.marker, s.color)}
                  activeDot={{ r: 4.5, strokeWidth: 2, stroke: "#ffffff" }} connectNulls={false} isAnimationActive={false} />
              ))}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="table-scroll tall">{table}</div>
      )}
    </figure>
  );
}

/** Horizontal comparison bars (lower is better for error metrics). */
export function MetricBars({ rows, unit = "" }: { rows: Array<{ label: ReactNode; value: number | null | undefined; kind: "model" | "baseline" | "best" }>; unit?: string }) {
  const { intl } = useT();
  const max = Math.max(...rows.map((r) => r.value ?? 0), 1e-9);
  return (
    <div className="bars">
      {rows.map((r, i) => (
        <div className="bar-row" key={i}>
          <span className="bar-label">{r.label}</span>
          <span className="bar-track" aria-hidden="true">
            <span className={`bar-fill ${r.kind}`} style={{ width: `${r.value ? Math.max(3, (r.value / max) * 100) : 0}%`, display: "block" }} />
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
        <div key={y} style={{ display: "contents" }} role="row">
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
