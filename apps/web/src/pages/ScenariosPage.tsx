/**
 * Simulasi Skenario: saved scenarios (left) and results (right, sticky on desktop): a scenario run
 * or a side-by-side comparison against the no-assumption baseline, with a link to the comparison
 * report. A result always shows the assumptions it was computed with, and is labelled stale as soon
 * as the saved scenario no longer matches them (edited or archived), with a way to run it again
 * (lib/scenario-stale.ts; B-P1-1). Plan limits and role rules are shown before they block an action.
 */
import type { ForecastRun, ForecastRunList, HistoryResponse, ModelList, OrganizationDetail, Scenario, ScenarioCompareResponse } from "../api/types";
import { Archive, FileText, GitCompare, LoaderCircle, Lock, Pencil, Play, Plus, RotateCcw, SlidersHorizontal, TriangleAlert } from "lucide-react";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { api, ApiError } from "../api/client";
import { useResource } from "../api/hooks";
import { labelFor } from "../api/product-types";
import { useAuth } from "../auth";
import { MARKERS, SERIES_COLORS, TimeSeriesChart, type SeriesSpec } from "../components/charts";
import { ForecastRunView, methodLabel, type Level } from "../components/forecast";
import { IllScenarios } from "../components/illustrations";
import { useToast } from "../components/toast";
import { Badge, Button, Callout, ConfirmDialog, EmptyState, ErrorState, KindChip, PageHeader, Panel, Skeleton, Stat, TableScroll, focusHeading, scrollToResult } from "../components/ui";
import { dateOnly, fmtNum, fmtSigned, NA, nextPeriods, period, rangeText } from "../format";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/market.css"; // page styles, loaded with this lazy chunk (not by the public site)
import "../styles/flows.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { LeaveGuard } from "../components/leave-guard";
import { compareStaleness, runStaleness } from "../lib/scenario-stale";
import { fetchAllScenarios } from "../lib/scenario-list";
import { emptyScenario, ScenarioEditor, scenarioFor, usedInputsOf, type ScenarioMap } from "./ForecastPage";

/**
 * Comparison results are cached for the tab so Back/reload restores them without a new (counted)
 * comparison. The entry carries the inputs it was computed with (the response's `scenarios[].inputs`
 * and `horizon`), so a restored comparison is checked against the saved scenarios like a fresh one
 * and labelled stale when they changed (B-P1-1: it used to come back silently with old values).
 */
const cacheKey = (ids: string[]) => `icp.compare.${[...ids].sort().join(",")}`;
function readCache(ids: string[]): ScenarioCompareResponse | null {
  try { const v = sessionStorage.getItem(cacheKey(ids)); return v ? (JSON.parse(v) as ScenarioCompareResponse) : null; } catch { return null; }
}
function writeCache(ids: string[], v: ScenarioCompareResponse): void {
  try { sessionStorage.setItem(cacheKey(ids), JSON.stringify(v)); } catch { /* per-tab convenience only */ }
}

// Validated with the dataviz palette check next to the ICP blue; status red is never a series colour.
const COLORS = [SERIES_COLORS.s1, SERIES_COLORS.s2, SERIES_COLORS.s3, SERIES_COLORS.s4];
const INPUTS = ["brent", "dubai", "wti", "usd_idr"] as const;
/** Cards rendered before "show all" (large plans allow up to 1,000 saved scenarios). */
const LIST_SHOWN = 50;

type Inputs = Partial<Record<string, Partial<Record<string, number>>>> | null | undefined;
type Compared = { id: string; name: string; forecast_id: string; inputs: Record<string, Record<string, number>> };
type CompareRow = { period: string; kind: string; baseline: number; scenarios: Record<string, { prediction: number; delta_vs_baseline: number; conditional_on_scenario: boolean }> };

function toMap(inputs: Scenario["inputs"]): ScenarioMap {
  const m = emptyScenario();
  for (const k of INPUTS) m[k] = { ...(inputs[k] ?? {}) };
  return m;
}

/** One line per input, e.g. "Brent 70,00 USD/bbl (Okt 2026 s.d. Des 2026)" or "Brent: Okt 2026 70,00 · Nov 2026 75,00 USD/bbl". */
function useInputLines(): (inputs: Inputs) => string[] {
  const { t, locale, intl } = useT();
  return (inputs) => {
    const lines: string[] = [];
    for (const k of INPUTS) {
      const m = inputs?.[k] ?? {};
      const months = Object.keys(m).filter((p) => typeof m[p] === "number").sort();
      if (!months.length) continue;
      const digits = k === "usd_idr" ? 0 : 2;
      const unit = k === "usd_idr" ? t("forecast.placeholderFx") : t("forecast.placeholderUsd");
      const label = t(`forecast.scenarioInput.${k}` as Key);
      const values = months.map((p) => m[p] as number);
      const consecutive = months.every((p, i) => i === 0 || nextPeriods(months[i - 1]!, 1)[0] === p);
      if (months.length > 1 && consecutive && values.every((v) => v === values[0])) {
        lines.push(t("scen.inputAll", { input: label, value: fmtNum(values[0], intl, digits), unit, range: rangeText(period(months[0], locale), period(months.at(-1), locale), locale) }));
      } else {
        lines.push(`${label}: ${months.map((p) => t("scen.inputMonth", { month: period(p, locale), value: fmtNum(m[p], intl, digits) })).join(" · ")} ${unit}`);
      }
    }
    return lines;
  };
}

/** Colour and marker shape of scenario #i, as drawn in the comparison chart (never colour alone). */
export function ScenarioSwatch({ i }: { i: number }) {
  const color = COLORS[i] ?? COLORS[0]!;
  const shape = MARKERS[i] ?? "circle";
  return (
    <svg className="scen-swatch" width="24" height="14" viewBox="0 0 24 14" aria-hidden="true" focusable="false">
      <line x1="0" x2="24" y1="7" y2="7" stroke={color} strokeWidth="2" strokeDasharray="4 3" />
      {shape === "circle" && <circle cx="12" cy="7" r="4.5" fill={color} />}
      {shape === "square" && <rect x="7.5" y="2.5" width="9" height="9" fill={color} />}
      {shape === "triangle" && <path d="M12 1.5 L17 11.5 H7 Z" fill={color} />}
      {shape === "diamond" && <path d="M12 1 L18 7 L12 13 L6 7 Z" fill={color} />}
    </svg>
  );
}

function ScenarioForm({ origin, initial, onDone, onCancel, maxHorizon, usedInputs, methodName }:
  { origin: string; initial?: Scenario; onDone: (name: string) => void; onCancel: () => void; maxHorizon: number; usedInputs?: ReadonlySet<string> | null; methodName?: string }) {
  const { t, locale } = useT();
  const [name, setName] = useState(initial?.name ?? "");
  const [description, setDescription] = useState(initial?.description ?? "");
  const [horizon, setHorizon] = useState(Math.min(initial?.horizon ?? 3, maxHorizon));
  const [inputs, setInputs] = useState<ScenarioMap>(initial ? toMap(initial.inputs) : emptyScenario());
  const [inputsValid, setInputsValid] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [nameTouched, setNameTouched] = useState(false);
  const periods = useMemo(() => nextPeriods(origin, horizon), [origin, horizon]);
  const initialSnapshot = useMemo(() => JSON.stringify([initial?.name ?? "", initial?.description ?? "", initial?.horizon ?? 3, initial ? toMap(initial.inputs) : emptyScenario()]), [initial]);
  const [saved, setSaved] = useState(false);
  const dirty = !saved && JSON.stringify([name, description, horizon, inputs]) !== initialSnapshot;
  const nameRef = useRef<HTMLInputElement>(null);
  useEffect(() => { nameRef.current?.focus(); }, []);
  // Validation in words: what will (not) be saved, before the server is asked.
  const kept = scenarioFor(inputs, periods);
  const keptCount = INPUTS.reduce((n, k) => n + Object.keys(kept[k]).length, 0);
  const inPeriods = new Set(periods);
  const dropped = [...new Set(INPUTS.flatMap((k) => Object.keys(inputs[k]).filter((p) => !inPeriods.has(p))))].sort();
  const nameMissing = !name.trim();

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (nameMissing) { setNameTouched(true); nameRef.current?.focus(); return; }
    if (busy || !inputsValid) return; // no double submission while the server is answering
    setBusy(true); setError(null);
    try {
      const body = { name, description: description || null, horizon, inputs: kept };
      if (initial) await api(`/scenarios/${initial.id}`, { method: "POST", json: body });
      else await api("/scenarios", { method: "POST", json: body });
      setBusy(false);
      setSaved(true);
      onDone(name.trim());
    } catch (err) { setError(err); setBusy(false); }
  };
  return (
    <form onSubmit={submit} className="stack scen-form" noValidate>
      <LeaveGuard dirty={dirty} />
      <div className="scen-form-grid">
        <div className="field">
          <label className="field-label" htmlFor="scen-name">{t("scen.name")}</label>
          <input id="scen-name" ref={nameRef} className="input" value={name} onChange={(e) => setName(e.target.value)} onBlur={() => setNameTouched(true)} required maxLength={120}
            aria-invalid={nameTouched && nameMissing ? true : undefined} aria-describedby={nameTouched && nameMissing ? "scen-name-err" : undefined} />
          {nameTouched && nameMissing && <span className="field-error" id="scen-name-err">{t("scen.nameRequired")}</span>}
        </div>
        <label className="field"><span className="field-label">{t("scen.description")} <span className="muted">({t("common.optional")})</span></span><input className="input" value={description} onChange={(e) => setDescription(e.target.value)} maxLength={1000} /></label>
        <div className="field"><span className="field-label" id="scen-horizon-label">{t("forecast.horizon")}</span>
          <div className="horizon-pick" role="group" aria-labelledby="scen-horizon-label" aria-describedby="scen-horizon-hint">{[1, 2, 3, 4, 5, 6].map((h) => <button key={h} type="button" aria-pressed={horizon === h} disabled={h > maxHorizon} onClick={() => setHorizon(h)}>{h}</button>)}</div>
          <span className="field-hint" id="scen-horizon-hint">{t("scen.hintHorizonPlan", { n: maxHorizon })}</span></div>
      </div>
      <div className="stack-sm">
        <div><div className="section-title flush">{t("scen.inputs")}</div><p className="small muted">{t("scen.inputsHelp")} {t("scen.originNote", { origin: period(origin, locale, true) })}</p></div>
        <ScenarioEditor periods={periods} value={inputs} onChange={setInputs} onValidity={setInputsValid} usedInputs={usedInputs} methodName={methodName} />
        <div aria-live="polite" className="stack-sm">
          {inputsValid && keptCount === 0 && <p className="note-line">{t("scen.hintNoInputs")}</p>}
          {dropped.length > 0 && <p className="note-line">{t("scen.hintDropped", { months: dropped.map((p) => period(p, locale)).join(", "), n: horizon })}</p>}
        </div>
      </div>
      {!inputsValid && <span className="field-error" role="alert">{t("forecast.scenarioInvalid")}</span>}
      <ErrorState error={error} />
      <div className="row"><Button type="submit" variant="primary" loading={busy} disabled={!inputsValid}>{initial ? t("scen.update") : t("scen.create")}</Button><Button type="button" variant="ghost" onClick={onCancel}>{t("common.cancel")}</Button></div>
    </form>
  );
}

function AssumptionsUsed({ inputs }: { inputs: Inputs }) {
  const { t } = useT();
  const lines = useInputLines()(inputs);
  return (
    <div className="scen-used">
      <div className="scen-used-title">{t("scen.assumptionsUsed")}</div>
      {lines.length ? <ul>{lines.map((l) => <li key={l}>{l}</li>)}</ul> : <p className="small muted">{t("scen.noInputs")}</p>}
    </div>
  );
}

/** Short repeat of the stale warning next to the numbers it applies to (chart, table), for phones and long scrolls. */
function StaleTag() {
  const { t } = useT();
  return <p className="scen-stale-tag"><TriangleAlert size={14} aria-hidden="true" /><span>{t("scen.staleTag")}</span></p>;
}

function CompareView({ cmp, history, stale = false }: { cmp: ScenarioCompareResponse; history: HistoryResponse | null; stale?: boolean }) {
  const { t, locale, intl } = useT();
  const inputLines = useInputLines();
  const scenarios = cmp.scenarios as unknown as Compared[];
  const rows = cmp.rows as unknown as CompareRow[];
  const chartRows = useMemo(() => {
    const hist = (history?.points ?? []).filter((p) => p.period <= cmp.forecast_origin).slice(-12).map((p) => ({ period: p.period, actual: p.icp_price ?? undefined }) as Record<string, unknown>);
    const last = hist.at(-1);
    if (last && typeof last.actual === "number") { last.baseline = last.actual; scenarios.forEach((s) => { last[`s_${s.id}`] = last.actual; }); }
    const fc = rows.map((r) => { const o: Record<string, unknown> = { period: r.period, baseline: r.baseline, kind: r.kind }; scenarios.forEach((s) => { o[`s_${s.id}`] = r.scenarios[s.id]?.prediction; }); return o; });
    return [...hist, ...fc];
  }, [history, rows, scenarios, cmp.forecast_origin]);
  const series: SeriesSpec[] = [
    { key: "actual", label: t("forecast.chartActual"), color: SERIES_COLORS.icp, width: 2.25 },
    { key: "baseline", label: t("scen.baseline"), color: SERIES_COLORS.forecast, dashed: true },
    ...scenarios.map((s, i) => ({ key: `s_${s.id}`, label: s.name, color: COLORS[i] ?? COLORS[0]!, dashed: true, marker: MARKERS[i] ?? "circle" })),
  ];
  return (
    <div className="stack">
      <div className="stat-grid">
        <Stat label={t("forecast.origin")} value={period(cmp.forecast_origin, locale, true)} />
        <Stat label={t("forecast.issueDate")} value={dateOnly(cmp.issue_date, intl)} />
        <Stat label={t("forecast.horizon")} value={t("forecast.horizonMonths", { n: cmp.horizon })} />
        <Stat label={t("common.method")} value={methodLabel(cmp.method, t)} />
      </div>
      <ul className="scen-legend" aria-label={t("scen.assumptionsUsed")}>
        <li><span className="scen-legend-key"><svg className="scen-swatch" width="24" height="14" viewBox="0 0 24 14" aria-hidden="true" focusable="false"><line x1="0" x2="24" y1="7" y2="7" stroke={SERIES_COLORS.forecast} strokeWidth="2" strokeDasharray="4 3" /></svg><strong>{t("scen.baselineShort")}</strong></span>
          <span className="scen-legend-inputs">{t("scen.noInputs")}</span></li>
        {scenarios.map((s, i) => {
          const lines = inputLines(s.inputs);
          return (
            <li key={s.id}><span className="scen-legend-key"><ScenarioSwatch i={i} /><strong>{s.name}</strong></span>
              <span className="scen-legend-inputs">{lines.length ? lines.join(" · ") : t("scen.noInputs")}</span></li>
          );
        })}
      </ul>
      {stale && <StaleTag />}
      <TimeSeriesChart ariaLabel={t("scen.chartTitle")} data={chartRows} series={series} zones={rows.length ? [{ from: rows[0]!.period, to: rows.at(-1)!.period, kind: "forecast" }] : []} />
      <p className="xs muted">{t("scen.markerNote")}</p>
      {stale && <StaleTag />}
      <div className="table-frame">
        <TableScroll label={t("scen.compareTitle")}>
          <table className="table rc scen-table">
            <caption className="sr-only">{t("scen.compareTitle")} (USD/bbl)</caption>
            <thead><tr><th>{t("common.month")}</th><th>{t("forecast.kindLegend")}</th><th className="num">{t("scen.baselineShort")} <span className="th-unit">USD/bbl</span></th>
              {scenarios.map((s, i) => <th key={s.id} className="num"><span className="scen-th"><ScenarioSwatch i={i} />{s.name}</span><span className="th-note">{t("scen.deltaCol")}</span></th>)}</tr></thead>
            <tbody>{rows.map((r) => (
              <tr key={r.period}>
                <td className="primary-cell rc-title">{period(r.period, locale)}</td>
                <td data-label={t("forecast.kindLegend")}><KindChip kind={r.kind as "future_forecast"} /></td>
                <td className="num strong" data-label={`${t("scen.baselineShort")} (USD/bbl)`}>{fmtNum(r.baseline, intl)}</td>
                {scenarios.map((s) => {
                  const v = r.scenarios[s.id];
                  return (
                    <td key={s.id} className="num" data-label={`${s.name} (USD/bbl)`}>
                      {v ? <><span className="strong">{fmtNum(v.prediction, intl)}</span>
                        <span className="sub">{fmtSigned(v.delta_vs_baseline, intl)} {t("scen.delta")}{v.conditional_on_scenario ? ` · ${t("forecast.conditional")}` : ""}</span></> : NA}
                    </td>
                  );
                })}
              </tr>
            ))}</tbody>
          </table>
        </TableScroll>
      </div>
      <Callout tone="info" title={t("scen.compareNote")}>{t("scen.compareNoteBody")}</Callout>
    </div>
  );
}

export default function ScenariosPage() {
  const { t, locale, intl } = useT();
  // Customer-only route (the router keeps guests and operators out).
  const { isUser, isOperator, principal, hasRole } = useAuth();
  const org = useResource((s) => isUser ? api<OrganizationDetail>("/org", { signal: s }) : Promise.resolve(null), [isUser]);
  // Every saved scenario, page by page (plans allow 100 or 1,000; W2-R5).
  const list = useResource((s) => fetchAllScenarios(s), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => api<ForecastRunList>("/forecasts", { query: { limit: 1 }, signal: s }).catch(() => null), []);
  const inputLines = useInputLines();
  const [editing, setEditing] = useState<Scenario | "new" | null>(null);
  // The comparison set lives in the URL (?bandingkan=id,id) so Back from the report page and reload keep it.
  const [params, setParams] = useSearchParams();
  const compareIds = useMemo(() => (params.get("bandingkan") ?? "").split(",").map((x) => x.trim()).filter(Boolean).slice(0, 4), [params]);
  const [selected, setSelected] = useState<string[]>(compareIds);
  const [cmp, setCmp] = useState<ScenarioCompareResponse | null>(() => (compareIds.length ? readCache(compareIds) : null));
  const models = useResource((s) => api<ModelList>("/models", { signal: s }).catch(() => null), []);
  const { used, prod } = useMemo(() => usedInputsOf(models.data), [models.data]);
  const [runResult, setRunResult] = useState<{ run: ForecastRun; scenario: Scenario } | null>(null);
  const [level, setLevel] = useState<Level>("0.8");
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const resultsRef = useRef<HTMLDivElement>(null);
  const editorRef = useRef<HTMLDivElement>(null);
  const [archiving, setArchiving] = useState<Scenario | null>(null);
  const [missingLink, setMissingLink] = useState<string[] | null>(null);
  const [showAll, setShowAll] = useState(false);
  const { show } = useToast();

  const lastOfficial = history.data?.points.filter((p) => p.icp_price != null).at(-1)?.period ?? null;
  const origin = latest.data?.items[0]?.forecast_origin ?? lastOfficial;
  const ent = org.data?.entitlements;
  const analyst = hasRole("analyst");
  const items = useMemo(() => list.data?.items ?? [], [list.data]);
  const listComplete = !!list.data?.complete;
  const listFailed = !!list.error && !list.data;
  // The plan could not be read (the list could: its page is up). Say so, never drop the limits silently (W2-R4).
  const orgFailed = isUser && !org.data && !!org.error;
  const usedCount = org.data?.saved_scenarios ?? items.length;
  const atLimit = isUser && !!ent && usedCount >= ent.saved_scenarios_max;
  // A readable scenario list proves the plan includes scenarios (GET /scenarios requires it), so an
  // unreadable plan does not block "new scenario": the server still enforces the plan's limit.
  const canCreate = analyst && (isOperator || !!ent?.scenarios || (orgFailed && !!list.data)) && !atLimit;
  const working = busy === "compare" || !!busy?.startsWith("run-");

  // Staleness is derived from the saved scenarios on every render: an edit, an archive or a
  // reload of the list updates the label without any extra bookkeeping. A scenario missing from a
  // list that could not be read to the end is unknown, not archived.
  const listReady = list.data ? items : undefined;
  const listed = (id: string) => { const s = items.find((x) => x.id === id); return s ?? (listComplete ? null : undefined); };
  const runStale = runResult ? runStaleness(runResult.run, listReady ? listed(runResult.scenario.id) : undefined) : null;
  const cmpStale = cmp ? compareStaleness(cmp, listReady, { complete: listComplete, latestOrigin: lastOfficial }) : null;

  // Selection from the URL (?bandingkan=) or from before an archive: only scenarios that are still
  // listed can be compared, so the compare button never counts or sends an archived one (W2-R2).
  useEffect(() => {
    if (!list.data?.complete) return;
    const ids = new Set(list.data.items.map((s) => s.id));
    setSelected((sel) => (sel.every((id) => ids.has(id)) ? sel : sel.filter((id) => ids.has(id))));
  }, [list.data]);

  // Move to a result the reader just asked for (run, compare); a comparison restored on page load
  // stays where it is and does not pull the page down.
  const focusResult = useRef(false);
  useEffect(() => {
    if (!focusResult.current || (!cmp && !runResult)) return;
    focusResult.current = false;
    const el = resultsRef.current;
    if (!el) return;
    scrollToResult(el);
    focusHeading(el);
  }, [cmp, runResult]);
  useEffect(() => { if (editing) scrollToResult(editorRef.current, "nearest"); }, [editing]);

  // Restore a shared/reloaded comparison that is not cached in this tab (runs the comparison once),
  // after the list has loaded: a link naming archived scenarios is explained, not sent to fail.
  const restored = useRef(false);
  useEffect(() => {
    if (restored.current || !compareIds.length || cmp || !list.data || !analyst) return;
    restored.current = true;
    // Only a complete list can tell that a linked scenario is gone (archived).
    const active = compareIds.filter((id) => !listComplete || items.some((s) => s.id === id));
    if (active.length < compareIds.length) { setMissingLink(active); return; }
    void (async () => {
      setBusy("compare");
      try { const r = await api<ScenarioCompareResponse>("/scenarios/compare", { method: "POST", json: { scenario_ids: compareIds } }); writeCache(compareIds, r); setCmp(r); }
      catch (e) { setError(e); }
      finally { setBusy(null); }
    })();
  }, [compareIds, cmp, list.data, items, listComplete, analyst]);

  const runCompare = async (ids: string[]) => {
    const r = await api<ScenarioCompareResponse>("/scenarios/compare", { method: "POST", json: { scenario_ids: ids } });
    writeCache(ids, r);
    focusResult.current = true;
    setRunResult(null); setCmp(r); setMissingLink(null);
    setSelected(ids);
    const next = new URLSearchParams(params); next.set("bandingkan", ids.join(",")); setParams(next);
    list.reload();
  };
  const runScenario = async (s: Scenario) => {
    const r = await api<ForecastRun>(`/scenarios/${s.id}/run`, { method: "POST" });
    focusResult.current = true;
    setCmp(null); setRunResult({ run: r, scenario: s });
    if (params.has("bandingkan")) { const next = new URLSearchParams(params); next.delete("bandingkan"); setParams(next, { replace: true }); }
    list.reload();
  };
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };

  const workingText = busy === "compare" ? t("scen.comparing", { n: selected.length || compareIds.length })
    : busy?.startsWith("run-") ? t("scen.running", { name: items.find((s) => `run-${s.id}` === busy)?.name ?? "" }) : "";
  const reportIds = cmpStale?.activeIds ?? [];
  const reportHref = `/laporan?jenis=perbandingan&skenario=${encodeURIComponent(reportIds.join(","))}`;
  const currentRunScenario = runResult ? items.find((s) => s.id === runResult.scenario.id) ?? null : null;
  const cmpIds = cmp ? (cmp.scenarios as unknown as Compared[]).map((c) => c.id) : [];
  // Large lists: the first cards plus everything selected or shown in the results; the rest on request.
  const visibleItems = showAll || items.length <= LIST_SHOWN ? items
    : items.filter((s, i) => i < LIST_SHOWN || selected.includes(s.id) || cmpIds.includes(s.id) || runResult?.scenario.id === s.id);

  // Scenario edits/archives (the saved scenarios no longer match) versus only newer official data.
  const cmpScenarioStale = !!cmpStale && (cmpStale.changed.length > 0 || cmpStale.archived.length > 0 || cmpStale.horizonChanged);
  // The report re-runs the comparison on the server: offered only for a comparison verified as current.
  const results = cmp ? (
    <Panel title={t("scen.compareTitle")} icon={GitCompare}
      actions={reportIds.length > 0 && !!list.data && !cmpStale?.stale ? <Link className="btn sm" to={reportHref}><FileText size={15} aria-hidden="true" />{t("scen.reportLink")}</Link> : undefined}>
      <div className="stack">
        {cmpStale?.stale && (
          <Callout tone="warn" title={cmpScenarioStale ? t("scen.staleCompareTitle") : t("scen.staleOriginTitle")}
            action={analyst && cmpStale.activeIds.length > 0 ? <Button size="sm" variant="primary" icon={RotateCcw} loading={busy === "compare"} disabled={!!busy && busy !== "compare"}
              onClick={() => void act("compare", () => runCompare(cmpStale.activeIds))}>
              {cmpStale.archived.length ? t("scen.recompareRemaining", { n: cmpStale.activeIds.length }) : t("scen.recompare")}</Button> : undefined}>
            {cmpStale.changed.length > 0 && <p>{t("scen.staleChanged", { list: cmpStale.changed.map((s) => `"${s.name}"`).join(", ") })}</p>}
            {cmpStale.archived.length > 0 && <p>{t("scen.staleArchived", { list: cmpStale.archived.map((s) => `"${s.name}"`).join(", ") })}</p>}
            {cmpStale.horizonChanged && <p>{t("scen.staleHorizon")}</p>}
            {cmpStale.newerOrigin && <p>{t("scen.staleOrigin", { latest: period(cmpStale.newerOrigin, locale, true), origin: period(cmp.forecast_origin, locale, true) })}</p>}
            <p>{cmpScenarioStale ? t("scen.staleCompareBody") : t("scen.staleCompareBodyData")}</p>
          </Callout>
        )}
        {/* Without the saved scenarios nothing can be checked: say so instead of implying it is current. */}
        {listFailed && <Callout tone="info" title={t("scen.unverifiedTitle")}>{t("scen.unverifiedBody")}</Callout>}
        <CompareView cmp={cmp} history={history.data} stale={!!cmpStale?.stale} />
      </div>
    </Panel>
  ) : runResult ? (
    <Panel title={t("scen.runResult", { name: currentRunScenario?.name ?? runResult.scenario.name })} icon={Play} subtitle={t("scen.runResultSub", { date: dateOnly(runResult.run.issue_date ?? null, intl) })}>
      <div className="stack">
        {runStale?.stale && (
          <Callout tone="warn" title={runStale.reason === "archived" ? t("scen.staleArchivedTitle") : t("scen.staleTitle")}
            action={runStale.reason === "changed" && analyst && currentRunScenario ? <Button size="sm" variant="primary" icon={RotateCcw} loading={busy === `run-${currentRunScenario.id}`} disabled={!!busy && busy !== `run-${currentRunScenario.id}`}
              onClick={() => void act(`run-${currentRunScenario.id}`, () => runScenario(currentRunScenario))}>{t("scen.rerun")}</Button> : undefined}>
            {runStale.reason === "archived" ? t("scen.staleArchivedRun") : t("scen.staleRunBody")}
          </Callout>
        )}
        <AssumptionsUsed inputs={runResult.run.scenario} />
        <ForecastRunView run={runResult.run} history={history.data?.points ?? []} level={level} onLevel={setLevel} />
      </div>
    </Panel>
  ) : (
    <Panel title={t("scen.resultsTitle")} icon={GitCompare}>
      {missingLink ? (
        <Callout tone="info" title={t("scen.linkMissing")}
          action={analyst && missingLink.length > 0 ? <Button size="sm" variant="primary" loading={busy === "compare"} onClick={() => void act("compare", () => runCompare(missingLink))}>{t("scen.recompareRemaining", { n: missingLink.length })}</Button> : undefined} />
      ) : working ? null : (
        <EmptyState title={t("scen.resultsEmpty")}>{t("scen.resultsEmptyBody")}</EmptyState>
      )}
    </Panel>
  );

  return (
    <div className="page">
      <PageHeader eyebrow={t("scen.eyebrow")} icon={SlidersHorizontal} title={t("scen.title")} description={t("scen.desc")}
        actions={<Button variant="primary" icon={Plus} onClick={() => setEditing("new")} disabled={!canCreate || !origin}
          title={!analyst ? t("scen.needsAnalyst") : atLimit ? t("scen.atLimitShort") : undefined}>{t("scen.new")}</Button>} />

      {ent && (
        <div className={`scen-usage${atLimit ? " at-limit" : ""}`}>
          <div className="scen-usage-line">
            <span>{t("scen.usage", { used: usedCount, n: ent.saved_scenarios_max, plan: labelFor(ent, locale) })}</span>
            <span className="scen-meter" aria-hidden="true"><span style={{ width: `${Math.min(100, (usedCount / Math.max(1, ent.saved_scenarios_max)) * 100)}%` }} /></span>
            <span className="muted">{t("scen.usageHorizon", { n: principal.horizonMax })}</span>
          </div>
          {atLimit && <p className="scen-usage-note"><Lock size={14} aria-hidden="true" /><span>{t("scen.atLimit")} <Link to="/akun#paket">{t("scen.planLink")}</Link></span></p>}
        </div>
      )}
      {orgFailed && !listFailed && <ErrorState error={org.error} onRetry={org.reload} compact titles={org.error instanceof ApiError ? { [org.error.code]: t("scen.planUnknownTitle") } : undefined} />}
      {isUser && ent && !ent.scenarios && <Callout tone="warn">{t("scen.noPlan")}</Callout>}
      {isUser && !analyst && <Callout tone="info">{t("scen.viewerNote")}</Callout>}
      <ErrorState error={error} />

      {editing && origin && (
        <div ref={editorRef} className="anchor">
          <Panel title={editing === "new" ? t("scen.new") : t("scen.editing", { name: editing.name })} icon={Pencil}>
            <ScenarioForm key={editing === "new" ? "new" : editing.id} origin={origin} initial={editing === "new" ? undefined : editing} maxHorizon={principal.horizonMax} usedInputs={used} methodName={prod ? methodLabel(prod.method, t) : undefined}
              onDone={(name) => { setEditing(null); list.reload(); org.reload(); show(t("toast.scenarioSaved", { name }), "good"); }} onCancel={() => setEditing(null)} />
          </Panel>
        </div>
      )}

      <div className="scen-layout">
        <Panel title={t("scen.listTitle")} icon={SlidersHorizontal} subtitle={t("scen.select")} className="scen-list-panel"
          footer={items.length > 0 ? (
            <div className="scen-compare-bar">
              <span>{analyst ? (selected.length ? t("scen.selectedCount", { n: selected.length }) : t("scen.compareHint")) : t("scen.needsAnalyst")}</span>
              <Button size="sm" variant={selected.length ? "primary" : "secondary"} icon={GitCompare} disabled={selected.length === 0 || !!busy || !analyst} loading={busy === "compare"}
                onClick={() => void act("compare", () => runCompare(selected))}>{t("scen.compare")} ({selected.length})</Button>
            </div>
          ) : undefined}>
          {list.loading && !list.data ? <div role="status" aria-label={t("common.loading")} className="stack-sm"><Skeleton height={120} /><Skeleton height={120} /></div>
            : list.error ? <ErrorState error={list.error} onRetry={list.reload} />
            : !items.length ? (
              <EmptyState illustration={<IllScenarios size={160} />} title={t("scen.empty")} action={canCreate && origin ? <Button variant="primary" icon={Plus} onClick={() => setEditing("new")}>{t("scen.new")}</Button> : undefined}>{t("scen.emptyBody")}</EmptyState>
            ) : (
              <><ul className="scen-list">
                {visibleItems.map((s) => {
                  const idx = selected.indexOf(s.id);
                  const n = Object.values(s.inputs).reduce((a, m) => a + Object.keys(m).length, 0);
                  const lines = inputLines(s.inputs);
                  const cmpIdx = cmpIds.indexOf(s.id);
                  const shown = (runResult?.scenario.id === s.id) || cmpIdx >= 0;
                  // Colour and marker follow the scenario as drawn in the comparison on screen (never the
                  // selection order, which can change afterwards); with no comparison shown, the order
                  // the next comparison will give it (W2-R7).
                  const swatch = cmp ? cmpIdx : idx;
                  const checkId = `scen-check-${s.id}`;
                  return (
                    <li key={s.id} className={`scenario-card${idx >= 0 ? " selected" : ""}`}>
                      <label className="scen-check-hit"><input id={checkId} type="checkbox" className="scen-check" checked={idx >= 0} aria-label={t("scen.selectFor", { name: s.name })} disabled={idx < 0 && selected.length >= 4}
                        onChange={(e) => setSelected(e.target.checked ? [...selected, s.id].slice(0, 4) : selected.filter((x) => x !== s.id))} /></label>
                      <div className="scen-card-body">
                        <div className="scen-card-head">
                          <label htmlFor={checkId} className="scen-card-name">{s.name}</label>
                          {swatch >= 0 && <span className="scen-card-key"><ScenarioSwatch i={swatch} /><span className="sr-only">{t("scen.compareOrder", { n: swatch + 1 })}</span></span>}
                        </div>
                        <div className="chip-row"><Badge tone="neutral">{t("forecast.horizonMonths", { n: s.horizon })}</Badge><Badge tone="brand">{t("scen.inputsSummary", { n })}</Badge>
                          {shown && <Badge tone="outline">{t("scen.shown")}</Badge>}</div>
                        {s.description && <p className="small muted">{s.description}</p>}
                        <ul className="scen-card-inputs">{lines.length ? lines.map((l) => <li key={l}>{l}</li>) : <li className="muted">{t("scen.noInputs")}</li>}</ul>
                        <p className="xs muted">{s.last_forecast_id ? t("scen.hasRun") : t("scen.never")} · {t("scen.lastActivity", { date: dateOnly(s.updated_at, intl) })}</p>
                        <div className="scen-card-actions">
                          <Button size="sm" variant="primary" icon={Play} loading={busy === `run-${s.id}`} disabled={!!busy || !analyst}
                            aria-label={`${t("scen.run")}: ${s.name}`} onClick={() => void act(`run-${s.id}`, () => runScenario(s))}>{t("scen.run")}</Button>
                          <Button size="sm" variant="ghost" icon={Pencil} onClick={() => setEditing(s)} disabled={!analyst || !origin} aria-label={`${t("scen.edit")}: ${s.name}`} title={t("scen.edit")} />
                          <Button size="sm" variant="ghost" icon={Archive} aria-label={`${t("scen.archive")}: ${s.name}`} title={t("scen.archive")} disabled={!analyst}
                            loading={busy === `arch-${s.id}`} onClick={() => setArchiving(s)} />
                        </div>
                      </div>
                    </li>
                  );
                })}
              </ul>
              {items.length > LIST_SHOWN && (
                <div className="scen-more"><Button size="sm" variant="ghost" aria-expanded={showAll} onClick={() => setShowAll((v) => !v)}>{showAll ? t("common.showLess") : t("common.showAll", { n: items.length })}</Button></div>
              )}</>
            )}
        </Panel>
        <div ref={resultsRef} id="hasil" className="anchor scen-results" role="region" aria-label={t("scen.resultsRegion")} aria-busy={working || undefined}>
          {working && (
            <div className="scen-working" role="status"><LoaderCircle size={16} className="spin" aria-hidden="true" /><span><strong>{workingText}</strong>{(cmp || runResult) && <span className="xs muted"> {t("scen.workingNote")}</span>}</span></div>
          )}
          <div className={working && (cmp || runResult) ? "scen-dim" : undefined} inert={working && (cmp || runResult) ? true : undefined}>
            {results}
          </div>
        </div>
      </div>
      <ConfirmDialog open={!!archiving} busy={!!archiving && busy === `arch-${archiving.id}`}
        title={t("scen.archiveConfirmTitle", { name: archiving?.name ?? "" })} confirmLabel={t("scen.archiveConfirm")}
        onCancel={() => setArchiving(null)}
        onConfirm={() => {
          const s = archiving;
          if (!s) return;
          void act(`arch-${s.id}`, async () => {
            await api(`/scenarios/${s.id}/archive`, { method: "POST" });
            // Only after the server confirmed: drop it from the selection. A result that used it stays
            // on screen labelled stale (archived), never silently presented as current.
            setSelected((sel) => sel.filter((x) => x !== s.id));
            list.reload(); org.reload();
            show(t("toast.scenarioArchived", { name: s.name }), "good");
          }).finally(() => setArchiving(null));
        }}>
        {t("scen.archiveConfirmBody")}
      </ConfirmDialog>
    </div>
  );
}
