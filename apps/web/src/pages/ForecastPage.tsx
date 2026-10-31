/**
 * Proyeksi ICP: run a forecast (horizon + optional scenario) at the top, then read the run on screen
 * in four tabs: Hasil (facts, assumption note, chart, table, export), Asumsi (per-month inputs and
 * their sources), Akurasi & metode (method, per-point backtest accuracy, limits) and Riwayat
 * terbitan (past issues). URL state: `?proyeksi=<id>` (issue on screen), `?tab=` (push: Back returns
 * to the previous tab) and `?tingkat=50|80|95` (interval level, replace).
 *
 * Stale guard: the form follows the issue on screen until the reader edits it. When the horizon or
 * the scenario in the form differ from those of the run on screen, a notice says so above the
 * results with a run button, so edited inputs never sit next to old results unmarked; when only
 * assumption values changed it names them. An issue from an earlier origin than a new run would use
 * is not compared with the form: a notice says so and names any scenario months the form could not
 * take over. While a run is computing, the previous result stays visible, dimmed and labelled; once
 * the server answers, the new run is shown from that answer until its own read has loaded.
 */
import type { ForecastRun, ForecastRunList, HistoryResponse, ModelList, ModelVersion } from "../api/types";
import { ArrowRight, Info, Play, SlidersHorizontal } from "lucide-react";
import { useCallback, useEffect, useId, useMemo, useRef, useState, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { api, nullOn404 } from "../api/client";
import { useResource } from "../api/hooks";
import type { ResultReadFields } from "../api/product-types";
import { useAuth } from "../auth";
import { AssumptionNote, ForecastAssumptionsView, ForecastResultView, InputUsageNotes, MethodPanel, methodLabel, RunFailed, type Level } from "../components/forecast";
import { ProcessExplainer } from "../components/process";
import { IllNoForecast } from "../components/illustrations";
import {
  Badge, Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, SlowNotice, TableScroll, Tabs, focusHeading, scrollToResult, tabPanelProps,
} from "../components/ui";
import { dateOnly, fmtNum, NA, nextPeriods, period, rangeText } from "../format";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/market.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { checkNumber, formatForInput } from "../lib/number";

const INPUTS = ["brent", "dubai", "wti", "usd_idr"] as const;
type Input = (typeof INPUTS)[number];
export type ScenarioMap = Record<Input, Record<string, number>>;

/** Plausible ranges: they catch a slipped decimal separator (955 instead of 95.5) before it reaches the model. */
export const INPUT_RANGE: Record<Input, { min: number; max: number; grouping: boolean; unit: "usd" | "fx" }> = {
  brent: { min: 10, max: 250, grouping: false, unit: "usd" },
  dubai: { min: 10, max: 250, grouping: false, unit: "usd" },
  wti: { min: 10, max: 250, grouping: false, unit: "usd" },
  usd_idr: { min: 5000, max: 30000, grouping: true, unit: "fx" },
};

const cellKey = (i: Input, p: string) => `${i}:${p}`;

/**
 * Scenario assumptions per month. Each input keeps the text the user typed (so "95," stays on
 * screen while typing) and is parsed locale-tolerantly ("95,5" and "95.5" both mean 95.5). Text
 * that is not a number, or a value outside the plausible range, shows an inline error and makes
 * the editor invalid; only valid numbers reach `onChange`.
 */
export function ScenarioEditor({ periods, value, onChange, onValidity, disabled, usedInputs, methodName }:
  { periods: string[]; value: ScenarioMap; onChange: (v: ScenarioMap) => void; onValidity?: (valid: boolean) => void; disabled?: boolean;
    /** Benchmarks the active method uses (from /models); other benchmark inputs are disabled with the reason. FX always applies. */
    usedInputs?: ReadonlySet<string> | null; methodName?: string }) {
  const { t, locale, intl } = useT();
  const uid = useId();
  const [raw, setRaw] = useState<Record<string, string>>(() => {
    const init: Record<string, string> = {};
    for (const i of INPUTS) for (const [p, v] of Object.entries(value[i])) init[cellKey(i, p)] = formatForInput(v, intl);
    return init;
  });
  const [touched, setTouched] = useState<Record<string, boolean>>({});
  const checks = useMemo(() => {
    const out: Record<string, ReturnType<typeof checkNumber>> = {};
    for (const p of periods) for (const i of INPUTS) out[cellKey(i, p)] = checkNumber(raw[cellKey(i, p)] ?? "", INPUT_RANGE[i]);
    return out;
  }, [periods, raw]);
  const valid = Object.values(checks).every((c) => c.error === null);
  useEffect(() => { onValidity?.(valid); }, [valid, onValidity]);

  const unused = (i: Input) => i !== "usd_idr" && !!usedInputs && !usedInputs.has(i);
  const unitLabel = (i: Input) => (INPUT_RANGE[i].unit === "fx" ? t("forecast.placeholderFx") : t("forecast.placeholderUsd"));
  const set = (i: Input, p: string, text: string) => {
    setRaw((r) => ({ ...r, [cellKey(i, p)]: text }));
    const c = checkNumber(text, INPUT_RANGE[i]);
    const next: ScenarioMap = { brent: { ...value.brent }, dubai: { ...value.dubai }, wti: { ...value.wti }, usd_idr: { ...value.usd_idr } };
    if (c.value === null) delete next[i][p]; else next[i][p] = c.value;
    onChange(next);
  };
  const message = (i: Input, k: string): string | null => {
    const c = checks[k];
    if (!c || c.error === null) return null;
    if (c.error === "invalid") return t("num.invalid");
    // Range errors wait for blur so typing "100" does not flash an error at "1".
    if (!touched[k]) return null;
    const r = INPUT_RANGE[i];
    return t("num.range", { min: fmtNum(r.min, intl, 0), max: fmtNum(r.max, intl, 0), unit: unitLabel(i) });
  };

  return (
    <div className="assump" role="group" aria-label={t("forecast.scenarioTitle")}>
      {usedInputs && INPUTS.some(unused) && <p className="field-hint" id={`${uid}-unused`}>{t("forecast.inputUnused", { method: methodName ?? "" })} ({INPUTS.filter(unused).map((i) => t(`forecast.scenarioInput.${i}` as Key)).join(", ")})</p>}
      <div className="assump-head" aria-hidden="true">
        <span>{t("common.month")}</span>
        {INPUTS.map((i) => <span key={i}>{t(`forecast.scenarioInput.${i}` as Key)} <span className="th-unit">{unused(i) ? t("forecast.inputUnusedShort") : unitLabel(i)}</span></span>)}
      </div>
      {periods.map((p) => (
        <div key={p} className="assump-row" role="group" aria-label={period(p, locale, true)}>
          <div className="assump-month">{period(p, locale)}</div>
          {INPUTS.map((i) => {
            const k = cellKey(i, p);
            const id = `${uid}-${i}-${p}`;
            const msg = message(i, k);
            const label = `${t(`forecast.scenarioInput.${i}` as Key)} (${unitLabel(i)})`;
            return (
              <div key={i} className="assump-cell">
                <label className="assump-label" htmlFor={id}>{label}{unused(i) ? ` · ${t("forecast.inputUnusedShort")}` : ""}</label>
                <div className="num-field">
                  <input id={id} className="input" inputMode="decimal" autoComplete="off" disabled={disabled || unused(i)}
                    aria-label={`${label} ${period(p, locale)}`} aria-invalid={msg ? true : undefined} aria-describedby={msg ? `${id}-err` : unused(i) ? `${uid}-unused` : undefined}
                    value={raw[k] ?? ""} onChange={(e) => set(i, p, e.target.value)} onBlur={() => setTouched((tc) => (tc[k] ? tc : { ...tc, [k]: true }))} />
                  <span className="num-suffix" aria-hidden="true">{unitLabel(i)}</span>
                </div>
                {msg && <span className="field-error" id={`${id}-err`}>{msg}</span>}
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

export const emptyScenario = (): ScenarioMap => ({ brent: {}, dubai: {}, wti: {}, usd_idr: {} });
export const scenarioCount = (s: ScenarioMap): number => INPUTS.reduce((n, k) => n + Object.keys(s[k]).length, 0);
/** Keep only the months that are part of the chosen horizon. */
export function scenarioFor(s: ScenarioMap, periods: string[]): ScenarioMap {
  const keep = new Set(periods);
  const out = emptyScenario();
  for (const k of INPUTS) for (const [p, v] of Object.entries(s[k])) if (keep.has(p)) out[k][p] = v;
  return out;
}

/** The active method's benchmark inputs (from /models), so unused scenario inputs can be disabled. */
export function usedInputsOf(models: ModelList | null | undefined): { used: ReadonlySet<string> | null; prod: ModelVersion | null } {
  const prod = models?.versions.find((v) => v.version === models.production_version) ?? null;
  return { used: prod?.benchmarks_used?.length ? new Set(prod.benchmarks_used) : null, prod };
}

/** Canonical text of a scenario map (non-empty cells only), to compare the form with a run. */
export function scenarioKey(s: Partial<Record<string, Record<string, number> | null | undefined>> | null | undefined): string {
  const parts: string[] = [];
  for (const k of [...INPUTS].sort()) {
    const m = s?.[k] ?? {};
    for (const p of Object.keys(m).sort()) parts.push(`${k}:${p}:${m[p]}`);
  }
  return parts.join("|");
}
/** A run's scenario in the editor's shape (empty maps for inputs it did not set). */
export function scenarioOfRun(run: ForecastRun): ScenarioMap {
  const out = emptyScenario();
  for (const k of INPUTS) Object.assign(out[k], run.scenario?.[k] ?? {});
  return out;
}
/** Months shown for a run (plan-clipped points), or its requested horizon when it has no result. */
export const runHorizon = (r: ForecastRun): number => r.result?.points.length || r.horizon;
/** The official month a run starts from. */
export const runOrigin = (r: ForecastRun): string | null => r.forecast_origin ?? r.last_observed_period ?? null;

/** Cells whose value differs between two scenarios (a cell set in only one of them counts too). */
export function scenarioDiff(a: ScenarioMap, b: ScenarioMap): Array<{ input: Input; period: string; from: number | null; to: number | null }> {
  const out: Array<{ input: Input; period: string; from: number | null; to: number | null }> = [];
  for (const input of INPUTS) {
    const months = [...new Set([...Object.keys(a[input]), ...Object.keys(b[input])])].sort();
    for (const p of months) {
      const from = a[input][p] ?? null, to = b[input][p] ?? null;
      if (from !== to) out.push({ input, period: p, from, to });
    }
  }
  return out.sort((x, y) => x.period.localeCompare(y.period));
}

export type ForecastTab = "hasil" | "asumsi" | "akurasi" | "riwayat";
const TABS: ForecastTab[] = ["hasil", "asumsi", "akurasi", "riwayat"];
const parseTab = (v: string | null): ForecastTab => ((TABS as string[]).includes(v ?? "") ? (v as ForecastTab) : "hasil");
const LEVELS: Record<string, Level> = { "50": "0.5", "80": "0.8", "95": "0.95" };
const levelParam = (l: Level) => String(Math.round(Number(l) * 100));

/**
 * The issue being viewed lives in the URL (`?proyeksi=<id>`): reload, Back and shared links show
 * the same issue. "Tampilkan" in the history and a new run push a history entry.
 */
export default function ForecastPage() {
  const { t, locale, intl } = useT();
  const { principal, isUser, isOperator, hasRole } = useAuth();
  const [params, setParams] = useSearchParams();
  const selectedId = params.get("proyeksi");
  const tab = parseTab(params.get("tab"));
  const level: Level = LEVELS[params.get("tingkat") ?? ""] ?? "0.8";
  const models = useResource((s) => api<ModelList>("/models", { signal: s }), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => nullOn404(api<ForecastRun>("/forecasts/latest", { signal: s })), []);
  const selected = useResource((s) => selectedId ? nullOn404(api<ForecastRun>(`/forecasts/${encodeURIComponent(selectedId)}`, { signal: s })) : Promise.resolve(null), [selectedId]);
  const [cursor, setCursor] = useState<string | null>(null);
  const runs = useResource((s) => api<ForecastRunList>("/forecasts", { query: { limit: 10, cursor }, signal: s }), [cursor]);
  const horizonCap = Math.max(1, principal.horizonMax);
  const [horizon, setHorizonState] = useState(Math.min(3, horizonCap));
  const [scenario, setScenarioState] = useState<ScenarioMap>(emptyScenario());
  const [editorKey, setEditorKey] = useState(0);
  const [scenarioValid, setScenarioValid] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  // The run just created: shown from the POST answer until its own GET has loaded, so the screen
  // never falls back to the previous issue (and a stale notice) in between.
  const [posted, setPosted] = useState<ForecastRun | null>(null);
  const resultRef = useRef<HTMLElement>(null);
  // The form follows the issue on screen until the reader edits it (then the stale guard speaks).
  const edited = useRef(false);
  const syncedFor = useRef<string | null>(null);
  const setHorizon = (h: number) => { edited.current = true; setHorizonState(h); };
  const setScenario = useCallback((s: ScenarioMap) => { edited.current = true; setScenarioState(s); }, []);

  const { used, prod } = useMemo(() => usedInputsOf(models.data), [models.data]);
  // A new run starts from the latest official ICP month (the history's last official value); the
  // latest issue's origin stands in while the history loads or when it is unavailable.
  const origin = history.data?.points.filter((p) => p.icp_price != null).at(-1)?.period ?? latest.data?.forecast_origin ?? null;
  const periods = useMemo(() => origin ? nextPeriods(origin, horizon) : [], [origin, horizon]);
  // Scenario forecasts need the Analyst role (same rule as saved scenarios); operators may preview.
  const canScenario = isOperator || (isUser && hasRole("analyst"));
  const active = canScenario ? scenarioFor(scenario, periods) : emptyScenario();
  // The issue on screen: the one in the URL once it has loaded (or the run just created), the latest
  // issue without `?proyeksi=` or when the linked issue does not exist. Never another issue while the
  // linked one loads.
  const selectedRun = selectedId && selected.data?.id === selectedId ? selected.data : null;
  const postedRun = selectedId && posted?.id === selectedId ? posted : null;
  const missingSelected = !!selectedId && !selected.loading && selected.data === null && !selected.error && !postedRun;
  const run = (selectedId && !missingSelected ? selectedRun ?? postedRun : latest.data) ?? null;
  const points = useMemo(() => history.data?.points ?? [], [history.data]);
  const officialSet = useMemo(() => new Set(points.filter((p) => p.icp_price != null).map((p) => p.period)), [points]);

  useEffect(() => {
    if (!run || syncedFor.current === run.id) return;
    syncedFor.current = run.id;
    if (edited.current) return;
    setHorizonState(Math.min(Math.max(1, runHorizon(run)), horizonCap));
    if (canScenario) { setScenarioState(scenarioOfRun(run)); setEditorKey((k) => k + 1); }
  }, [run, horizonCap, canScenario]);

  // Stale guard: the inputs in the form are not those of the result on screen. A form and an issue
  // are compared only when they start from the same official month: an issue from an earlier origin
  // is explained instead (a new run would start later, so its months and assumptions differ anyway).
  const fromMonth = run ? runOrigin(run) : null;
  const originShift = !!run && !!origin && !!fromMonth && fromMonth !== origin;
  const runSig = run && !originShift ? { h: runHorizon(run), s: scenarioKey(run.scenario) } : null;
  const formSig = { h: horizon, s: scenarioKey(active) };
  const stale = !!runSig && (runSig.h !== formSig.h || (canScenario && runSig.s !== formSig.s));
  const sigText = (h: number, s: string) => (s ? t("forecast.sigScenario", { n: h, k: s.split("|").length }) : t("forecast.sigPlain", { n: h }));
  const cellText = (v: number | null) => (v === null ? t("forecast.staleNone") : new Intl.NumberFormat(intl, { maximumFractionDigits: 2 }).format(v));
  const staleDetail = () => {
    if (!runSig) return "";
    const result = sigText(runSig.h, runSig.s), form = sigText(formSig.h, formSig.s);
    if (result !== form) return t("forecast.staleDetail", { result, form });
    // Same horizon and number of assumptions: name the values that changed.
    const diff = run ? scenarioDiff(scenarioFor(scenarioOfRun(run), periods), active) : [];
    const shown = diff.slice(0, 3).map((d) => `${t(`forecast.scenarioInput.${d.input}` as Key)} ${period(d.period, locale)} (${INPUT_RANGE[d.input].unit === "fx" ? t("forecast.placeholderFx") : t("forecast.placeholderUsd")}): ${cellText(d.from)} → ${cellText(d.to)}`);
    const list = shown.join("; ") + (diff.length > 3 ? `; ${t("forecast.staleMore", { n: diff.length - 3 })}` : "");
    return diff.length ? t("forecast.staleChanged", { list }) : t("forecast.staleDetail", { result, form });
  };
  // Scenario months of an earlier-origin issue that a new run cannot take over: said, never dropped silently.
  const droppedMonths = useMemo(() => {
    if (!run || !originShift || !canScenario) return [] as string[];
    const s = scenarioOfRun(run);
    return [...new Set(INPUTS.flatMap((k) => Object.keys(s[k])))].filter((p) => !periods.includes(p)).sort();
  }, [run, originShift, canScenario, periods]);

  const focusResult = useCallback(() => {
    window.setTimeout(() => {
      const el = resultRef.current;
      if (!el) return;
      scrollToResult(el);
      focusHeading(el);
    }, 50);
  }, []);
  const show = useCallback((r: ForecastRun) => {
    const next = new URLSearchParams(params);
    next.set("proyeksi", r.id);
    next.delete("tab");
    setParams(next);
    setNotice(t("forecast.resultReady"));
    focusResult();
  }, [params, setParams, t, focusResult]);
  const setTab = (v: ForecastTab) => {
    const next = new URLSearchParams(params);
    if (v === "hasil") next.delete("tab"); else next.set("tab", v);
    setParams(next);
  };
  const setLevel = (l: Level) => {
    const next = new URLSearchParams(params);
    if (l === "0.8") next.delete("tingkat"); else next.set("tingkat", levelParam(l));
    setParams(next, { replace: true });
  };

  const submit = async () => {
    if (busy || (canScenario && !scenarioValid)) return; // ignore repeated clicks while the server computes
    setBusy(true); setError(null); setNotice(null);
    try {
      const body: Record<string, unknown> = { horizon };
      if (scenarioCount(active) > 0) body.scenario = active;
      const r = await api<ForecastRun>("/forecasts", { method: "POST", json: body });
      // The new run is exactly what the form says: keep the form (and the editor's text) as it is.
      syncedFor.current = r.id;
      edited.current = false;
      setPosted(r);
      show(r); runs.reload();
    } catch (e) { setError(e); } finally { setBusy(false); }
  };

  const followsLink = !!selectedId && !missingSelected;
  const loadingRun = (history.loading && !history.data) || (!run && (followsLink ? selected.loading : latest.loading && !latest.data));
  // An error matters only while there is no run to show (the history feeds every tab, so it always does).
  const runError = history.error ?? (run ? null : followsLink ? selected.error : latest.error);
  const retryRun = () => { history.reload(); latest.reload(); selected.reload(); };
  const res = run?.result ?? null;
  const tabLabel = (v: ForecastTab) => t(`forecast.tab.${v}` as Key);

  /** Shared gate of the tabs that show the run on screen: loading, error, or no run yet. */
  const gate = (content: (r: ForecastRun) => ReactNode) => (loadingRun
    ? <div className="stack-sm" role="status" aria-label={t("common.loading")}><Skeleton height={64} /><Skeleton height={300} /></div>
    : runError ? <ErrorState error={runError} onRetry={retryRun} />
    : run ? content(run)
    : <EmptyState illustration={<IllNoForecast size={150} />} title={t("overview.noForecast")}>{t("forecast.noRunBody")}</EmptyState>);

  return (
    <div className="page">
      <PageHeader title={t("forecast.title")} description={t("forecast.desc")}
        meta={origin ? <>{t("forecast.metaOrigin", { period: period(origin, locale, true) })}{prod && <> · {t("forecast.metaMethod", { method: methodLabel(prod.method, t) })}</>}</> : undefined} />
      <SlowNotice active={loadingRun} />

      <Panel title={t("forecast.formTitle")} icon={Play} subtitle={origin ? t("forecast.formSub", { period: period(origin, locale, true) }) : undefined}>
        <div className="stack">
          <div className="fc-form">
            <div className="field"><span className="field-label" id="fc-horizon-label">{t("forecast.horizon")}</span>
              <div className="horizon-pick" role="group" aria-labelledby="fc-horizon-label">
                {[1, 2, 3, 4, 5, 6].map((h) => <button key={h} type="button" aria-pressed={horizon === h} disabled={h > principal.horizonMax} onClick={() => setHorizon(h)}
                  aria-label={t("forecast.horizonMonths", { n: h })}>{h}</button>)}
              </div>
              <span className="field-hint">{t("forecast.horizonLimit", { n: principal.horizonMax })}
                {isUser && principal.horizonMax < 6 && <> {t("forecast.horizonUpgrade")} <Link to="/akun#paket">{t("forecast.plansLink")}</Link></>}</span>
            </div>
            <Button variant="primary" size="lg" icon={Play} loading={busy} onClick={submit} disabled={!origin || (canScenario && !scenarioValid)}>{busy ? t("forecast.running") : t("forecast.run")}</Button>
          </div>
          {canScenario && !scenarioValid && <span className="field-error" role="alert">{t("forecast.scenarioInvalid")}</span>}
          <details className="fc-scenario">
            <summary className="link-btn"><SlidersHorizontal size={14} aria-hidden="true" />{t("forecast.scenarioTitle")} {scenarioCount(active) > 0 && <Badge tone="brand">{scenarioCount(active)}</Badge>}</summary>
            <div className="stack-sm mt-3">
              <p className="small muted">{t("forecast.scenarioHelp")}</p>
              {!canScenario && <Callout tone="info">{t("forecast.scenarioRole")}</Callout>}
              {periods.length > 0 && <ScenarioEditor key={editorKey} periods={periods} value={scenario} onChange={setScenario} onValidity={setScenarioValid} disabled={!canScenario}
                usedInputs={used} methodName={prod ? methodLabel(prod.method, t) : undefined} />}
            </div>
          </details>
          <ErrorState error={error} />
        </div>
      </Panel>

      <section ref={resultRef} id="hasil" className={`fc-results anchor${busy && run ? " is-busy" : ""}`} aria-labelledby="fc-results-title" aria-busy={busy || undefined}>
        <div className="fc-results-head">
          <h2 id="fc-results-title" className="section-heading">{t("forecast.resultTitle")}</h2>
          {run && <p className="small muted">{t("forecast.resultSub", { date: dateOnly(run.issue_date ?? run.created_at, intl), workflow: t(`forecast.workflow.${run.workflow ?? "api"}` as Key), n: runHorizon(run) })}{scenarioKey(run.scenario) ? ` · ${t("forecast.withScenario")}` : ""}</p>}
        </div>
        {notice && <div className="sr-only" role="status">{notice}</div>}
        {missingSelected && <Callout tone="warn">{t("forecast.notFoundIssue")}</Callout>}
        {stale && runSig && !busy && (
          <Callout tone="warn" title={t("forecast.staleTitle")}
            action={<Button size="sm" icon={Play} onClick={submit} disabled={!origin || (canScenario && !scenarioValid)}>{t("forecast.rerun")}</Button>}>
            {t("forecast.staleBody")} <span className="muted">{staleDetail()}</span>
          </Callout>
        )}
        {originShift && run && !busy && (
          <Callout tone="info" title={t("forecast.originTitle")}>
            {t("forecast.originBody", { from: period(fromMonth, locale, true), to: period(origin, locale, true) })}
            {droppedMonths.length > 0 && <> {t("forecast.originDropped", { months: droppedMonths.map((p) => period(p, locale, true)).join(", ") })}</>}
          </Callout>
        )}
        {busy && run && <p className="fc-busy-note" role="status"><Info size={15} aria-hidden="true" />{t("forecast.busyNote")}</p>}
        <Tabs idBase="fc" label={t("forecast.tabsLabel")} value={tab} onChange={setTab} tabs={TABS.map((v) => ({ value: v, label: tabLabel(v) }))} />
        <div {...tabPanelProps("fc", tab)} className="fc-tabpanel">
          {tab === "hasil" && <Panel>{gate((r) => <div key={r.id} className="reveal"><ForecastResultView run={r} history={points} level={level} onLevel={setLevel} /></div>)}</Panel>}
          {tab === "asumsi" && <Panel>{gate((r) => r.result ? (
            <div className="stack">
              <AssumptionNote points={r.result.points} history={points} runId={r.id} />
              <InputUsageNotes run={r} />
              <ForecastAssumptionsView run={r} />
            </div>
          ) : <RunFailed run={r} />)}</Panel>}
          {tab === "akurasi" && (
            <div className="stack">
              {models.error ? <Panel><ErrorState error={models.error} onRetry={models.reload} /></Panel>
                : models.loading && !models.data ? <Panel><div role="status" aria-label={t("common.loading")}><Skeleton height={220} /></div></Panel>
                : prod ? <MethodPanel prod={prod} points={res?.points} level={level} notes={(res as (ForecastRun["result"] & ResultReadFields) | null)?.notes} />
                : <Panel><p className="small"><Link to="/metodologi">{t("forecast.methodologyLink")}</Link></p></Panel>}
              <Panel title={t("forecast.limitsTitle")} subtitle={t("forecast.limitsSub")}>
                <div className="grid cols-2 align-start">
                  <ul className="prose-sm fc-limits"><li>{t("meth.limits.1")}</li><li>{t("meth.limits.2")}</li><li>{t("meth.limits.3")}</li><li>{t("meth.limits.4")}</li></ul>
                  <div>
                    <div className="section-title">{t("forecast.processTitle")}</div>
                    <ProcessExplainer />
                  </div>
                </div>
              </Panel>
            </div>
          )}
          {tab === "riwayat" && (
            <Panel subtitle={t("forecast.historySub")} flush>
              {runs.error ? <div className="panel-body"><ErrorState error={runs.error} onRetry={runs.reload} /></div>
                : !runs.data ? <div className="panel-body" role="status" aria-label={t("common.loading")}><Skeleton height={160} /></div>
                : !runs.data.items.length ? <EmptyState title={t("forecast.historyEmpty")}>{t("forecast.historyEmptyBody")}</EmptyState>
                : (
                <TableScroll label={t("forecast.historyTitle")}>
                  <table className="table rc">
                    <caption className="sr-only">{t("forecast.historyTitle")}</caption>
                    <thead><tr><th>{t("forecast.issued")}</th><th>{t("forecast.origin")}</th><th>{t("forecast.window")}</th><th className="hide-sm">{t("common.method")}</th><th>{t("common.status")}</th><th><span className="sr-only">{t("common.actions")}</span></th></tr></thead>
                    <tbody>{runs.data.items.map((r) => {
                      const evaluable = !!r.result?.points.length && r.result.points.every((p) => officialSet.has(p.period));
                      return (
                        <tr key={r.id} className={run?.id === r.id ? "selected" : ""} aria-current={run?.id === r.id ? "true" : undefined}>
                          <td className="primary-cell nowrap rc-title">{dateOnly(r.issue_date ?? r.created_at, intl)}<span className="sub">{t("forecast.horizonMonths", { n: r.result?.points.length ?? r.horizon })} · {t(`forecast.workflow.${r.workflow ?? "api"}` as Key)}{scenarioKey(r.scenario) ? ` · ${t("forecast.withScenario")}` : ""}</span></td>
                          <td data-label={t("forecast.origin")}>{period(r.forecast_origin ?? r.last_observed_period, locale)}</td>
                          <td data-label={t("forecast.window")}>{r.result ? rangeText(period(r.result.points[0]?.period, locale), period(r.result.points.at(-1)?.period, locale), locale) : NA}
                            {r.result?.points.some((p) => p.kind === "legacy_record") && <span className="sub">{t("kind.legacy_record")}</span>}
                            {evaluable && <span className="sub">{t("kind.official_published")} · {t("forecast.evaluable")}</span>}</td>
                          <td className="hide-sm" data-label={t("common.method")}>{methodLabel(r.method, t)}</td>
                          <td data-label={t("common.status")}>{r.status === "succeeded" ? <Badge tone="good">{t("status.succeeded")}</Badge> : <Badge tone="bad">{t("status.failed")}</Badge>}</td>
                          <td className="num rc-full">{r.status === "succeeded" && <button type="button" className="link-btn small" onClick={() => show(r)} aria-label={`${t("forecast.viewRun")}: ${dateOnly(r.issue_date ?? r.created_at, intl)}`}>{t("forecast.viewRun")} <ArrowRight size={13} aria-hidden="true" /></button>}</td>
                        </tr>
                      );
                    })}</tbody>
                  </table>
                </TableScroll>
              )}
              <Pager hasPrev={!!cursor} hasNext={!!runs.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(runs.data?.next_cursor ?? null)} />
            </Panel>
          )}
        </div>
      </section>
    </div>
  );
}
