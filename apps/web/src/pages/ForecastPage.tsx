/** Proyeksi ICP: run a forecast (horizon + optional scenario), inspect assumptions, intervals, method, history. */
import type { ForecastRun, ForecastRunList, HistoryResponse, ModelList } from "../api/types";
import { Activity, ArrowRight, History, Play, SlidersHorizontal } from "lucide-react";
import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { api, nullOn404 } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { ForecastRunView, MethodPanel, methodLabel, type Level } from "../components/forecast";
import { ProcessExplainer } from "../components/process";
import { IllNoForecast } from "../components/illustrations";
import { Badge, Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, focusHeading, scrollToResult } from "../components/ui";
import { dateOnly, fmtNum, NA, nextPeriods, period, rangeText } from "../format";
import { useT, type Key } from "../i18n";
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
export function ScenarioEditor({ periods, value, onChange, onValidity, disabled }:
  { periods: string[]; value: ScenarioMap; onChange: (v: ScenarioMap) => void; onValidity?: (valid: boolean) => void; disabled?: boolean }) {
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
      <div className="assump-head" aria-hidden="true">
        <span>{t("common.month")}</span>
        {INPUTS.map((i) => <span key={i}>{t(`forecast.scenarioInput.${i}` as Key)} <span className="th-unit">{unitLabel(i)}</span></span>)}
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
                <label className="assump-label" htmlFor={id}>{label}</label>
                <div className="num-field">
                  <input id={id} className="input" inputMode="decimal" autoComplete="off" disabled={disabled}
                    aria-label={`${label} ${period(p, locale)}`} aria-invalid={msg ? true : undefined} aria-describedby={msg ? `${id}-err` : undefined}
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
const hasScenario = (r: ForecastRun): boolean => !!r.scenario && Object.values(r.scenario).some((m) => Object.keys(m ?? {}).length > 0);

export default function ForecastPage() {
  const { t, locale, intl } = useT();
  const { principal, isUser, isOperator, hasRole } = useAuth();
  const models = useResource((s) => api<ModelList>("/models", { signal: s }), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => nullOn404(api<ForecastRun>("/forecasts/latest", { signal: s })), []);
  const [cursor, setCursor] = useState<string | null>(null);
  const runs = useResource((s) => api<ForecastRunList>("/forecasts", { query: { limit: 10, cursor }, signal: s }), [cursor]);
  const [horizon, setHorizon] = useState(Math.min(3, principal.horizonMax));
  const [scenario, setScenario] = useState<ScenarioMap>(emptyScenario());
  const [scenarioValid, setScenarioValid] = useState(true);
  const [level, setLevel] = useState<Level>("0.8");
  const [current, setCurrent] = useState<ForecastRun | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const resultRef = useRef<HTMLDivElement>(null);

  const prod = useMemo(() => models.data?.versions.find((v) => v.version === models.data?.production_version) ?? null, [models.data]);
  const origin = latest.data?.forecast_origin ?? history.data?.points.filter((p) => p.icp_price != null).at(-1)?.period ?? null;
  const periods = useMemo(() => origin ? nextPeriods(origin, horizon) : [], [origin, horizon]);
  // Scenario forecasts need the Analyst role (same rule as saved scenarios); operators may preview.
  const canScenario = isOperator || (isUser && hasRole("analyst"));
  const active = canScenario ? scenarioFor(scenario, periods) : emptyScenario();
  const run = current ?? latest.data ?? null;
  const points = history.data?.points ?? [];

  const showResult = useCallback((r: ForecastRun) => {
    setCurrent(r);
    setNotice(t("forecast.resultReady"));
    window.setTimeout(() => {
      const el = resultRef.current;
      if (!el) return;
      scrollToResult(el);
      focusHeading(el);
    }, 50);
  }, [t]);

  const submit = async () => {
    if (busy || (canScenario && !scenarioValid)) return; // ignore repeated clicks while the server computes
    setBusy(true); setError(null); setNotice(null);
    try {
      const body: Record<string, unknown> = { horizon };
      if (scenarioCount(active) > 0) body.scenario = active;
      const r = await api<ForecastRun>("/forecasts", { method: "POST", json: body });
      showResult(r); runs.reload();
    } catch (e) { setError(e); } finally { setBusy(false); }
  };

  return (
    <div className="page">
      <PageHeader eyebrow={t("forecast.eyebrow")} icon={Activity} title={t("forecast.title")} description={t("forecast.desc")} />
      {models.error && <ErrorState error={models.error} onRetry={models.reload} />}

      <Panel title={t("forecast.run")} icon={Play} subtitle={origin ? `${t("forecast.origin")}: ${period(origin, locale, true)}` : undefined}>
        <div className="stack">
          <div className="row" style={{ gap: 20, alignItems: "flex-end" }}>
            <div className="field"><span className="field-label">{t("forecast.horizon")}</span>
              <div className="horizon-pick" role="group" aria-label={t("forecast.horizon")}>
                {[1, 2, 3, 4, 5, 6].map((h) => <button key={h} type="button" aria-pressed={horizon === h} disabled={h > principal.horizonMax} onClick={() => setHorizon(h)}>{h}</button>)}
              </div>
              <span className="field-hint">{t("forecast.horizonLimit", { n: principal.horizonMax })}
                {isUser && principal.horizonMax < 6 && <> {t("forecast.horizonUpgrade")} <Link to="/akun#paket">{t("forecast.plansLink")}</Link></>}</span>
            </div>
            <Button variant="primary" size="lg" icon={Play} loading={busy} onClick={submit} disabled={!origin || (canScenario && !scenarioValid)}>{busy ? t("forecast.running") : t("forecast.run")}</Button>
          </div>
          {canScenario && !scenarioValid && <span className="field-error" role="alert">{t("forecast.scenarioInvalid")}</span>}
          <details>
            <summary className="link-btn"><SlidersHorizontal size={14} aria-hidden="true" />{t("forecast.scenarioTitle")} {scenarioCount(active) > 0 && <Badge tone="brand">{scenarioCount(active)}</Badge>}</summary>
            <div className="stack-sm mt-3">
              <p className="small muted">{t("forecast.scenarioHelp")}</p>
              {!canScenario && <Callout tone="info">{t("forecast.scenarioRole")}</Callout>}
              {periods.length > 0 && <ScenarioEditor periods={periods} value={scenario} onChange={setScenario} onValidity={setScenarioValid} disabled={!canScenario} />}
            </div>
          </details>
          <ErrorState error={error} />
        </div>
      </Panel>

      <div ref={resultRef} id="hasil" className="anchor">
        <Panel title={t("forecast.resultTitle")} icon={Activity}
          subtitle={run ? <>{t("forecast.resultSub", { date: dateOnly(run.issue_date ?? run.created_at, intl), workflow: t(`forecast.workflow.${run.workflow ?? "api"}` as Key), n: run.horizon })}{hasScenario(run) ? ` · ${t("forecast.withScenario")}` : ""}</> : undefined}>
          {notice && <div className="sr-only" role="status">{notice}</div>}
          {history.loading && !history.data ? <Skeleton height={320} />
            : history.error || latest.error ? <ErrorState error={history.error ?? latest.error} onRetry={() => { history.reload(); latest.reload(); }} />
            : run ? <div key={run.id} className="reveal"><ForecastRunView run={run} history={points} level={level} onLevel={setLevel} prod={prod} /></div> : (
            <EmptyState illustration={<IllNoForecast size={160} />} title={t("overview.noForecast")}>{t("forecast.historyEmptyBody")}</EmptyState>
          )}
        </Panel>
      </div>
      {!run && prod && <MethodPanel prod={prod} />}

      <Panel title={t("forecast.limitsTitle")} icon={History}>
        <div className="grid cols-2" style={{ alignItems: "start" }}>
          <ul className="prose-sm"><li>{t("forecast.limits1")}</li><li>{t("forecast.limits2")}</li><li>{t("forecast.limits3")}</li><li>{t("forecast.limits4")}</li></ul>
          <div>
            <div className="section-title">{t("forecast.processTitle")}</div>
            <ProcessExplainer />
          </div>
        </div>
      </Panel>

      <Panel title={t("forecast.historyTitle")} icon={History} subtitle={t("forecast.historySub")} flush>
        {runs.error ? <div className="panel-body"><ErrorState error={runs.error} onRetry={runs.reload} /></div> : !runs.data ? <div className="panel-body"><Skeleton height={160} /></div> : !runs.data.items.length ? (
          <EmptyState title={t("forecast.historyEmpty")}>{t("forecast.historyEmptyBody")}</EmptyState>
        ) : (
          <div className="table-scroll">
            <table className="table">
              <thead><tr><th>{t("forecast.issued")}</th><th>{t("forecast.origin")}</th><th>{t("forecast.window")}</th><th className="hide-sm">{t("common.method")}</th><th>{t("common.status")}</th><th /></tr></thead>
              <tbody>{runs.data.items.map((r) => (
                <tr key={r.id} className={run?.id === r.id ? "selected" : ""}>
                  <td className="primary-cell nowrap">{dateOnly(r.issue_date ?? r.created_at, intl)}<span className="sub">{t("forecast.horizonMonths", { n: r.horizon })} · {t(`forecast.workflow.${r.workflow ?? "api"}` as Key)}{hasScenario(r) ? ` · ${t("forecast.withScenario")}` : ""}</span></td>
                  <td>{period(r.forecast_origin ?? r.last_observed_period, locale)}</td>
                  <td>{r.result ? rangeText(period(r.result.points[0]?.period, locale), period(r.result.points.at(-1)?.period, locale), locale) : NA}
                    {r.result?.points.some((p) => p.kind === "legacy_record") && <span className="sub">{t("kind.legacy_record")}</span>}</td>
                  <td className="hide-sm">{methodLabel(r.method, t)}</td>
                  <td>{r.status === "succeeded" ? <Badge tone="good">{t("status.succeeded")}</Badge> : <Badge tone="bad">{t("status.failed")}</Badge>}</td>
                  <td className="num">{r.status === "succeeded" && <button className="link-btn small" onClick={() => showResult(r)} aria-label={`${t("forecast.viewRun")}: ${dateOnly(r.issue_date ?? r.created_at, intl)}`}>{t("forecast.viewRun")} <ArrowRight size={13} aria-hidden="true" /></button>}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <Pager hasPrev={!!cursor} hasNext={!!runs.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(runs.data?.next_cursor ?? null)} />
      </Panel>
    </div>
  );
}
