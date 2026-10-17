/** Proyeksi ICP: run a forecast (horizon + optional scenario), inspect assumptions, intervals, method, history. */
import type { ForecastRun, ForecastRunList, HistoryResponse, ModelList } from "../api/types";
import { Activity, ArrowRight, History, Play, SlidersHorizontal } from "lucide-react";
import { useCallback, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { api, ApiError, nullOn404 } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { ForecastRunView, MethodPanel, methodLabel, ProcessExplainer, type Level } from "../components/forecast";
import { IllNoForecast } from "../components/illustrations";
import { Badge, Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, focusHeading } from "../components/ui";
import { dateOnly, NA, nextPeriods, period, rangeText } from "../format";
import { useT, type Key } from "../i18n";

const INPUTS = ["brent", "dubai", "wti", "usd_idr"] as const;
type Input = (typeof INPUTS)[number];
export type ScenarioMap = Record<Input, Record<string, number>>;

export function ScenarioEditor({ periods, value, onChange, disabled }: { periods: string[]; value: ScenarioMap; onChange: (v: ScenarioMap) => void; disabled?: boolean }) {
  const { t, locale } = useT();
  const set = (inp: Input, p: string, raw: string) => {
    const next: ScenarioMap = { brent: { ...value.brent }, dubai: { ...value.dubai }, wti: { ...value.wti }, usd_idr: { ...value.usd_idr } };
    const n = Number(raw.replace(",", "."));
    if (!raw.trim() || !Number.isFinite(n)) delete next[inp][p]; else next[inp][p] = n;
    onChange(next);
  };
  return (
    <div className="assump-grid" role="group" aria-label={t("forecast.scenarioTitle")}>
      <span className="hd">{t("common.month")}</span>
      {INPUTS.map((i) => <span key={i} className="hd">{t(`forecast.scenarioInput.${i}` as Key)}</span>)}
      {periods.map((p) => (
        <span key={p} style={{ display: "contents" }}>
          <span className="small strong">{period(p, locale)}</span>
          {INPUTS.map((i) => (
            <input key={i} className="input" inputMode="decimal" disabled={disabled} aria-label={`${t(`forecast.scenarioInput.${i}` as Key)} ${period(p, locale)}`}
              placeholder={i === "usd_idr" ? t("forecast.placeholderFx") : t("forecast.placeholderUsd")} value={value[i][p] ?? ""} onChange={(e) => set(i, p, e.target.value)} />
          ))}
        </span>
      ))}
    </div>
  );
}

export const emptyScenario = (): ScenarioMap => ({ brent: {}, dubai: {}, wti: {}, usd_idr: {} });
export const scenarioCount = (s: ScenarioMap): number => INPUTS.reduce((n, k) => n + Object.keys(s[k]).length, 0);
const hasScenario = (r: ForecastRun): boolean => !!r.scenario && Object.values(r.scenario).some((m) => Object.keys(m ?? {}).length > 0);

export default function ForecastPage() {
  const { t, locale, intl } = useT();
  const { principal, isUser, isOperator } = useAuth();
  const models = useResource((s) => api<ModelList>("/models", { signal: s }), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => nullOn404(api<ForecastRun>("/forecasts/latest", { signal: s })), []);
  const [cursor, setCursor] = useState<string | null>(null);
  const runs = useResource((s) => api<ForecastRunList>("/forecasts", { query: { limit: 10, cursor }, signal: s }), [cursor]);
  const [horizon, setHorizon] = useState(3);
  const [scenario, setScenario] = useState<ScenarioMap>(emptyScenario());
  const [level, setLevel] = useState<Level>("0.8");
  const [current, setCurrent] = useState<ForecastRun | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const resultRef = useRef<HTMLDivElement>(null);

  const prod = useMemo(() => models.data?.versions.find((v) => v.version === models.data?.production_version) ?? null, [models.data]);
  const origin = latest.data?.forecast_origin ?? history.data?.points.filter((p) => p.icp_price != null).at(-1)?.period ?? null;
  const periods = useMemo(() => origin ? nextPeriods(origin, horizon) : [], [origin, horizon]);
  const canScenario = isUser || isOperator;
  const run = current ?? latest.data ?? null;
  const points = history.data?.points ?? [];
  const signinHref = "/masuk?next=%2Fproyeksi";

  const showResult = useCallback((r: ForecastRun) => {
    setCurrent(r);
    setNotice(t("forecast.resultReady"));
    window.setTimeout(() => {
      const el = resultRef.current;
      if (!el) return;
      try { el.scrollIntoView({ behavior: "smooth", block: "start" }); } catch { /* jsdom */ }
      focusHeading(el);
    }, 50);
  }, [t]);

  const submit = async () => {
    if (busy) return; // ignore repeated clicks while the server computes
    setBusy(true); setError(null); setNotice(null);
    try {
      const body: Record<string, unknown> = { horizon };
      if (canScenario && scenarioCount(scenario) > 0) body.scenario = scenario;
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
              <span className="field-hint">{t("forecast.horizonLimit", { n: principal.horizonMax })} {principal.horizonMax < 6 && t("forecast.horizonUpgrade")}</span>
            </div>
            <Button variant="primary" size="lg" icon={Play} loading={busy} onClick={submit} disabled={!origin}>{busy ? t("forecast.running") : t("forecast.run")}</Button>
          </div>
          <details>
            <summary className="link-btn"><SlidersHorizontal size={14} aria-hidden="true" />{t("forecast.scenarioTitle")} {scenarioCount(scenario) > 0 && <Badge tone="brand">{scenarioCount(scenario)}</Badge>}</summary>
            <div className="stack-sm mt-3">
              <p className="small muted">{t("forecast.scenarioHelp")}</p>
              {!canScenario && <Callout tone="info" action={<Link className="btn sm primary" to={signinHref}>{t("shell.signin")}</Link>}>{t("forecast.scenarioSignin")}</Callout>}
              {periods.length > 0 && <ScenarioEditor periods={periods} value={scenario} onChange={setScenario} disabled={!canScenario} />}
            </div>
          </details>
          {error instanceof ApiError && error.code === "AUTH_REQUIRED" ? <Callout tone="warn" action={<Link className="btn sm primary" to={signinHref}>{t("shell.signin")}</Link>}>{t("forecast.scenarioSignin")}</Callout> : <ErrorState error={error} />}
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
              <thead><tr><th>{t("forecast.issued")}</th><th>{t("forecast.origin")}</th><th>{t("forecast.window")}</th><th className="hide-sm">{t("common.method")}</th><th className="hide-md">{t("forecast.workflow")}</th><th>{t("common.status")}</th><th /></tr></thead>
              <tbody>{runs.data.items.map((r) => (
                <tr key={r.id} className={run?.id === r.id ? "selected" : ""}>
                  <td className="primary-cell nowrap">{dateOnly(r.issue_date ?? r.created_at, intl)}<span className="sub">{t("forecast.horizonMonths", { n: r.horizon })}{hasScenario(r) ? ` · ${t("forecast.withScenario")}` : ""}</span></td>
                  <td>{period(r.forecast_origin ?? r.last_observed_period, locale)}</td>
                  <td>{r.result ? rangeText(period(r.result.points[0]?.period, locale), period(r.result.points.at(-1)?.period, locale), locale) : NA}
                    {r.result?.points.some((p) => p.kind === "legacy_record") && <span className="sub">{t("kind.legacy_record")}</span>}</td>
                  <td className="hide-sm">{methodLabel(r.method, t)}</td>
                  <td className="hide-md">{t(`forecast.workflow.${r.workflow ?? "api"}` as Key)}</td>
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
