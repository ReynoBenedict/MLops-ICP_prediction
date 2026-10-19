/**
 * Simulasi Skenario: saved scenarios (left) and results (right): a scenario run or a side-by-side
 * comparison against the no-assumption baseline, with a link to the comparison report.
 */
import type { ForecastRun, ForecastRunList, HistoryResponse, OrganizationDetail, Scenario, ScenarioCompareResponse, ScenarioList } from "../api/types";
import { Archive, FileText, GitCompare, Pencil, Play, Plus, SlidersHorizontal } from "lucide-react";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { MARKERS, SERIES_COLORS, TimeSeriesChart, type SeriesSpec } from "../components/charts";
import { ForecastRunView, methodLabel, type Level } from "../components/forecast";
import { IllScenarios } from "../components/illustrations";
import { useToast } from "../components/toast";
import { Badge, Button, Callout, ConfirmDialog, EmptyState, ErrorState, KindChip, PageHeader, Panel, Skeleton, Stat, focusHeading, scrollToResult } from "../components/ui";
import { dateOnly, fmtNum, fmtSigned, NA, nextPeriods, period } from "../format";
import { useT, type Key } from "../i18n";
import { emptyScenario, ScenarioEditor, scenarioFor, type ScenarioMap } from "./ForecastPage";

const COLORS = [SERIES_COLORS.s1, SERIES_COLORS.s2, SERIES_COLORS.s3, SERIES_COLORS.s4];

function toMap(inputs: Scenario["inputs"]): ScenarioMap {
  const m = emptyScenario();
  for (const k of ["brent", "dubai", "wti", "usd_idr"] as const) m[k] = { ...(inputs[k] ?? {}) };
  return m;
}

function ScenarioForm({ origin, initial, onDone, onCancel, maxHorizon }: { origin: string; initial?: Scenario; onDone: (name: string) => void; onCancel: () => void; maxHorizon: number }) {
  const { t } = useT();
  const [name, setName] = useState(initial?.name ?? "");
  const [description, setDescription] = useState(initial?.description ?? "");
  const [horizon, setHorizon] = useState(initial?.horizon ?? 3);
  const [inputs, setInputs] = useState<ScenarioMap>(initial ? toMap(initial.inputs) : emptyScenario());
  const [inputsValid, setInputsValid] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const periods = useMemo(() => nextPeriods(origin, horizon), [origin, horizon]);
  const nameRef = useRef<HTMLInputElement>(null);
  useEffect(() => { nameRef.current?.focus(); }, []);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (busy || !inputsValid) return; // no double submission while the server is answering
    setBusy(true); setError(null);
    try {
      const body = { name, description: description || null, horizon, inputs: scenarioFor(inputs, periods) };
      if (initial) await api(`/scenarios/${initial.id}`, { method: "POST", json: body });
      else await api("/scenarios", { method: "POST", json: body });
      setBusy(false);
      onDone(name.trim());
    } catch (err) { setError(err); setBusy(false); }
  };
  return (
    <form onSubmit={submit} className="stack">
      <div className="grid cols-3">
        <label className="field"><span className="field-label">{t("scen.name")}</span><input ref={nameRef} className="input" value={name} onChange={(e) => setName(e.target.value)} required maxLength={120} /></label>
        <label className="field"><span className="field-label">{t("scen.description")} <span className="muted">({t("common.optional")})</span></span><input className="input" value={description} onChange={(e) => setDescription(e.target.value)} maxLength={1000} /></label>
        <div className="field"><span className="field-label">{t("forecast.horizon")}</span>
          <div className="horizon-pick" role="group" aria-label={t("forecast.horizon")}>{[1, 2, 3, 4, 5, 6].map((h) => <button key={h} type="button" aria-pressed={horizon === h} disabled={h > maxHorizon} onClick={() => setHorizon(h)}>{h}</button>)}</div></div>
      </div>
      <div><div className="section-title">{t("scen.inputs")}</div><p className="small muted" style={{ marginBottom: 8 }}>{t("scen.inputsHelp")}</p>
        <ScenarioEditor periods={periods} value={inputs} onChange={setInputs} onValidity={setInputsValid} /></div>
      {!inputsValid && <span className="field-error" role="alert">{t("forecast.scenarioInvalid")}</span>}
      <ErrorState error={error} />
      <div className="row"><Button type="submit" variant="primary" loading={busy} disabled={!name.trim() || !inputsValid}>{initial ? t("scen.update") : t("scen.create")}</Button><Button type="button" variant="ghost" onClick={onCancel}>{t("common.cancel")}</Button></div>
    </form>
  );
}

function CompareView({ cmp, history }: { cmp: ScenarioCompareResponse; history: HistoryResponse | null }) {
  const { t, locale, intl } = useT();
  const scenarios = cmp.scenarios as Array<{ id: string; name: string; forecast_id: string; inputs: Record<string, Record<string, number>> }>;
  const rows = cmp.rows as Array<{ period: string; kind: string; baseline: number; scenarios: Record<string, { prediction: number; delta_vs_baseline: number; conditional_on_scenario: boolean }> }>;
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
  const reportHref = `/laporan?jenis=perbandingan&skenario=${encodeURIComponent(scenarios.map((s) => s.id).join(","))}`;
  return (
    <div className="stack">
      <div className="stat-grid">
        <Stat label={t("forecast.origin")} value={period(cmp.forecast_origin, locale)} />
        <Stat label={t("forecast.issueDate")} value={dateOnly(cmp.issue_date, intl)} />
        <Stat label={t("common.method")} value={methodLabel(cmp.method, t)} />
      </div>
      <TimeSeriesChart ariaLabel={t("scen.chartTitle")} data={chartRows} series={series} zones={rows.length ? [{ from: rows[0]!.period, to: rows.at(-1)!.period, kind: "forecast" }] : []} />
      <p className="xs muted">{t("scen.markerNote")}</p>
      <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: "var(--r-card)" }}>
        <table className="table">
          <thead><tr><th>{t("common.month")}</th><th>{t("forecast.kindLegend")}</th><th className="num">{t("scen.baseline")}</th>
            {scenarios.map((s, i) => <th key={s.id} className="num"><span className="legend-key"><span className="swatch-line" style={{ background: COLORS[i] }} />{s.name}</span></th>)}</tr></thead>
          <tbody>{rows.map((r) => (
            <tr key={r.period}>
              <td className="primary-cell">{period(r.period, locale)}</td>
              <td><KindChip kind={r.kind as "future_forecast"} /></td>
              <td className="num strong">{fmtNum(r.baseline, intl)}</td>
              {scenarios.map((s) => { const v = r.scenarios[s.id]; return <td key={s.id} className="num">{v ? <>{fmtNum(v.prediction, intl)}<span className="sub">{fmtSigned(v.delta_vs_baseline, intl)} {t("scen.delta")}{v.conditional_on_scenario ? ` · ${t("forecast.conditional")}` : ""}</span></> : NA}</td>; })}
            </tr>
          ))}</tbody>
        </table>
      </div>
      <Callout tone="info" title={t("scen.compareNote")} action={<Link className="btn sm" to={reportHref}><FileText size={15} aria-hidden="true" />{t("scen.reportLink")}</Link>}>{cmp.note}</Callout>
    </div>
  );
}

export default function ScenariosPage() {
  const { t, locale, intl } = useT();
  // Customer-only route (the router keeps guests and operators out).
  const { isUser, isOperator, principal, hasRole } = useAuth();
  const org = useResource((s) => isUser ? api<OrganizationDetail>("/org", { signal: s }) : Promise.resolve(null), [isUser]);
  const list = useResource((s) => api<ScenarioList>("/scenarios", { query: { limit: 50 }, signal: s }), []);
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const latest = useResource((s) => api<ForecastRunList>("/forecasts", { query: { limit: 1 }, signal: s }).catch(() => null), []);
  const [editing, setEditing] = useState<Scenario | "new" | null>(null);
  const [selected, setSelected] = useState<string[]>([]);
  const [cmp, setCmp] = useState<ScenarioCompareResponse | null>(null);
  const [runResult, setRunResult] = useState<{ run: ForecastRun; scenario: Scenario } | null>(null);
  const [level, setLevel] = useState<Level>("0.8");
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null);
  const resultsRef = useRef<HTMLDivElement>(null);
  const [archiving, setArchiving] = useState<Scenario | null>(null);
  const { show } = useToast();

  const origin = latest.data?.items[0]?.forecast_origin ?? history.data?.points.filter((p) => p.icp_price != null).at(-1)?.period ?? null;
  const ent = org.data?.entitlements;
  const canCreate = hasRole("analyst") && (isOperator || !!ent?.scenarios);
  const items = list.data?.items ?? [];

  useEffect(() => {
    if (!cmp && !runResult) return;
    const el = resultsRef.current;
    if (!el) return;
    scrollToResult(el);
    focusHeading(el);
  }, [cmp, runResult]);
  const act = async (key: string, fn: () => Promise<void>) => { setBusy(key); setError(null); try { await fn(); } catch (e) { setError(e); } finally { setBusy(null); } };

  const results = cmp ? (
    <Panel title={t("scen.compareTitle")} icon={GitCompare}><CompareView cmp={cmp} history={history.data} /></Panel>
  ) : runResult ? (
    <Panel title={t("scen.runResult", { name: runResult.scenario.name })} icon={Play} subtitle={t("scen.runResultSub", { date: dateOnly(runResult.run.issue_date ?? null, intl) })}>
      <ForecastRunView run={runResult.run} history={history.data?.points ?? []} level={level} onLevel={setLevel} />
    </Panel>
  ) : (
    <Panel title={t("scen.resultsTitle")} icon={GitCompare}>
      <EmptyState illustration={<IllScenarios size={150} />} title={t("scen.resultsEmpty")}>{t("scen.resultsEmptyBody")}</EmptyState>
    </Panel>
  );

  return (
    <div className="page">
      <PageHeader eyebrow={t("scen.eyebrow")} icon={SlidersHorizontal} title={t("scen.title")} description={t("scen.desc")}
        actions={<>
          <Button variant="primary" icon={Plus} onClick={() => setEditing("new")} disabled={!canCreate || !origin}>{t("scen.new")}</Button>
          <Button icon={GitCompare} disabled={selected.length === 0 || !!busy} loading={busy === "compare"}
            onClick={() => act("compare", async () => { setRunResult(null); setCmp(await api<ScenarioCompareResponse>("/scenarios/compare", { method: "POST", json: { scenario_ids: selected } })); })}>{t("scen.compare")} ({selected.length})</Button>
        </>} />
      {ent && <p className="small muted">{t("scen.limit", { plan: ent.label_id, n: ent.saved_scenarios_max, used: org.data?.saved_scenarios ?? items.length })}</p>}
      {isUser && ent && !ent.scenarios && <Callout tone="warn">{t("scen.noPlan")}</Callout>}
      {isUser && !hasRole("analyst") && <Callout tone="info">{t("scen.viewerNote")}</Callout>}
      <ErrorState error={error} />

      {editing && origin && (
        <Panel title={editing === "new" ? t("scen.new") : t("scen.edit")} icon={Pencil}>
          <ScenarioForm origin={origin} initial={editing === "new" ? undefined : editing} maxHorizon={principal.horizonMax}
            onDone={(name) => { setEditing(null); list.reload(); org.reload(); show(t("toast.scenarioSaved", { name }), "good"); }} onCancel={() => setEditing(null)} />
        </Panel>
      )}

      <div className="layout-list-detail">
        <Panel title={t("nav.scenarios")} icon={SlidersHorizontal} subtitle={t("scen.select")}>
          {list.loading && !list.data ? <Skeleton height={140} /> : list.error ? <ErrorState error={list.error} onRetry={list.reload} /> : !items.length ? (
            <EmptyState illustration={<IllScenarios size={160} />} title={t("scen.empty")} action={canCreate && origin ? <Button variant="primary" icon={Plus} onClick={() => setEditing("new")}>{t("scen.new")}</Button> : undefined}>{t("scen.emptyBody")}</EmptyState>
          ) : (
            <div className="stack-sm">
              {items.map((s) => {
                const idx = selected.indexOf(s.id);
                const n = Object.values(s.inputs).reduce((a, m) => a + Object.keys(m).length, 0);
                return (
                  <div key={s.id} className={`scenario-card${idx >= 0 ? " selected" : ""}`}>
                    <label className="check" style={{ marginTop: 2 }}><input type="checkbox" checked={idx >= 0} aria-label={s.name} disabled={idx < 0 && selected.length >= 4}
                      onChange={(e) => setSelected(e.target.checked ? [...selected, s.id].slice(0, 4) : selected.filter((x) => x !== s.id))} /></label>
                    <div className="grow" style={{ minWidth: 0 }}>
                      <div className="row"><strong>{s.name}</strong><Badge tone="neutral">{t("forecast.horizonMonths", { n: s.horizon })}</Badge><Badge tone="brand">{t("scen.inputsSummary", { n })}</Badge>
                        {idx >= 0 && <span className="legend-key xs"><span className="swatch-line" style={{ background: COLORS[idx] }} />#{idx + 1}</span>}</div>
                      {s.description && <div className="small muted">{s.description}</div>}
                      <div className="xs muted">{Object.entries(s.inputs).flatMap(([k, m]) => Object.entries(m).map(([p, v]) => `${t(`forecast.scenarioInput.${k}` as Key)} ${period(p, locale)}: ${fmtNum(v, intl, k === "usd_idr" ? 0 : 2)}`)).join(" · ") || t("common.none")}</div>
                      <div className="xs muted">{s.last_forecast_id ? t("scen.hasRun") : t("scen.never")} · {t("common.updated")} {dateOnly(s.updated_at, intl)}</div>
                    </div>
                    <div className="row" style={{ flexWrap: "nowrap" }}>
                      <Button size="sm" variant="primary" icon={Play} loading={busy === `run-${s.id}`} disabled={!!busy || !hasRole("analyst")}
                        onClick={() => act(`run-${s.id}`, async () => { const r = await api<ForecastRun>(`/scenarios/${s.id}/run`, { method: "POST" }); setCmp(null); setRunResult({ run: r, scenario: s }); list.reload(); })}>{t("scen.run")}</Button>
                      <Button size="sm" variant="ghost" icon={Pencil} onClick={() => setEditing(s)} disabled={!hasRole("analyst")} aria-label={`${t("scen.edit")}: ${s.name}`} title={t("scen.edit")} />
                      <Button size="sm" variant="ghost" icon={Archive} aria-label={`${t("scen.archive")}: ${s.name}`} title={t("scen.archive")} disabled={!hasRole("analyst")}
                        loading={busy === `arch-${s.id}`} onClick={() => setArchiving(s)} />
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </Panel>
        <div ref={resultsRef} id="hasil" className="anchor">{results}</div>
      </div>
      <ConfirmDialog open={!!archiving} busy={!!archiving && busy === `arch-${archiving.id}`}
        title={t("scen.archiveConfirmTitle", { name: archiving?.name ?? "" })} confirmLabel={t("scen.archiveConfirm")}
        onCancel={() => setArchiving(null)}
        onConfirm={() => {
          const s = archiving;
          if (!s) return;
          void act(`arch-${s.id}`, async () => {
            await api(`/scenarios/${s.id}/archive`, { method: "POST" });
            // Only after the server confirmed: drop it from the selection and from any result on screen.
            setSelected((sel) => sel.filter((x) => x !== s.id));
            if (cmp?.scenarios.some((c) => (c as { id?: string }).id === s.id)) setCmp(null);
            if (runResult?.scenario.id === s.id) setRunResult(null);
            list.reload(); org.reload();
            show(t("toast.scenarioArchived", { name: s.name }), "good");
          }).finally(() => setArchiving(null));
        }}>
        {t("scen.archiveConfirmBody")}
      </ConfirmDialog>
    </div>
  );
}
