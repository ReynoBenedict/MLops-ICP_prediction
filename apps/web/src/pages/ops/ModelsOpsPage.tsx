import type { DriftResponse, ExperimentRunList, ModelList, ModelVersion, ModelVersionDetail, PromotionList, PromotionResult, ValidationReport } from "../../api/types";
import { Activity, Beaker, BookOpen, CircleCheck, CircleX, FlaskConical, GitCompare, History, Layers, Rocket, ShieldCheck, Undo2 } from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api, ApiError } from "../../api/client";
import { useResource } from "../../api/hooks";
import { MetricBars, SERIES_COLORS, TimeSeriesChart, type Zone } from "../../components/charts";
import { Badge, Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, Stat, StatusBadge, Tabs } from "../../components/ui";
import { dateTime, fmtNum, fromMs, monthRange, period } from "../../format";
import { useT, type Key } from "../../i18n";

type Tab = "evaluation" | "versions" | "experiments" | "promotions" | "drift" | "card";

function HorizonTable({ v }: { v: ModelVersion }) {
  const { t, intl } = useT();
  const rows = v.metrics.by_horizon ?? [];
  if (!rows.length) return null;
  return (
    <div className="grid cols-2">
      {(["observed", "hold_last"] as const).map((mode) => (
        <div key={mode}>
          <div className="section-title">{t(`ops.models.mode.${mode}` as Key)}</div>
          <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}>
            <table className="table"><thead><tr><th>h</th><th className="num">n</th><th className="num">MAE</th><th className="num">RMSE</th><th className="num hide-sm">Bias</th><th className="num hide-sm">MASE</th><th className="num hide-sm">Dir.</th></tr></thead>
              <tbody>{rows.filter((r) => r.mode === mode).map((r) => <tr key={r.horizon}><td className="primary-cell">{r.horizon}</td><td className="num">{r.n}</td><td className="num strong">{fmtNum(r.mae, intl)}</td><td className="num">{fmtNum(r.rmse, intl)}</td><td className="num hide-sm">{fmtNum(r.bias, intl)}</td><td className="num hide-sm">{fmtNum(r.mase, intl)}</td><td className="num hide-sm">{r.dir_acc == null ? "-" : `${Math.round(r.dir_acc * 100)}%`}</td></tr>)}</tbody></table>
          </div>
        </div>
      ))}
    </div>
  );
}

function ProductionSummary({ prod }: { prod: ModelVersion }) {
  const { t, intl } = useT();
  const m = prod.metrics;
  return (
    <Panel className="accent-top" title={t("ops.models.production", { v: prod.version })} icon={Rocket}
      subtitle={<>{prod.method ?? prod.candidate} · {prod.rule_based ? t("ops.models.ruleBadge") : t("ops.models.mlBadge")} · {t("ops.models.registered", { date: fromMs(prod.created_at_ms, intl) })}</>}
      actions={<Badge tone="good" icon={CircleCheck}>{t("ops.models.alias")}</Badge>}>
      <div className="grid prod-grid">
        <div className="stack-sm"><div className="section-title">{t("forecast.accuracyTitle")}</div>
          <div className="stat-grid" style={{ gridTemplateColumns: "repeat(2, minmax(0, 1fr))" }}>
            <Stat label={t("ops.models.score")} value={fmtNum(m.score, intl)} hint={prod.selection ?? ""} /><Stat label={t("ops.models.h1mae")} value={fmtNum(m.mae, intl)} hint="USD/bbl" />
            <Stat label={t("ops.models.holdLast")} value={fmtNum(m.hold_last_h1_mae, intl)} hint="USD/bbl" /><Stat label="sMAPE h1" value={m.smape != null ? `${fmtNum(m.smape, intl)} %` : "-"} /></div>
          <div className="small muted">{t("ops.models.origins")}: {prod.backtest_origins ?? "-"} · {t("ops.models.fit")}: {prod.fit_period ?? "-"}</div></div>
        <div className="stack-sm"><div className="section-title">{t("forecast.baselineTitle")} (MAE h1)</div>
          <MetricBars rows={[{ label: <strong>v{prod.version}</strong>, value: m.mae, kind: "model" }, { label: t("ops.models.naive"), value: m.baseline_naive_last_mae, kind: "baseline" }]} />
          <div className="xs muted">{t("ops.models.mode.hold_last")}</div>
          <MetricBars rows={[{ label: <strong>v{prod.version}</strong>, value: m.hold_last_h1_mae, kind: "model" }, { label: t("ops.models.naive"), value: m.baseline_naive_hold_last_h1_mae, kind: "baseline" }]} /></div>
        <div className="stack-sm"><div className="section-title">{t("ops.models.traceability")}</div>
          <dl className="dl small"><dt>{t("ops.models.trainingJob")}</dt><dd className="mono">{prod.training_job_id ?? "-"}</dd><dt>{t("ops.models.commit")}</dt><dd className="mono">{prod.git_commit?.slice(0, 12) ?? "-"}</dd><dt>{t("ops.models.config")}</dt><dd className="mono">{prod.config_sha256?.slice(0, 12) ?? "-"}</dd>
            {Object.entries(prod.datasets).map(([k, d]) => <span key={k} style={{ display: "contents" }}><dt>{k}</dt><dd className="mono">{d.version_id?.slice(0, 20) ?? "-"}</dd></span>)}</dl></div>
      </div>
    </Panel>
  );
}

function Evaluation({ versions, initial }: { versions: ModelVersion[]; initial: string }) {
  const { t, intl, locale } = useT();
  const [a, setA] = useState(initial); const [b, setB] = useState("");
  const ids = [a, b].filter(Boolean);
  const detail = useResource((s) => ids.length > 1 ? api<ModelVersionDetail[]>("/models/compare", { query: { versions: ids.join(",") }, signal: s }) : api<ModelVersionDetail>(`/models/versions/${ids[0]}`, { signal: s }).then((d) => [d]), [ids.join(",")]);
  const data = useMemo(() => detail.data ?? [], [detail.data]);
  const rows = useMemo(() => {
    const byPeriod = new Map<string, Record<string, unknown>>();
    data.forEach((d, i) => d.holdout_predictions.forEach((h) => { const r = byPeriod.get(h.period) ?? { period: h.period, actual: h.actual }; r[`pred_${i}`] = h.predicted; byPeriod.set(h.period, r); }));
    const periods = [...byPeriod.keys()].sort();
    return periods.length ? monthRange(periods[0]!, periods.at(-1)!).map((p) => byPeriod.get(p) ?? { period: p }) : [];
  }, [data]);
  const gaps = useMemo(() => { const z: Zone[] = []; let start: string | null = null, prev: string | null = null; for (const r of rows) { if (r.actual === undefined) { start ??= String(r.period); prev = String(r.period); } else if (start && prev) { z.push({ from: start, to: prev, kind: "gap" }); start = prev = null; } } return z; }, [rows]);
  const colors = [SERIES_COLORS.brent, SERIES_COLORS.dubai];
  const cols: Array<[keyof ModelVersion["metrics"], string]> = [["score", t("ops.models.score")], ["mae", "MAE h1"], ["rmse", "RMSE h1"], ["hold_last_h1_mae", t("ops.models.holdLast")], ["baseline_naive_last_mae", t("ops.models.naive")]];
  return (
    <div className="stack">
      <div className="row">
        <label className="field" style={{ width: 240 }}><span className="field-label">{t("common.version")}</span><select className="select" value={a} onChange={(e) => setA(e.target.value)}>{versions.map((v) => <option key={v.version} value={v.version}>v{v.version} · {v.method ?? v.candidate}</option>)}</select></label>
        <label className="field" style={{ width: 260 }}><span className="field-label">{t("ops.models.compareWith")}</span><select className="select" value={b} onChange={(e) => setB(e.target.value)}><option value="">{t("ops.models.noCompare")}</option>{versions.filter((v) => v.version !== a).map((v) => <option key={v.version} value={v.version}>v{v.version} · {v.method ?? v.candidate}</option>)}</select></label>
      </div>
      {detail.loading && !detail.data ? <Skeleton height={360} /> : detail.error ? <ErrorState error={detail.error} onRetry={detail.reload} /> : (<>
        <TimeSeriesChart ariaLabel={t("ops.models.chartTitle")} data={rows} zones={gaps}
          series={[{ key: "actual", label: t("ops.models.actual"), color: SERIES_COLORS.icp, width: 2.25 }, ...data.map((d, i) => ({ key: `pred_${i}`, label: t("ops.models.predicted", { v: d.version }), color: colors[i] ?? colors[0]! }))]}
          table={<table className="table"><thead><tr><th>{t("common.month")}</th><th className="num">{t("ops.models.actual")}</th>{data.map((d) => <th key={d.version} className="num">v{d.version}</th>)}{data.map((d) => <th key={`e${d.version}`} className="num">{t("ops.models.error", { v: d.version })}</th>)}</tr></thead>
            <tbody>{rows.filter((r) => r.actual !== undefined).map((r) => <tr key={String(r.period)}><td className="primary-cell">{period(String(r.period), locale)}</td><td className="num">{fmtNum(r.actual as number, intl)}</td>{data.map((d, i) => <td key={d.version} className="num">{fmtNum((r[`pred_${i}`] as number) ?? null, intl)}</td>)}{data.map((d, i) => { const p = r[`pred_${i}`] as number | undefined; return <td key={`e${d.version}`} className="num">{p === undefined ? "-" : fmtNum(p - (r.actual as number), intl)}</td>; })}</tr>)}</tbody></table>} />
        <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}><table className="table">
          <thead><tr><th>{t("common.version")}</th>{cols.map(([, l]) => <th key={l} className="num">{l}</th>)}<th className="hide-md">{t("ops.models.origins")}</th><th className="hide-md">{t("ops.models.fit")}</th></tr></thead>
          <tbody>{data.map((d) => <tr key={d.version}><td className="primary-cell">v{d.version}<span className="sub">{d.method ?? d.candidate}{d.legacy ? ` · ${t("ops.models.legacy")}` : ""}</span></td>{cols.map(([k]) => <td key={k} className="num">{fmtNum(d.metrics[k] as number | null, intl)}</td>)}<td className="hide-md">{d.backtest_origins ?? "-"}</td><td className="hide-md">{d.fit_period ?? "-"}</td></tr>)}</tbody></table></div>
        {data[0] && <HorizonTable v={data[0]} />}
        {data[0]?.intervals && (
          <div><div className="section-title">{t("ops.models.intervals")}</div>
            <div className="grid cols-2">{Object.entries(data[0].intervals).map(([mode, hs]) => (
              <div key={mode} className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}><table className="table"><thead><tr><th>{t(`ops.models.mode.${mode}` as Key)}</th>{Object.keys(Object.values(hs)[0] ?? {}).map((lv) => <th key={lv} className="num">{Math.round(Number(lv) * 100)} %</th>)}<th className="num hide-sm">cov. 80 %</th></tr></thead>
                <tbody>{Object.entries(hs).map(([h, lv]) => <tr key={h}><td className="primary-cell">h{h}</td>{Object.values(lv).map((pair, i) => <td key={i} className="num">{fmtNum(pair[0], intl, 1)} … {fmtNum(pair[1], intl, 1)}</td>)}<td className="num hide-sm">{data[0]?.interval_coverage?.[mode]?.[h]?.["0.8"] != null ? `${Math.round((data[0].interval_coverage[mode]![h]!["0.8"] ?? 0) * 100)} %` : "-"}</td></tr>)}</tbody></table></div>))}</div>
            <p className="xs muted mt-2">{t("ops.models.coverageNote")}</p></div>
        )}
      </>)}
    </div>
  );
}

function Lifecycle({ version, isProduction, onChange }: { version: string; isProduction: boolean; onChange: () => void }) {
  const { t } = useT();
  const [reason, setReason] = useState(""); const [force, setForce] = useState(false);
  const [report, setReport] = useState<ValidationReport | null>(null); const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<unknown>(null); const [busy, setBusy] = useState<string | null>(null);
  const act = async (kind: string, fn: () => Promise<void>) => { setBusy(kind); setError(null); setResult(null); try { await fn(); } catch (e) { setError(e); if (e instanceof ApiError && e.details.validation) setReport(e.details.validation as ValidationReport); } finally { setBusy(null); } };
  const reasonOk = reason.trim().length >= 5;
  return (
    <Panel title={t("ops.models.lifecycle", { v: version })} icon={ShieldCheck} subtitle={t("ops.models.lifecycleSub")}>
      <div className="grid cols-2" style={{ alignItems: "start" }}>
        <div className="stack-sm"><div className="section-title">{t("ops.models.validate")}</div><p className="small muted">{t("ops.models.validateBody")}</p>
          <div><Button icon={ShieldCheck} loading={busy === "validate"} disabled={!!busy} onClick={() => act("validate", async () => setReport(await api<ValidationReport>(`/models/versions/${version}/validate`, { method: "POST" })))}>{t("ops.models.runValidation")}</Button></div>
          {report && <div className="stack-sm"><StatusBadge status={report.passed ? "passed" : "failed"} label={report.passed ? t("ops.models.allPassed") : t("ops.models.gatesFailed")} />
            <ul style={{ listStyle: "none", margin: 0, padding: 0, display: "grid", gap: 6 }}>{report.checks.map((c) => <li key={c.name} className="row small" style={{ flexWrap: "nowrap", alignItems: "flex-start" }}>{c.passed ? <CircleCheck size={16} className="up-text" style={{ flex: "none", marginTop: 2 }} /> : <CircleX size={16} className="down-text" style={{ flex: "none", marginTop: 2 }} />}<span><strong>{c.name.replaceAll("_", " ")}</strong> <span className="muted">{c.detail}</span></span></li>)}</ul></div>}
        </div>
        <div className="stack-sm"><div className="section-title">{t("ops.models.promoteOrRollback")}</div>
          <label className="field"><span className="field-label">{t("ops.models.reason")}</span><input className="input" value={reason} onChange={(e) => setReason(e.target.value)} maxLength={500} /></label>
          <label className="check"><input type="checkbox" checked={force} onChange={(e) => setForce(e.target.checked)} /><span>{t("ops.models.force")}</span></label>
          <div className="row">
            <Button variant="primary" icon={Rocket} loading={busy === "promote"} disabled={!!busy || isProduction || !reasonOk} onClick={() => act("promote", async () => { const r = await api<PromotionResult>(`/models/versions/${version}/promote`, { method: "POST", json: { reason, force } }); setResult(t("ops.models.promoted", { to: r.to_version, from: r.from_version ? `v${r.from_version}` : "-" })); onChange(); })}>{isProduction ? t("ops.models.already") : t("ops.models.promote", { v: version })}</Button>
            <Button variant="danger" icon={Undo2} loading={busy === "rollback"} disabled={!!busy || !reasonOk} onClick={() => act("rollback", async () => { const r = await api<PromotionResult>("/models/rollback", { method: "POST", json: { reason } }); setResult(t("ops.models.rolledBack", { from: r.from_version, to: r.to_version })); onChange(); })}>{t("ops.models.rollback")}</Button>
          </div>
          {!reasonOk && <span className="field-hint">{t("ops.models.reasonHint")}</span>}
          {result && <Callout tone="good">{result}</Callout>}<ErrorState error={error} />
        </div>
      </div>
    </Panel>
  );
}

function DriftView() {
  const { t, intl } = useT();
  const drift = useResource((s) => api<DriftResponse>("/models/drift", { signal: s }), []);
  if (drift.loading && !drift.data) return <Skeleton height={260} />;
  if (drift.error) return <ErrorState error={drift.error} onRetry={drift.reload} />;
  const d = drift.data!;
  const maxScale = Math.max(0.3, d.max_psi * 1.05);
  return (
    <div className="stack">
      <div className="row-between"><div className="row"><StatusBadge status={d.status} /><span className="small muted">max PSI {fmtNum(d.max_psi, intl, 3)} · v{d.model_version} · {d.method}</span></div><span className="small muted">{d.reference_period} ({d.reference_rows}) vs {d.current_period} ({d.current_rows})</span></div>
      <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}><table className="table"><thead><tr><th>{t("ops.models.feature")}</th><th style={{ width: "40%" }}>PSI</th><th>{t("common.status")}</th><th className="num hide-sm">{t("ops.models.refMean")}</th><th className="num hide-sm">{t("ops.models.curMean")}</th></tr></thead>
        <tbody>{Object.entries(d.features).map(([k, f]) => <tr key={k}><td className="primary-cell mono">{k}</td><td><div className="row" style={{ flexWrap: "nowrap" }}><span className="bar-track grow" aria-hidden="true"><span className="bar-fill" style={{ display: "block", width: `${Math.min(100, (f.psi / maxScale) * 100)}%`, background: f.status === "significant" ? "#f04438" : f.status === "moderate" ? "#f79009" : "#12b76a" }} /></span><span className="num strong" style={{ width: 52, textAlign: "right" }}>{fmtNum(f.psi, intl, 3)}</span></div></td><td><StatusBadge status={f.status} /></td><td className="num hide-sm">{fmtNum(f.reference_mean, intl)}</td><td className="num hide-sm">{fmtNum(f.current_mean, intl)}</td></tr>)}</tbody></table></div>
      <Callout tone="info">{t("ops.models.driftNote")} {d.note}</Callout>
    </div>
  );
}

function ModelCard({ version }: { version: string }) {
  const detail = useResource((s) => api<ModelVersionDetail>(`/models/versions/${version}`, { signal: s }), [version]);
  if (detail.loading && !detail.data) return <Skeleton height={300} />;
  if (detail.error) return <ErrorState error={detail.error} />;
  return <pre className="model-card-md">{detail.data?.model_card ?? "-"}</pre>;
}

export default function ModelsOpsPage() {
  const { t, intl } = useT();
  const models = useResource((s) => api<ModelList>("/models", { signal: s }), []);
  const promotions = useResource((s) => api<PromotionList>("/models/promotions", { query: { limit: 20 }, signal: s }), []);
  const [runCursor, setRunCursor] = useState<string | null>(null);
  const runs = useResource((s) => api<ExperimentRunList>("/models/experiments/runs", { query: { limit: 15, cursor: runCursor }, signal: s }), [runCursor]);
  const [tab, setTab] = useState<Tab>("evaluation"); const [selected, setSelected] = useState<string | null>(null);
  const versions = models.data?.versions ?? [];
  const prodVersion = models.data?.production_version ?? null;
  const prod = versions.find((v) => v.version === prodVersion);
  const target = selected ?? prodVersion ?? versions[0]?.version ?? null;
  const refresh = () => { models.reload(); promotions.reload(); };
  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={FlaskConical} title={t("ops.models.title")} description={t("ops.models.desc")} actions={<Link className="btn" to="/operasi/pekerjaan"><Beaker size={16} aria-hidden="true" />{t("ops.models.train")}</Link>} />
      {models.error && <ErrorState error={models.error} onRetry={models.reload} />}
      {models.loading && !models.data ? <Skeleton height={220} /> : prod ? <ProductionSummary prod={prod} /> : <Panel><EmptyState icon={FlaskConical} title={t("ops.models.noProd")}>{t("ops.models.noProdBody")}</EmptyState></Panel>}
      {versions.length > 0 && (
        <section className="panel">
          <div style={{ padding: "4px 22px 0" }}><Tabs label={t("ops.models.title")} value={tab} onChange={setTab} tabs={[
            { value: "evaluation", label: t("ops.models.tab.evaluation"), icon: GitCompare }, { value: "versions", label: t("ops.models.tab.versions"), icon: Layers, count: versions.length },
            { value: "experiments", label: t("ops.models.tab.experiments"), icon: Beaker }, { value: "promotions", label: t("ops.models.tab.promotions"), icon: History, count: promotions.data?.items.length },
            { value: "drift", label: t("ops.models.tab.drift"), icon: Activity }, { value: "card", label: t("ops.models.tab.card"), icon: BookOpen }]} /></div>
          <div className={tab === "versions" || tab === "experiments" ? "" : "panel-body"} style={{ paddingTop: 20 }}>
            {tab === "evaluation" && target && <Evaluation key={target} versions={versions} initial={target} />}
            {tab === "versions" && <div className="table-scroll"><table className="table"><thead><tr><th>{t("common.version")}</th><th className="hide-sm">{t("common.method")}</th><th className="num">{t("ops.models.score")}</th><th className="num">MAE h1</th><th className="num hide-sm">{t("ops.models.naive")}</th><th className="hide-md">{t("ops.models.origins")}</th><th className="hide-md">{t("common.created")}</th><th /></tr></thead>
              <tbody>{versions.map((v) => <tr key={v.version} className={`clickable${target === v.version ? " selected" : ""}`} onClick={() => setSelected(v.version)}><td className="primary-cell">v{v.version} {v.aliases.includes(models.data!.alias) && <Badge tone="good">{t("ops.models.alias")}</Badge>}{v.legacy && <Badge tone="warn">{t("ops.models.legacy")}</Badge>}<span className="sub mono">{v.training_job_id ?? ""}</span></td><td className="hide-sm">{v.method ?? v.candidate} {v.rule_based != null && <Badge tone={v.rule_based ? "brand" : "neutral"}>{v.rule_based ? t("ops.models.ruleBadge") : t("ops.models.mlBadge")}</Badge>}</td><td className="num strong">{fmtNum(v.metrics.score, intl)}</td><td className="num">{fmtNum(v.metrics.mae, intl)}</td><td className="num hide-sm">{fmtNum(v.metrics.baseline_naive_last_mae, intl)}</td><td className="hide-md">{v.backtest_origins ?? "-"}</td><td className="hide-md">{fromMs(v.created_at_ms, intl)}</td><td className="num"><span className="link-btn small">{target === v.version ? "✓" : t("forecast.viewRun")}</span></td></tr>)}</tbody></table></div>}
            {tab === "experiments" && (runs.error ? <div className="panel-body"><ErrorState error={runs.error} onRetry={runs.reload} /></div> : !runs.data ? <div className="panel-body"><Skeleton height={200} /></div> : (<>
              <div className="table-scroll"><table className="table"><thead><tr><th>{t("ops.models.tab.experiments")}</th><th className="hide-sm">{t("common.method")}</th><th className="num">{t("ops.models.score")}</th><th className="num hide-sm">MAE h1</th><th className="num hide-sm">{t("ops.models.holdLast")}</th><th className="hide-md">{t("ops.models.origins")}</th><th className="hide-md">{t("ops.models.trainingJob")}</th><th className="hide-md">{t("ops.jobs.started")}</th></tr></thead>
                <tbody>{runs.data.items.map((r) => <tr key={r.run_id}><td className="primary-cell">{r.candidate}<span className="sub mono">{r.run_id.slice(0, 12)}</span></td><td className="hide-sm">{r.method_family ?? "-"} {r.rule_based && <Badge tone="brand">{t("ops.models.ruleBadge")}</Badge>}</td><td className="num strong">{fmtNum(r.metrics.score, intl)}</td><td className="num hide-sm">{fmtNum(r.metrics.mae, intl)}</td><td className="num hide-sm">{fmtNum(r.metrics.hold_last_h1_mae, intl)}</td><td className="hide-md">{r.backtest_origins ?? "-"}</td><td className="hide-md mono small">{r.training_job_id ?? "-"}</td><td className="hide-md">{fromMs(r.start_time_ms, intl)}</td></tr>)}</tbody></table></div>
              <Pager hasPrev={!!runCursor} hasNext={!!runs.data.next_cursor} onFirst={() => setRunCursor(null)} onNext={() => setRunCursor(runs.data?.next_cursor ?? null)} /></>))}
            {tab === "promotions" && (promotions.error ? <ErrorState error={promotions.error} /> : !promotions.data?.items.length ? <EmptyState icon={History} title={t("ops.models.noPromotions")} /> : (
              <ol className="timeline">{promotions.data.items.map((p) => <li key={p.id}><span className={`tl-icon${p.action === "rollback" ? " rollback" : ""}`}>{p.action === "rollback" ? <Undo2 size={14} /> : <Rocket size={14} />}</span><div><div className="row"><span className="strong">{p.action} {p.from_version ? `v${p.from_version}` : "-"} → v{p.to_version}</span>{p.forced && <Badge tone="warn">{t("ops.models.forced")}</Badge>}</div><div className="small muted">{dateTime(p.created_at, intl)} · {p.actor}</div><div className="small mt-2">“{p.reason}”</div></div></li>)}</ol>))}
            {tab === "drift" && <DriftView />}
            {tab === "card" && target && <ModelCard version={target} />}
          </div>
        </section>
      )}
      {target && <Lifecycle version={target} isProduction={target === prodVersion} onChange={refresh} />}
    </div>
  );
}
