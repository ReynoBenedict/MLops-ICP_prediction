import type { ActiveDatasets, DataQualityResponse, DatasetVersionList, EvidenceResponse, HistoryResponse, ImportResponse, IngestionRunList, QualityReport } from "../../api/types";
import { CalendarRange, CircleCheck, Database, Download, FileUp, Fingerprint, History, RotateCcw, ShieldCheck, Upload } from "lucide-react";
import { useMemo, useState, type ChangeEvent } from "react";
import { api, downloadUrl } from "../../api/client";
import { useResource } from "../../api/hooks";
import { CoverageGrid } from "../../components/charts";
import { Badge, Button, Callout, Delta, EmptyState, ErrorState, Kpi, PageHeader, Pager, Panel, Segmented, Skeleton, StatusBadge } from "../../components/ui";
import { dateTime, fmtNum, issueLabel, period, shiftPeriod } from "../../format";
import { useT } from "../../i18n";

const DATASETS = ["icp", "wti", "brent", "dubai", "usd_idr", "usd_idr_eom"] as const;
type DatasetName = (typeof DATASETS)[number];
const INFO_CODES = new Set(["UNSORTED", "VALUE_UNUSUAL"]);
const COL: Record<DatasetName, "icp_price" | "wti_price" | "brent_price" | "dubai_price" | "usd_idr" | "usd_idr_eom"> = { icp: "icp_price", wti: "wti_price", brent: "brent_price", dubai: "dubai_price", usd_idr: "usd_idr", usd_idr_eom: "usd_idr_eom" };

function IssueList({ report }: { report: QualityReport }) {
  const { t, locale } = useT();
  if (!report.errors.length && !report.warnings.length) return <Callout tone="good" title={t("ops.data.noIssues")}>{t("ops.data.noIssuesBody")}</Callout>;
  return (
    <div className="stack-sm">
      {report.errors.map((e) => <Callout key={e.code} tone="bad" title={issueLabel(e.code, locale)}>{e.message}{!!e.periods?.length && <div className="row mt-2" style={{ gap: 6 }}>{e.periods.slice(0, 18).map((p) => <span key={p} className="tag">{p}</span>)}</div>}</Callout>)}
      {report.warnings.map((w) => <Callout key={w.code} tone={INFO_CODES.has(w.code) ? "info" : "warn"} title={issueLabel(w.code, locale)}>{w.message}{!!w.periods?.length && <div className="row mt-2" style={{ gap: 6 }}>{w.periods.slice(0, 18).map((p) => <span key={p} className="tag">{p}</span>)}{w.periods.length > 18 && <span className="xs muted">{t("ops.data.more", { n: w.periods.length - 18 })}</span>}</div>}</Callout>)}
    </div>
  );
}

function ImportPanel({ onDone }: { onDone: () => void }) {
  const { t, locale, intl } = useT();
  const [dataset, setDataset] = useState<DatasetName>("icp");
  const [mode, setMode] = useState<"merge" | "replace">("merge");
  const [csv, setCsv] = useState(""); const [fileName, setFileName] = useState<string | null>(null);
  const [confirm, setConfirm] = useState(false); const [preview, setPreview] = useState<ImportResponse | null>(null);
  const [error, setError] = useState<unknown>(null); const [busy, setBusy] = useState<"preview" | "commit" | null>(null);
  const reset = () => { setPreview(null); setConfirm(false); setError(null); };
  const send = async (commit: boolean) => {
    setBusy(commit ? "commit" : "preview"); setError(null);
    try { const res = await api<ImportResponse>(`/datasets/${dataset}/imports`, { method: "POST", text: csv, contentType: "text/csv", query: { mode, commit, confirm_overwrite: commit && confirm } }); setPreview(res); if (commit && res.committed) onDone(); }
    catch (e) { setError(e); } finally { setBusy(null); }
  };
  const onFile = async (e: ChangeEvent<HTMLInputElement>) => { const f = e.target.files?.[0]; if (f) { setCsv(await f.text()); setFileName(f.name); reset(); } };
  const step = preview?.committed ? 3 : preview?.valid ? 2 : 1;
  return (
    <Panel title={t("ops.data.import")} icon={Upload} subtitle={t("ops.data.importSub")}>
      <div className="stack">
        <div className="steps">{[t("ops.data.step1"), t("ops.data.step2"), t("ops.data.step3")].map((label, i) => (
          <span key={label} style={{ display: "contents" }}>{i > 0 && <span className="step-sep" aria-hidden="true" />}<span className={`step${step === i + 1 ? " active" : step > i + 1 ? " done" : ""}`}><span className="n">{step > i + 1 ? <CircleCheck size={13} /> : i + 1}</span>{label}</span></span>))}</div>
        <div className="grid cols-3">
          <label className="field"><span className="field-label">{t("ops.data.dataset")}</span><select className="select" value={dataset} onChange={(e) => { setDataset(e.target.value as DatasetName); reset(); }}>{DATASETS.map((d) => <option key={d} value={d}>{d}</option>)}</select></label>
          <label className="field"><span className="field-label">{t("ops.data.mode")}</span><select className="select" value={mode} onChange={(e) => { setMode(e.target.value as "merge" | "replace"); reset(); }}><option value="merge">{t("ops.data.modeMerge")}</option><option value="replace">{t("ops.data.modeReplace")}</option></select></label>
          <label className="field"><span className="field-label">{t("ops.data.csvFile")}</span><span className="btn" style={{ justifyContent: "flex-start", overflow: "hidden" }}><FileUp size={16} aria-hidden="true" /><span className="truncate">{fileName ?? t("ops.data.chooseFile")}</span><input type="file" accept=".csv,text/csv" onChange={onFile} className="sr-only" /></span></label>
        </div>
        <label className="field"><span className="field-label">{t("ops.data.paste")}</span><textarea className="textarea" aria-label="CSV" value={csv} onChange={(e) => { setCsv(e.target.value); setFileName(null); reset(); }} placeholder={dataset === "icp" ? "month,year,icp_price\n4,2026,98.50" : `period,${COL[dataset]}\n2026-09,72.40`} /></label>
        <div className="row"><Button icon={ShieldCheck} onClick={() => send(false)} loading={busy === "preview"} disabled={!csv.trim() || !!busy}>{t("ops.data.validate")}</Button>{preview && !preview.committed && <Button variant="ghost" icon={RotateCcw} onClick={reset}>{t("ops.data.startOver")}</Button>}</div>
        <ErrorState error={error} />
        {preview && (
          <div className="stack">
            {preview.committed && <Callout tone="good" title={t("ops.data.committed", { ds: preview.dataset, v: preview.version?.version })}>{t("ops.data.committedBody")}</Callout>}
            {preview.note && <Callout tone="info">{preview.note}</Callout>}
            {preview.diff && <div className="row"><Badge tone="good">{t("ops.data.added", { n: preview.diff.added.length })}</Badge><Badge tone={preview.diff.changed.length ? "warn" : "neutral"}>{t("ops.data.changed", { n: preview.diff.changed.length })}</Badge><Badge tone={preview.diff.removed.length ? "bad" : "neutral"}>{t("ops.data.removed", { n: preview.diff.removed.length })}</Badge><span className="small muted">{t("ops.data.unchanged", { n: preview.diff.unchanged })}</span></div>}
            {!!preview.diff?.changed.length && <div className="table-scroll" style={{ border: "1px solid var(--border)", borderRadius: 10 }}><table className="table"><thead><tr><th>{t("common.month")}</th><th className="num">{t("ops.data.current")}</th><th className="num">{t("ops.data.new")}</th><th className="num">{t("common.change")}</th></tr></thead>
              <tbody>{preview.diff.changed.map((c) => <tr key={c.period}><td className="primary-cell">{period(c.period, locale)}</td><td className="num">{fmtNum(c.old, intl)}</td><td className="num">{fmtNum(c.new, intl)}</td><td className="num"><Delta value={((c.new - c.old) / c.old) * 100} digits={2} /></td></tr>)}</tbody></table></div>}
            <div><div className="section-title">{t("ops.data.uploadedRows")}</div><IssueList report={preview.upload_report} /></div>
            {preview.valid && !preview.committed && (
              <div className="callout" style={{ alignItems: "center" }}>
                <div className="grow">{preview.requires_confirmation ? <label className="check"><input type="checkbox" checked={confirm} onChange={(e) => setConfirm(e.target.checked)} /><span>{t("ops.data.confirmOverwrite")}</span></label> : <span className="small">{t("ops.data.readyCommit", { ds: dataset })}</span>}</div>
                <Button variant={preview.requires_confirmation ? "danger-solid" : "primary"} icon={Upload} loading={busy === "commit"} disabled={!!busy || (preview.requires_confirmation && !confirm)} onClick={() => send(true)}>{t("ops.data.commit")}</Button>
              </div>
            )}
          </div>
        )}
      </div>
    </Panel>
  );
}

export default function DataOpsPage() {
  const { t, locale, intl } = useT();
  const [dataset, setDataset] = useState<DatasetName>("icp");
  const [cursor, setCursor] = useState<string | null>(null);
  const [actionError, setActionError] = useState<unknown>(null);
  const [activating, setActivating] = useState<string | null>(null);
  const active = useResource((s) => api<ActiveDatasets>("/datasets/active", { signal: s }), []);
  const quality = useResource((s) => api<DataQualityResponse>("/data/quality", { query: { dataset }, signal: s }), [dataset]);
  const versions = useResource((s) => api<DatasetVersionList>("/datasets/versions", { query: { dataset, limit: 8, cursor }, signal: s }), [dataset, cursor]);
  const ingestions = useResource((s) => api<IngestionRunList>("/datasets/ingestions", { query: { limit: 10 }, signal: s }), []);
  const evidence = useResource((s) => api<EvidenceResponse>("/data/evidence", { query: { dataset }, signal: s }), [dataset]);
  const prov = active.data?.datasets[dataset];
  const v = prov?.active_version;
  const end = v?.period_end ?? null;
  const coverageStart = v?.period_start && end ? (v.period_start > shiftPeriod(end, -119) ? v.period_start : shiftPeriod(end, -119)) : null;
  const history = useResource((s) => coverageStart && end ? api<HistoryResponse>("/data/history", { query: { start: coverageStart, end }, signal: s }) : Promise.resolve({ points: [] } as HistoryResponse), [coverageStart, end]);
  const key = COL[dataset];
  const presence = useMemo(() => new Map((history.data?.points ?? []).map((p) => [p.period, p[key] != null])), [history.data, key]);
  const report = quality.data?.report;
  const refreshAll = () => { active.reload(); quality.reload(); versions.reload(); history.reload(); ingestions.reload(); evidence.reload(); };
  const activate = async (id: string) => { setActionError(null); setActivating(id); try { await api(`/datasets/versions/${id}/activate`, { method: "POST" }); refreshAll(); } catch (e) { setActionError(e); } finally { setActivating(null); } };
  const evidenceRows = useMemo(() => [...(evidence.data?.items ?? [])].sort((a, b) => b.period.localeCompare(a.period)).slice(0, 120), [evidence.data]);

  return (
    <div className="page">
      <PageHeader eyebrow={t("nav.ops")} icon={Database} title={t("ops.data.title")} description={t("ops.data.desc")}
        actions={<><Segmented label={t("ops.data.dataset")} value={dataset} onChange={(d) => { setDataset(d); setCursor(null); }} options={DATASETS.map((d) => ({ value: d, label: d }))} />
          {v && <a className="btn" href={downloadUrl(`/datasets/versions/${v.id}/download`)} download><Download size={16} aria-hidden="true" />{t("common.downloadCsv")}</a>}</>} />
      {active.error && <ErrorState error={active.error} onRetry={active.reload} />}
      <div className="grid cols-4">
        <Kpi label={t("ops.data.coverage")} icon={CalendarRange} iconTone="brand" value={v ? `${v.row_count}` : active.loading ? <Skeleton height={34} width={90} /> : "-"} unit={t("explorer.months", { n: "" }).trim()} meta={v ? `${period(v.period_start, locale)} – ${period(v.period_end, locale)}` : t("ops.data.noVersion")} />
        <Kpi label={t("ops.data.freshness")} icon={History} iconTone="brand" tone={prov?.freshness.status === "stale" ? "warn" : undefined} value={prov ? <StatusBadge status={prov.freshness.status} /> : <Skeleton height={28} width={90} />}
          meta={prov?.freshness.months_behind ? t("ops.data.monthsBehind", { n: prov.freshness.months_behind, period: period(prov.freshness.expected_period, locale) }) : prov ? t("ops.data.inLine") : undefined} />
        <Kpi label={t("ops.data.quality")} icon={ShieldCheck} iconTone="brand" tone={report && !report.ok ? "bad" : undefined} value={report ? <StatusBadge status={report.ok ? "passed" : "failed"} /> : <Skeleton height={28} width={90} />}
          meta={report ? t("ops.data.qualityMeta", { e: report.errors.length, w: report.warnings.length, m: report.missing_periods.length }) : undefined} />
        <Kpi label={t("ops.data.activeVersion")} icon={Fingerprint} iconTone="brand" value={v ? `v${v.version}` : "-"} meta={v ? <>{v.source} · {dateTime(v.created_at, intl)}</> : undefined} />
      </div>
      <div className="grid split-main">
        <Panel title={t("ops.data.coverageTitle")} icon={CalendarRange} subtitle={coverageStart && end ? `${period(coverageStart, locale)} – ${period(end, locale)} ${dataset !== "icp" ? t("ops.data.last10") : ""}` : undefined}>
          {history.loading && !history.data?.points.length ? <Skeleton height={220} /> : history.error ? <ErrorState error={history.error} /> : coverageStart && end ? (
            <div className="stack-sm"><CoverageGrid points={presence} startYear={Number(coverageStart.slice(0, 4))} endPeriod={end} labels={{ observed: t("common.observed"), missing: t("common.missing"), outside: t("common.none"), title: t("ops.data.coverageTitle") }} />
              <div className="legend"><span className="legend-key"><span className="swatch-area" style={{ background: "#8fb8e0" }} />{t("explorer.legendObserved")}</span><span className="legend-key"><span className="swatch-area" style={{ background: "var(--warn-soft)", border: "1.5px dashed #e19a3c" }} />{t("explorer.legendMissing")}</span></div>
              {report && <div className="stat-grid mt-3" style={{ borderTop: "1px solid var(--border)", paddingTop: 16 }}>{(["latest", "min", "max", "mean", "std"] as const).map((k) => <div key={k} className="stat"><span className="stat-label">{k}</span><span className="stat-value" style={{ fontSize: 17 }}>{fmtNum(report.stats[k] ?? null, intl)}</span></div>)}</div>}
            </div>) : <EmptyState title={t("ops.data.noVersion")} />}
        </Panel>
        <Panel title={t("ops.data.qualityTitle")} icon={ShieldCheck}>{quality.loading && !report ? <Skeleton height={160} /> : quality.error ? <ErrorState error={quality.error} onRetry={quality.reload} /> : report && <IssueList report={report} />}</Panel>
      </div>
      <div className="grid split-main">
        <Panel title={t("ops.data.versions")} icon={History} subtitle={t("ops.data.versionsSub")} flush actions={<Button size="sm" variant="ghost" icon={RotateCcw} onClick={versions.reload}>{t("common.refresh")}</Button>}>
          {actionError ? <div className="panel-body"><ErrorState error={actionError} /></div> : null}
          {versions.error ? <div className="panel-body"><ErrorState error={versions.error} onRetry={versions.reload} /></div> : !versions.data ? <div className="panel-body"><Skeleton height={140} /></div> : (
            <div className="table-scroll"><table className="table">
              <thead><tr><th>{t("common.version")}</th><th>{t("ops.data.coverage")}</th><th className="hide-sm">{t("common.source")}</th><th>{t("ops.data.quality")}</th><th className="hide-md">{t("common.created")}</th><th /></tr></thead>
              <tbody>{versions.data.items.map((ver) => (
                <tr key={ver.id} className={ver.is_active ? "selected" : ""}>
                  <td className="primary-cell">v{ver.version} {ver.is_active && <Badge tone="good">{t("status.active")}</Badge>}<span className="sub mono">{ver.sha256.slice(0, 12)}</span></td>
                  <td>{period(ver.period_start, locale)} – {period(ver.period_end, locale)}<span className="sub">{t("ops.data.rows", { n: ver.row_count })}</span></td>
                  <td className="hide-sm">{ver.source.replaceAll("_", " ")}{ver.diff && <span className="sub">+{ver.diff.added.length} · ~{ver.diff.changed.length} · −{ver.diff.removed.length}</span>}</td>
                  <td><StatusBadge status={ver.quality.ok ? "passed" : "failed"} /></td>
                  <td className="hide-md">{dateTime(ver.created_at, intl)}<span className="sub">{ver.created_by}</span></td>
                  <td className="num"><div className="row" style={{ justifyContent: "flex-end", flexWrap: "nowrap" }}><a className="btn sm ghost" href={downloadUrl(`/datasets/versions/${ver.id}/download`)} download aria-label={`${t("common.download")} v${ver.version}`}><Download size={15} /></a>{!ver.is_active && ver.quality.ok && <Button size="sm" onClick={() => activate(ver.id)} loading={activating === ver.id}>{t("ops.data.activate")}</Button>}</div></td>
                </tr>))}</tbody></table></div>
          )}
          <Pager hasPrev={!!cursor} hasNext={!!versions.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(versions.data?.next_cursor ?? null)} />
        </Panel>
        <Panel title={t("ops.data.provenance")} icon={Fingerprint} subtitle={prov ? `${prov.label_id} · ${prov.provider}` : undefined}>
          {v ? <dl className="dl"><dt>{t("ops.data.versionId")}</dt><dd className="mono">{v.id}</dd><dt>{t("common.source")}</dt><dd>{v.source.replaceAll("_", " ")}{v.source_detail ? <span className="muted"> · {v.source_detail}</span> : null}</dd><dt>{t("explorer.definition")}</dt><dd>{prov?.definition}</dd>
            <dt>{t("common.created")}</dt><dd>{dateTime(v.created_at, intl)} · {v.created_by}</dd><dt>SHA-256</dt><dd className="mono xs">{v.sha256}</dd><dt>{t("ops.data.size")}</dt><dd>{(v.size_bytes / 1024).toFixed(1)} KB · {t("ops.data.rows", { n: v.row_count })}</dd><dt>{t("ops.data.parent")}</dt><dd className="mono">{v.parent_version_id ?? "-"}</dd></dl> : <Skeleton height={140} />}
        </Panel>
      </div>
      <div className="grid split-main">
        <Panel title={t("ops.data.evidence")} icon={Fingerprint} subtitle={t("ops.data.evidenceSub")} flush>
          {evidence.error ? <div className="panel-body"><ErrorState error={evidence.error} /></div> : !evidence.data ? <div className="panel-body"><Skeleton height={140} /></div> : !evidenceRows.length ? <EmptyState title={t("common.none")} /> : (
            <div className="table-scroll tall"><table className="table"><thead><tr><th>{t("common.month")}</th><th className="num">{t("common.value")}</th><th>{t("common.status")}</th><th className="hide-sm">{t("explorer.evidenceRef")}</th><th className="hide-md">{t("explorer.evidenceDoc")}</th></tr></thead>
              <tbody>{evidenceRows.map((e) => <tr key={e.id}><td className="primary-cell nowrap">{period(e.period, locale)}</td><td className="num">{fmtNum(e.value, intl)}</td><td><StatusBadge status={e.verification_status} /><span className="sub">{e.kind}</span></td><td className="hide-sm small">{e.reference ?? "-"}</td><td className="hide-md xs"><a href={e.source_url} target="_blank" rel="noreferrer" className="truncate" style={{ display: "inline-block", maxWidth: 260 }}>{e.source_url}</a></td></tr>)}</tbody></table></div>
          )}
        </Panel>
        <Panel title={t("ops.data.ingestions")} icon={History} flush>
          {ingestions.error ? <div className="panel-body"><ErrorState error={ingestions.error} /></div> : !ingestions.data ? <div className="panel-body"><Skeleton height={140} /></div> : !ingestions.data.items.length ? <EmptyState title={t("common.none")} /> : (
            <div className="table-scroll"><table className="table"><thead><tr><th>{t("ops.data.dataset")}</th><th>{t("common.status")}</th><th className="hide-sm">{t("ops.data.connector")}</th><th>{t("ops.jobs.finished")}</th></tr></thead>
              <tbody>{ingestions.data.items.map((r) => <tr key={r.id}><td className="primary-cell">{r.dataset}<span className="sub mono">{r.version_id ?? "-"}</span></td><td><StatusBadge status={r.status} /></td><td className="hide-sm small">{r.connector}</td><td className="small">{dateTime(r.finished_at, intl)}<span className="sub">{r.requested_by}</span></td></tr>)}</tbody></table></div>
          )}
        </Panel>
      </div>
      <ImportPanel onDone={refreshAll} />
    </div>
  );
}
