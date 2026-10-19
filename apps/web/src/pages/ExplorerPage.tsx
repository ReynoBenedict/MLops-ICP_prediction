/** Eksplorasi Harga: history ranges, benchmark comparison, spreads, FX, monthly table, provenance. */
import type { ActiveDatasets, EvidenceResponse, HistoryResponse } from "../api/types";
import { ChartLine, Database, Download, Fingerprint, Table2 } from "lucide-react";
import { useMemo, useState } from "react";
import { api, downloadWithAuth } from "../api/client";
import { useResource } from "../api/hooks";
import { CoverageGrid, SERIES_COLORS, TimeSeriesChart } from "../components/charts";
import { Badge, Button, Delta, ErrorState, KindChip, PageHeader, Panel, Segmented, Skeleton, StatusBadge } from "../components/ui";
import { dateTime, downloadText, fmtIdr, fmtNum, period, rangeText, shiftPeriod, toCsv } from "../format";
import { useT } from "../i18n";
import { filterRange, gapZones, mean, spreadSeries, type Key } from "../lib/market";

type Range = 1 | 3 | 5 | "all";
type Bench = "brent" | "dubai" | "wti";
const BENCH_KEY: Record<Bench, Key> = { brent: "brent_price", dubai: "dubai_price", wti: "wti_price" };
const BENCH_LABEL: Record<Bench, string> = { brent: "Brent", dubai: "Dubai", wti: "WTI" };

export default function ExplorerPage() {
  const { t, td, locale, intl } = useT();
  const [chosenRange, setRange] = useState<Range>(5);
  const [bench, setBench] = useState<Bench>("brent");
  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const active = useResource((s) => api<ActiveDatasets>("/datasets/active", { signal: s }), []);
  const evidence = useResource((s) => api<EvidenceResponse>("/data/evidence", { query: { dataset: "icp" }, signal: s }), []);

  const all = useMemo(() => history.data?.points ?? [], [history.data]);
  const planYears = history.data?.history_years ?? null;
  // A range longer than the plan's history would silently show less: fall back to "all available".
  const range: Range = planYears !== null && chosenRange !== "all" && chosenRange > planYears ? "all" : chosenRange;
  const icpRange = useMemo(() => {
    const icpOnly = all.filter((p) => p.icp_price != null);
    if (!icpOnly.length) return [] as typeof all;
    const first = icpOnly[0]!.period;
    const last = all.at(-1)!.period;
    return filterRange(all.filter((p) => p.period >= first && p.period <= last), range);
  }, [all, range]);
  const zones = useMemo(() => gapZones(icpRange, "icp_price"), [icpRange]);
  const spreads = useMemo(() => spreadSeries(icpRange, BENCH_KEY[bench]), [icpRange, bench]);
  const spread12 = useMemo(() => mean(spreads.slice(-12).map((x) => x.spread)), [spreads]);
  const tableRows = useMemo(() => [...icpRange].reverse().map((p, i, arr) => {
    const prev = arr[i + 1];
    const chg = p.icp_price != null && prev?.icp_price != null && prev.period === shiftPeriod(p.period, -1) ? ((p.icp_price - prev.icp_price) / prev.icp_price) * 100 : null;
    return { ...p, chg };
  }), [icpRange]);
  const evidenceByPeriod = useMemo(() => {
    const m = new Map<string, { status: string; reference: string | null; url: string }>();
    for (const e of evidence.data?.items ?? []) {
      const cur = m.get(e.period);
      if (!cur || e.verification_status === "verified") m.set(e.period, { status: e.verification_status, reference: e.reference, url: e.source_url });
    }
    return m;
  }, [evidence.data]);
  const icpProv = active.data?.datasets.icp;
  const presence = useMemo(() => new Map(all.filter((p) => p.period >= (icpProv?.active_version?.period_start ?? "")).map((p) => [p.period, p.icp_price != null])), [all, icpProv]);

  const [dl, setDl] = useState<{ busy: string | null; error: unknown }>({ busy: null, error: null });
  const downloadDataset = async (id: string, dataset: string, version: number) => {
    if (dl.busy) return;
    setDl({ busy: id, error: null });
    try { await downloadWithAuth(`/datasets/versions/${id}/download`, undefined, `icp-intelligence_${dataset}_v${version}.csv`); setDl({ busy: null, error: null }); }
    catch (e) { setDl({ busy: null, error: e }); }
  };
  const exportCsv = () => downloadText("icp-eksplorasi.csv", toCsv(["period", "icp_usd", "brent_usd", "dubai_usd", "wti_usd", "usd_idr", "icp_idr", "mom_pct"],
    tableRows.map((r) => [r.period, r.icp_price ?? null, r.brent_price ?? null, r.dubai_price ?? null, r.wti_price ?? null, r.usd_idr ?? null, r.icp_idr ?? null, r.chg == null ? null : Number(r.chg.toFixed(2))])));

  return (
    <div className="page">
      <PageHeader eyebrow={t("explorer.eyebrow")} icon={ChartLine} title={t("explorer.title")} description={t("explorer.desc")}
        actions={<>
          <Segmented label={t("explorer.range")} value={range} onChange={setRange} options={([1, 3, 5] as const).filter((n) => planYears === null || n <= planYears).map((n) => ({ value: n as Range, label: t("explorer.years", { n }) })).concat([{ value: "all", label: t("common.all") }])} />
          <Segmented label={t("explorer.compare")} value={bench} onChange={setBench} options={(["brent", "dubai", "wti"] as Bench[]).map((b) => ({ value: b, label: BENCH_LABEL[b] }))} />
          <button className="btn" onClick={exportCsv} disabled={!history.data}><Download size={16} aria-hidden="true" />{t("common.downloadCsv")}</button>
        </>} />
      {history.error && <ErrorState error={history.error} onRetry={history.reload} />}
      {planYears !== null && history.data?.earliest_period && <p className="small muted">{t("history.clipped", { period: period(history.data.earliest_period, locale, true), n: planYears })}</p>}

      <Panel title={t("explorer.priceChart")} icon={ChartLine}>
        {history.loading && !history.data ? <Skeleton height={340} /> : !history.data ? <p className="muted small">{t("common.dataUnavailable")}</p> : (
          <TimeSeriesChart ariaLabel={t("explorer.priceChart")} data={icpRange as unknown as Array<Record<string, unknown>>} zones={zones}
            series={[{ key: "icp_price", label: "ICP", color: SERIES_COLORS.icp, width: 2.25 }, { key: BENCH_KEY[bench], label: BENCH_LABEL[bench], color: SERIES_COLORS[bench] }]} />
        )}
      </Panel>

      <div className="grid cols-2">
        <Panel title={t("explorer.spreadChart")} icon={ChartLine} subtitle={t("explorer.spreadNote", { mean: spread12 == null ? "-" : `${spread12 > 0 ? "+" : ""}${fmtNum(spread12, intl)} USD/bbl` })}>
          {history.loading && !history.data ? <Skeleton height={260} /> : !history.data ? <p className="muted small">{t("common.dataUnavailable")}</p> : (
            <TimeSeriesChart ariaLabel={t("explorer.spreadChart")} height="sm" unit="spread" data={spreads as unknown as Array<Record<string, unknown>>}
              series={[{ key: "spread", label: t("overview.spread", { b: BENCH_LABEL[bench] }), color: SERIES_COLORS.forecast }]} />
          )}
        </Panel>
        <Panel title={t("explorer.fxChart")} icon={ChartLine} subtitle={t("explorer.fxNote")}>
          {history.loading && !history.data ? <Skeleton height={260} /> : !history.data ? <p className="muted small">{t("common.dataUnavailable")}</p> : icpRange.some((p) => p.icp_idr != null) ? (
            <TimeSeriesChart ariaLabel={t("explorer.fxChart")} height="sm" unit="idr" data={icpRange as unknown as Array<Record<string, unknown>>}
              series={[{ key: "icp_idr", label: t("common.idrPerBbl"), color: SERIES_COLORS.wti }]} />
          ) : <p className="muted small">{t("explorer.noFx")}</p>}
        </Panel>
      </div>

      <Panel title={t("explorer.tableTitle")} icon={Table2} subtitle={t("explorer.tableSub")} flush>
          <div className="table-scroll tall">
            <table className="table table-sticky">
              <thead><tr><th>{t("common.month")}</th><th className="num">ICP</th><th className="num">{t("common.change")}</th><th className="num">Brent</th><th className="num">Dubai</th><th className="num">WTI</th><th className="num">{t("explorer.fxRate")}</th><th className="num">{t("common.idrPerBbl")} ({t("common.derived").toLowerCase()})</th><th>{t("common.status")}</th></tr></thead>
              <tbody>{tableRows.map((r) => {
                const ev = evidenceByPeriod.get(r.period);
                return (
                  <tr key={r.period}>
                    <td className="primary-cell nowrap">{period(r.period, locale)}</td>
                    <td className="num">{r.icp_price == null ? <Badge tone="warn">{t("common.missing")}</Badge> : fmtNum(r.icp_price, intl)}</td>
                    <td className="num">{r.chg == null ? <span className="muted">-</span> : <Delta value={r.chg} />}</td>
                    <td className="num">{fmtNum(r.brent_price, intl)}</td><td className="num">{fmtNum(r.dubai_price, intl)}</td>
                    <td className="num">{fmtNum(r.wti_price, intl)}</td><td className="num">{fmtNum(r.usd_idr, intl, 0)}</td>
                    <td className="num nowrap">{r.icp_idr == null ? "-" : fmtIdr(r.icp_idr, intl)}</td>
                    <td>{r.icp_price == null ? "" : ev ? <StatusBadge status={ev.status} /> : <KindChip kind="observed" />}</td>
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
      </Panel>
      <div className="grid cols-2 align-start">
          <Panel title={t("explorer.provenance")} icon={Fingerprint} subtitle={t("explorer.provenanceSub")}>
            {active.loading && !active.data ? <Skeleton height={160} /> : active.error ? <ErrorState error={active.error} compact /> : (
              <div className="stack-sm">
                {Object.values(active.data?.datasets ?? {}).map((d) => (
                  <div key={d.dataset} style={{ borderBottom: "1px solid var(--border)", paddingBottom: 10 }}>
                    <div className="row-between"><strong>{d.label_id}</strong>{d.active_version && (() => { const av = d.active_version; return <Button size="sm" variant="ghost" icon={Download} loading={dl.busy === av.id} disabled={!!dl.busy && dl.busy !== av.id}
                      onClick={() => void downloadDataset(av.id, d.dataset, av.version)} aria-label={t("explorer.downloadDataset", { name: d.label_id })} title={t("explorer.downloadDataset", { name: d.label_id })} />; })()}</div>
                    <div className="xs muted">{t("explorer.provider")}: {td("provider", d.dataset, d.provider)}</div>
                    <div className="xs muted">{t("explorer.definition")}: {locale === "id" && d.definition_id ? d.definition_id : d.definition}</div>
                    {d.active_version ? <div className="xs muted">{t("common.period")} {rangeText(period(d.active_version.period_start, locale), period(d.active_version.period_end, locale), locale)} · {t("common.updated").toLowerCase()} {dateTime(d.active_version.created_at, intl)} · <StatusBadge status={d.freshness.status} /></div> : <div className="xs muted">{t("ops.data.noVersion")}</div>}
                  </div>
                ))}
                {dl.error ? <ErrorState error={dl.error} compact /> : null}
                <p className="xs muted">{t("explorer.aggregationNote")}</p>
              </div>
            )}
          </Panel>
          <Panel title={t("explorer.verification")} icon={Database}>
            {icpProv?.active_version?.period_end ? (
              <div className="stack-sm">
                <CoverageGrid points={presence} startYear={Number((icpProv.active_version.period_start ?? "2019").slice(0, 4))} endPeriod={icpProv.active_version.period_end}
                  labels={{ observed: t("common.observed"), missing: t("common.missing"), outside: t("common.none"), title: t("explorer.coverage") }} />
                <div className="legend"><span className="legend-key"><span className="swatch-area" style={{ background: "#8fb8e0" }} />{t("explorer.legendObserved")}</span><span className="legend-key"><span className="swatch-area" style={{ background: "var(--warn-soft)", border: "1.5px dashed #e19a3c" }} />{t("explorer.legendMissing")}</span></div>
                <div className="row" style={{ gap: 6 }}>
                  {(["verified", "single_source", "machine_extracted", "legacy_unverified"] as const).map((st) => {
                    const n = [...evidenceByPeriod.values()].filter((e) => e.status === st).length;
                    return n ? <span key={st} className="row" style={{ gap: 4 }}><StatusBadge status={st} /><span className="xs muted">{n}</span></span> : null;
                  })}
                </div>
              </div>
            ) : <Skeleton height={120} />}
          </Panel>
      </div>
    </div>
  );
}
