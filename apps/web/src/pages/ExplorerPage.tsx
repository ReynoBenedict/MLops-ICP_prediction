/**
 * Riwayat Harga: history ranges, benchmark comparison, spreads, FX, monthly table, provenance.
 * View state lives in the URL (`?rentang=1|3|5|semua&acuan=brent|dubai|wti&bulan=YYYY-MM`) so Back,
 * reload and shared links restore it. The monthly table grows with the page (no inner scroller,
 * header sticky under the top bar) while it fits its panel; in a narrower frame the evidence column
 * folds under the month, and a table that still does not fit scrolls inside its frame with an edge
 * cue (never spilling out of the panel). It shows 12 months with "Tampilkan semua" and becomes
 * record cards on phones.
 * `#bukti` is the per-month verification panel: the top bar's data status links there, and the
 * page scrolls to it and moves focus to its heading once the evidence has loaded.
 */
import type { ActiveDatasets, HistoryResponse } from "../api/types";
import { ChartLine, Database, Download, Fingerprint, Table2 } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useLocation, useSearchParams } from "react-router-dom";
import { api, downloadWithAuth } from "../api/client";
import { useResource } from "../api/hooks";
import { CoverageGrid, SERIES_COLORS, TimeSeriesChart } from "../components/charts";
import { Badge, Button, Delta, ErrorState, PageHeader, Panel, Segmented, Skeleton, SlowNotice, StatusBadge, TableScroll, ViewBar, ViewField, focusHeading, scrollToResult, useMediaQuery, useTableFrame } from "../components/ui";
import { dateTime, downloadText, fmtIdr, fmtNum, period, rangeText, shiftPeriod, toCsv } from "../format";
import { useT } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/market.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { filterRange, gapZones, mean, spreadSeries, type Key as SeriesKey } from "../lib/market";
import { EvidenceBadge, EvidenceCell, useVerification } from "../lib/verification";
import { labelFor, type Basis, type LabelEn } from "../api/product-types";

type Range = 1 | 3 | 5 | "all";
type Bench = "brent" | "dubai" | "wti";
const BENCH_KEY: Record<Bench, SeriesKey> = { brent: "brent_price", dubai: "dubai_price", wti: "wti_price" };
const BENCH_LABEL: Record<Bench, string> = { brent: "Brent", dubai: "Dubai", wti: "WTI" };
const BASES: Basis[] = ["two_documents", "decree_text", "single_document", "document_plus_legacy", "ocr_single", "conflict", "no_evidence"];
const PAGE = 12;
/**
 * Frame width (px) below which the monthly table is compact: the evidence column folds under the
 * month and long headers wrap. The full nine columns need about 1030 px (Indonesian and English).
 */
const TABLE_COMPACT_BELOW = 1080;

const parseRange = (v: string | null): Range | null => (v === "semua" ? "all" : v === "1" ? 1 : v === "3" ? 3 : v === "5" ? 5 : null);
const parseBench = (v: string | null): Bench => (v === "dubai" || v === "wti" ? v : "brent");

export default function ExplorerPage() {
  const { t, td, locale, intl } = useT();
  const [params, setParams] = useSearchParams();
  const chosenRange: Range = parseRange(params.get("rentang")) ?? 5;
  const bench = parseBench(params.get("acuan"));
  const focus = /^\d{4}-\d{2}$/.test(params.get("bulan") ?? "") ? params.get("bulan") : null;
  const setParam = (k: string, v: string | null) => {
    const next = new URLSearchParams(params);
    if (v === null) next.delete(k); else next.set(k, v);
    setParams(next, { replace: true });
  };
  const setRange = (r: Range) => setParam("rentang", r === "all" ? "semua" : String(r));
  const setBench = (b: Bench) => setParam("acuan", b === "brent" ? null : b);
  const [showAll, setShowAll] = useState(false);
  // Monthly table layout: compact in a narrow frame (phones keep their record cards); the header
  // sticks to the page only while the table fits its frame, otherwise it scrolls inside the frame.
  const [tableRef, frame] = useTableFrame<HTMLTableElement>(TABLE_COMPACT_BELOW);
  const phone = useMediaQuery("(max-width: 560px)");
  const compact = frame.narrow && !phone;

  const history = useResource((s) => api<HistoryResponse>("/data/history", { signal: s }), []);
  const active = useResource((s) => api<ActiveDatasets>("/datasets/active", { signal: s }), []);
  const verification = useVerification("icp");

  // Deep link to the evidence panel (#bukti): scroll and focus once its content is there. Keyed by
  // the navigation, so following the link again from this page scrolls again.
  const loc = useLocation();
  const evidenceSettled = verification.ready || !!verification.error;
  useEffect(() => {
    if (loc.hash !== "#bukti" || !evidenceSettled) return;
    // After the shell's own route handling (scroll to top, focus on the page heading).
    const id = window.setTimeout(() => {
      const el = document.getElementById("bukti");
      if (!el) return;
      scrollToResult(el);
      focusHeading(el);
    }, 150);
    return () => window.clearTimeout(id);
  }, [loc.hash, loc.key, evidenceSettled]);

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
  // A deep-linked month (from a notification) is always visible, even beyond the first page.
  const focusIndex = focus ? tableRows.findIndex((r) => r.period === focus) : -1;
  const visibleRows = showAll || tableRows.length <= PAGE + 2 ? tableRows : tableRows.slice(0, Math.max(PAGE, focusIndex + 1));
  const icpProv = active.data?.datasets.icp;
  const firstObserved = all.find((p) => p.icp_price != null)?.period ?? null;
  const presence = useMemo(() => new Map(all.filter((p) => p.period >= (icpProv?.active_version?.period_start ?? "")).map((p) => [p.period, p.icp_price != null])), [all, icpProv]);
  // Counts in the plan's range only: every ICP month either has an evidence basis or "no evidence".
  const counts = useMemo(() => {
    const out: Partial<Record<Basis, number>> = {};
    for (const p of all) if (p.icp_price != null) { const b = verification.forPeriod(p.period).basis; out[b] = (out[b] ?? 0) + 1; }
    return out;
  }, [all, verification]);

  const [dl, setDl] = useState<{ busy: string | null; error: unknown }>({ busy: null, error: null });
  const downloadDataset = async (id: string, dataset: string, version: number) => {
    if (dl.busy) return;
    setDl({ busy: id, error: null });
    try { await downloadWithAuth(`/datasets/versions/${id}/download`, undefined, `icp-intelligence_${dataset}_v${version}.csv`); setDl({ busy: null, error: null }); }
    catch (e) { setDl({ busy: null, error: e }); }
  };
  const exportCsv = () => downloadText("icp-eksplorasi.csv", toCsv(["period", "icp_usd", "brent_usd", "dubai_usd", "wti_usd", "usd_idr", "icp_idr", "mom_pct", "icp_evidence"],
    tableRows.map((r) => [r.period, r.icp_price ?? null, r.brent_price ?? null, r.dubai_price ?? null, r.wti_price ?? null, r.usd_idr ?? null, r.icp_idr ?? null,
      r.chg == null ? null : Number(r.chg.toFixed(2)), r.icp_price == null ? "" : verification.forPeriod(r.period).label])));

  const rangeLabel = (r: Range) => r === "all" ? t("common.all") : t("explorer.years", { n: r });
  const rangeWords = range === "all" ? (planYears !== null ? t("explorer.rangeWordsPlan") : t("explorer.rangeWordsAll")) : range === 1 ? t("explorer.rangeWords1") : t("explorer.rangeWordsN", { n: range });
  const spanText = icpRange.length ? rangeText(period(icpRange[0]!.period, locale), period(icpRange.at(-1)!.period, locale), locale) : null;

  return (
    <div className="page">
      <PageHeader title={t("explorer.title")} description={t("explorer.desc")}
        meta={planYears !== null && history.data?.earliest_period ? t("history.clipped", { period: period(history.data.earliest_period, locale, true), n: planYears }) : undefined} />
      <ViewBar label={t("explorer.viewBar")}>
        <ViewField label={t("explorer.range")}>
          <Segmented label={t("explorer.range")} value={range} onChange={setRange} options={([1, 3, 5] as const).filter((n) => planYears === null || n <= planYears).map((n) => ({ value: n as Range, label: rangeLabel(n) })).concat([{ value: "all", label: t("common.all") }])} />
        </ViewField>
        <ViewField label={t("explorer.compare")}>
          <Segmented label={t("explorer.compare")} value={bench} onChange={setBench} options={(["brent", "dubai", "wti"] as Bench[]).map((b) => ({ value: b, label: BENCH_LABEL[b] }))} />
        </ViewField>
        <div className="view-end"><Button icon={Download} onClick={exportCsv} disabled={!history.data || tableRows.length === 0}>{t("common.downloadCsv")}</Button></div>
      </ViewBar>
      <SlowNotice active={history.loading && !history.data} />
      {history.error && <ErrorState error={history.error} onRetry={history.reload} />}

      <Panel title={t("explorer.chartTitle", { bench: BENCH_LABEL[bench], range: rangeWords })} icon={ChartLine}
        subtitle={<>{spanText && <span className="num">{spanText} · </span>}{t("explorer.benchSource")}</>}>
        {history.loading && !history.data ? <div role="status" aria-label={t("common.loading")}><Skeleton height={340} /></div> : !history.data ? <p className="muted small">{t("common.dataUnavailable")}</p> : (
          <TimeSeriesChart ariaLabel={t("explorer.priceChart")} data={icpRange as unknown as Array<Record<string, unknown>>} zones={zones}
            series={[{ key: "icp_price", label: "ICP", color: SERIES_COLORS.icp, width: 2.25 }, { key: BENCH_KEY[bench], label: BENCH_LABEL[bench], color: SERIES_COLORS[bench] }]} />
        )}
      </Panel>

      <div className="grid cols-2">
        <Panel title={t("explorer.spreadChartBench", { b: BENCH_LABEL[bench] })} icon={ChartLine} subtitle={t("explorer.spreadNote", { mean: spread12 == null ? "-" : `${spread12 > 0 ? "+" : ""}${fmtNum(spread12, intl)} USD/bbl` })}>
          {history.loading && !history.data ? <div role="status" aria-label={t("common.loading")}><Skeleton height={260} /></div> : !history.data ? <p className="muted small">{t("common.dataUnavailable")}</p> : (
            <TimeSeriesChart ariaLabel={t("explorer.spreadChart")} height="sm" unit="spread" data={spreads as unknown as Array<Record<string, unknown>>}
              series={[{ key: "spread", label: t("overview.spread", { b: BENCH_LABEL[bench] }), color: SERIES_COLORS.spread }]} />
          )}
        </Panel>
        <Panel title={t("explorer.fxChart")} icon={ChartLine} subtitle={t("explorer.fxNote")}>
          {history.loading && !history.data ? <div role="status" aria-label={t("common.loading")}><Skeleton height={260} /></div> : !history.data ? <p className="muted small">{t("common.dataUnavailable")}</p> : icpRange.some((p) => p.icp_idr != null) ? (
            <TimeSeriesChart ariaLabel={t("explorer.fxChart")} height="sm" unit="idr" data={icpRange as unknown as Array<Record<string, unknown>>}
              series={[{ key: "icp_idr", label: t("common.idrPerBbl"), color: SERIES_COLORS.idr }]} />
          ) : <p className="muted small">{t("explorer.noFx")}</p>}
        </Panel>
      </div>

      <Panel title={t("explorer.tableTitle")} icon={Table2} subtitle={<>{t("explorer.tableSub")} {focus && <strong>{t("explorer.monthFocus", { period: period(focus, locale, true) })}</strong>}</>} flush>
        {!history.data ? <div className="panel-body">{history.loading ? <div role="status" aria-label={t("common.loading")}><Skeleton height={200} /></div> : <p className="muted small">{t("common.dataUnavailable")}</p>}</div> : (<>
          <TableScroll label={t("explorer.tableTitle")} className={`page-sticky${frame.overflows ? "" : " fits"}`}>
            <table ref={tableRef} className={`table table-sticky rc${compact ? " compact" : ""}`}>
              <caption className="sr-only">{t("explorer.tableTitle")} (USD/bbl)</caption>
              <thead><tr><th>{compact ? <>{t("common.month")} <span className="th-unit">· {t("verify.column")}</span></> : t("common.month")}</th><th className="num">ICP <span className="th-unit">USD/bbl</span></th><th className="num">{t("common.change")}</th><th className="num">Brent</th><th className="num">Dubai</th><th className="num">WTI</th><th className="num">{t("explorer.fxRate")}</th><th className="num">{t("common.idrPerBbl")} ({t("common.derived").toLowerCase()})</th>{!compact && <th>{t("verify.column")}</th>}</tr></thead>
              <tbody>{visibleRows.map((r) => (
                <tr key={r.period} className={r.period === focus ? "selected" : undefined} aria-current={r.period === focus ? "true" : undefined}>
                  <td className={`primary-cell rc-title${compact ? "" : " nowrap"}`}>{period(r.period, locale)}{compact && r.icp_price != null && <span className="fold"><EvidenceCell v={verification.forPeriod(r.period)} /></span>}</td>
                  <td className="num rc-main" data-label="ICP (USD/bbl)">{r.icp_price == null ? <Badge tone="warn">{t("common.missing")}</Badge> : fmtNum(r.icp_price, intl)}</td>
                  <td className="num" data-label={t("common.change")}>{r.chg == null ? <span className="muted">-</span> : <Delta value={r.chg} />}</td>
                  <td className="num" data-label="Brent">{fmtNum(r.brent_price, intl)}</td><td className="num" data-label="Dubai">{fmtNum(r.dubai_price, intl)}</td>
                  <td className="num" data-label="WTI">{fmtNum(r.wti_price, intl)}</td><td className="num" data-label={t("explorer.fxRate")}>{fmtNum(r.usd_idr, intl, 0)}</td>
                  <td className="num nowrap" data-label={`${t("common.idrPerBbl")} (${t("common.derived").toLowerCase()})`}>{r.icp_idr == null ? "-" : fmtIdr(r.icp_idr, intl)}</td>
                  {!compact && <td className="rc-full" data-label={t("verify.column")}>{r.icp_price == null ? "" : <EvidenceCell v={verification.forPeriod(r.period)} />}</td>}
                </tr>
              ))}</tbody>
            </table>
          </TableScroll>
          {tableRows.length > visibleRows.length || showAll ? (
            <div className="table-more">
              <span className="xs muted table-more-note">{t("explorer.tableShown", { n: visibleRows.length, total: tableRows.length })}</span>
              <Button size="sm" onClick={() => setShowAll((v) => !v)} aria-expanded={showAll}>{showAll ? t("common.showLess") : t("common.showAll", { n: tableRows.length })}</Button>
            </div>
          ) : null}
        </>)}
      </Panel>
      <div className="grid cols-2 align-start">
          <Panel title={t("explorer.provenance")} icon={Fingerprint} subtitle={t("explorer.provenanceSub")}>
            {active.loading && !active.data ? <div role="status" aria-label={t("common.loading")}><Skeleton height={160} /></div> : active.error ? <ErrorState error={active.error} compact onRetry={active.reload} /> : (
              <div className="stack-sm">
                {Object.values(active.data?.datasets ?? {}).map((d) => {
                  const name = labelFor(d as typeof d & LabelEn, locale);
                  return (
                  <div key={d.dataset} className="prov-item">
                    <div className="row-between"><strong>{name}</strong>{d.active_version && (() => { const av = d.active_version; return <Button size="sm" variant="ghost" icon={Download} loading={dl.busy === av.id} disabled={!!dl.busy && dl.busy !== av.id}
                      onClick={() => void downloadDataset(av.id, d.dataset, av.version)} aria-label={t("explorer.downloadDataset", { name })} title={t("explorer.downloadDataset", { name })} />; })()}</div>
                    <div className="xs muted">{t("explorer.provider")}: {td("provider", d.dataset, d.provider)}</div>
                    <div className="xs muted">{t("explorer.definition")}: {locale === "id" && d.definition_id ? d.definition_id : d.definition}</div>
                    {d.active_version ? <div className="xs muted">{t("common.period")} {rangeText(period(d.active_version.period_start, locale), period(d.active_version.period_end, locale), locale)} · {t("common.updated").toLowerCase()} {dateTime(d.active_version.created_at, intl)} · <StatusBadge status={d.freshness.status} /></div> : <div className="xs muted">{t("explorer.noVersion")}</div>}
                  </div>
                  );
                })}
                {dl.error ? <ErrorState error={dl.error} compact /> : null}
                <p className="xs muted">{t("meth.sources.aggregationNote")}</p>
              </div>
            )}
          </Panel>
          <Panel id="bukti" title={t("explorer.verification")} icon={Database} subtitle={t("verify.summaryTitle")}>
            {verification.error ? <ErrorState error={verification.error} compact onRetry={verification.reload} /> : active.error ? <p className="muted small">{t("explorer.coverageUnavailable")}</p> : icpProv?.active_version?.period_end ? (
              <div className="stack-sm">
                <CoverageGrid points={presence} startYear={Number((firstObserved ?? icpProv.active_version.period_start ?? "2019").slice(0, 4))} endPeriod={icpProv.active_version.period_end}
                  labels={{ observed: t("explorer.legendObserved"), missing: t("common.missing"), outside: t("common.none"), title: t("explorer.coverage") }} />
                <div className="legend"><span className="legend-key"><span className="swatch-area coverage-present" />{t("explorer.legendObserved")}</span><span className="legend-key"><span className="swatch-area coverage-missing" />{t("explorer.legendMissing")}</span></div>
                <ul className="verify-counts">
                  {BASES.map((b) => counts[b] ? <li key={b}><EvidenceBadge v={{ basis: b, ...verification.labelOf(b) }} /><span className="num strong">{counts[b]}</span></li> : null)}
                </ul>
                {verification.dissentMonths > 0 && <p className="xs muted">{t("verify.dissentMonths", { n: verification.dissentMonths })}</p>}
              </div>
            ) : (active.loading && !active.data) || verification.loading ? <div role="status" aria-label={t("common.loading")}><Skeleton height={120} /></div>
              : <p className="muted small">{t("explorer.noVersion")}</p>}
          </Panel>
      </div>
    </div>
  );
}
