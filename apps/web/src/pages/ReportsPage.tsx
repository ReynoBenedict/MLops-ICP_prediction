/**
 * Laporan: configure → "Isi laporan" (what the report will contain) → generate → result. Every
 * outcome has its own state: a rejected request (plan, role, validation, rate limit) says why and
 * what to do; generation shows its progress; a request without a definite answer (the browser's
 * timeout, the gateway's 504, a broken connection: the report may have finished or failed on the
 * server) tells the reader to check the list, and the list then refreshes itself and marks reports
 * that appeared since; a failed report is never presented as a deliverable (no download buttons); a
 * finished one offers its files with per-format progress. Each submission carries an
 * Idempotency-Key that a retry of the same submission reuses, so a retry returns a report that did
 * finish instead of generating and counting a second one (lib/report-request.ts).
 * The result lives outside the form: no button in it can submit the form again (B-P0-3).
 * Deep links: `/laporan?jenis=perbandingan&skenario=<id,id>` (scenario page) and
 * `/laporan?jenis=proyeksi&proyeksi=<forecastId>` (forecast page) preselect kind and inputs.
 */
import type { ActiveDatasets, ForecastRunList, OrganizationDetail, ReportList, ReportRun, UserPublic } from "../api/types";
import { CircleCheck, Clock, Download, FileText, ListChecks, LoaderCircle, Lock, Play, RefreshCw } from "lucide-react";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { api, ApiError, downloadWithAuth, TIMEOUTS } from "../api/client";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { IllReports } from "../components/illustrations";
import { useToast } from "../components/toast";
import { Badge, Button, Callout, EmptyState, ErrorState, PageHeader, Pager, Panel, Skeleton, StatusBadge, retryAfterSeconds, scrollToResult } from "../components/ui";
import { dateOnly, dateTime, period, rangeText } from "../format";
import { useT, type Key } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/flows.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { nextAttempt, REPORT_FORMATS, reportBody, unknownOutcome, type ReportAttempt, type ReportFormat, type UnknownOutcome } from "../lib/report-request";
import { fetchAllScenarios } from "../lib/scenario-list";

type Kind = "market_monthly" | "forecast" | "scenario_comparison" | "data_provenance";
type Fmt = ReportFormat;
const KINDS: Kind[] = ["market_monthly", "forecast", "scenario_comparison", "data_provenance"];
const KIND_PARAM: Record<string, Kind> = { bulanan: "market_monthly", proyeksi: "forecast", perbandingan: "scenario_comparison", provenans: "data_provenance" };
const MAX_SECONDS = Math.round(TIMEOUTS.report / 1000);
/** After an unknown outcome the list is re-read at these moments (ms) to find a report that finished late. */
const RECHECK_MS = [10_000, 30_000, 60_000, 120_000];
/** Wait before re-sending a request the server is still generating, when it names no wait itself. */
const IN_PROGRESS_WAIT_S = 15;
/** When the list was not loaded at submit time, reports created from this long before the request count as new. */
const NEW_TOLERANCE_MS = 5_000;

type RowState = { busy?: string; ok?: string[]; error?: unknown; failedFmt?: string };
type Gen =
  | { state: "idle" }
  | { state: "working"; kind: Kind; startedAt: number }
  | { state: "rejected"; error: unknown }
  | { state: "error"; error: unknown }
  | { state: "unknown"; reason: UnknownOutcome; kind: Kind; startedAt: number; firstSentAt: number; seconds: number }
  | { state: "failed"; run: ReportRun }
  | { state: "done"; run: ReportRun };
/** What the plan allows is known (or the reader is an operator), still loading, or could not be read. */
type EntState = "known" | "loading" | "failed";

/** Human description of a report's parameters (from its stored summary/params; nothing invented). */
export function reportParams(r: ReportRun, names: Map<string, string>, t: ReturnType<typeof useT>["t"], locale: "id" | "en", intl: string): string | null {
  const sum = (r.summary ?? {}) as Record<string, unknown>;
  const params = (r.params ?? {}) as Record<string, unknown>;
  const str = (v: unknown) => (typeof v === "string" && v ? v : null);
  if (r.kind === "scenario_comparison") {
    const ids = (Array.isArray(sum.scenarios) ? sum.scenarios : Array.isArray(params.scenario_ids) ? params.scenario_ids : []).map(String);
    if (!ids.length) return null;
    const known = ids.map((id) => names.get(id)).filter((n): n is string => !!n);
    return known.length === ids.length ? known.join(t("rep.vs")) : t("rep.paramScenarios", { n: ids.length });
  }
  if (r.kind === "forecast") {
    const origin = str(sum.forecast_origin), issued = str(sum.issue_date);
    return origin || issued ? t("rep.paramForecast", { date: dateOnly(issued, intl), origin: period(origin, locale) }) : null;
  }
  if (r.kind === "market_monthly") return str(sum.period) ? t("rep.paramMarket", { period: period(str(sum.period), locale, true) }) : null;
  if (r.kind === "data_provenance") return typeof sum.months === "number" ? t("rep.paramProvenance", { n: sum.months }) : null;
  return null;
}

/** Seconds since `startedAt`, updated every second while `active`. */
function useElapsed(active: boolean, startedAt: number | null): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [active]);
  return active && startedAt ? Math.max(0, Math.floor((now - startedAt) / 1000)) : 0;
}

function ReportsTab({ ent, entState, entError, onRetryEnt, initialKind, initialScenarios, initialForecast }:
  { ent: OrganizationDetail["entitlements"] | null; entState: EntState; entError: unknown; onRetryEnt: () => void; initialKind: Kind | null; initialScenarios: string[]; initialForecast: string }) {
  const { t, locale, intl } = useT();
  const { isOperator, isUser, hasRole } = useAuth();
  const { show } = useToast();
  const [kind, setKind] = useState<Kind>(initialKind === "scenario_comparison" && !hasRole("analyst") ? "market_monthly" : initialKind ?? "market_monthly");
  const [formats, setFormats] = useState<Fmt[]>(["pdf", "xlsx", "csv"]);
  const [forecastId, setForecastId] = useState(initialForecast);
  const [scenarioIds, setScenarioIds] = useState<string[]>(initialScenarios);
  const [cursor, setCursor] = useState<string | null>(null);
  const [gen, setGen] = useState<Gen>({ state: "idle" });
  const [rows, setRows] = useState<Record<string, RowState>>({});
  /**
   * What "new" means for the last request: the report ids already listed when it was sent (anything
   * else that appears is new), or, when the list had not loaded then, its send time (W2-R8).
   */
  const [since, setSince] = useState<{ ids: Set<string> | null; at: number } | null>(null);
  /** The submission in flight or last left without a definite answer; a retry of it reuses its key. */
  const attemptRef = useRef<ReportAttempt | null>(null);
  /**
   * Seconds left before the server accepts another request: after a 429 with a wait time, or after a
   * 409 IDEMPOTENCY_IN_PROGRESS (Retry-After, else IN_PROGRESS_WAIT_S) while the first request still runs.
   */
  const [cooldown, setCooldown] = useState(0);
  const resultRef = useRef<HTMLDivElement>(null);
  const list = useResource((s) => api<ReportList>("/reports", { query: { limit: 10, cursor }, signal: s }), [cursor]);
  // Scenario names serve both the comparison picker and the report list (parameters per row); every page of them.
  const scenarios = useResource((s) => !isOperator ? fetchAllScenarios(s).catch(() => null) : Promise.resolve(null), [isOperator]);
  // Member names for the "created by" column (any member may read the list; on failure the column says "member").
  const members = useResource((s) => isUser ? api<UserPublic[]>("/org/members", { signal: s }).catch(() => null) : Promise.resolve(null), [isUser]);
  const forecasts = useResource((s) => kind === "forecast" ? api<ForecastRunList>("/forecasts", { query: { limit: 10 }, signal: s }) : Promise.resolve(null), [kind]);
  const active = useResource((s) => api<ActiveDatasets>("/datasets/active", { signal: s }).catch(() => null), []);
  // A format is offered only when the plan is known to include it (W2-R4: unknown is not "everything").
  // CSV is part of every plan. `null` = not known yet (plan loading or unavailable).
  const planHas = (f: Fmt): boolean | null => (isOperator || f === "csv") ? true : !ent ? null : f === "pdf" ? ent.reports_pdf : ent.reports_excel;
  const allowed = (f: Fmt) => planHas(f) === true;
  // Files of an existing report: hidden only when the plan is known to exclude the format (the server
  // checks every download anyway), so a plan that failed to load never hides a report's files.
  const downloadable = (f: Fmt) => planHas(f) !== false;
  const names = useMemo(() => new Map((scenarios.data?.items ?? []).map((s) => [s.id, s.name])), [scenarios.data]);
  const memberNames = useMemo(() => new Map((members.data ?? []).map((m) => [m.id, m.name])), [members.data]);
  const chosenFormats = formats.filter(allowed);
  const working = gen.state === "working";
  const elapsed = useElapsed(working, working ? gen.startedAt : null);
  useEffect(() => {
    if (cooldown <= 0) return;
    const id = window.setTimeout(() => setCooldown((c) => Math.max(0, c - 1)), 1000);
    return () => window.clearTimeout(id);
  }, [cooldown]);

  // An unknown outcome: re-read the list a few times so a report that finished late shows up.
  const reloadList = list.reload;
  useEffect(() => {
    if (gen.state !== "unknown") return;
    const ids = RECHECK_MS.map((ms) => window.setTimeout(() => reloadList(), ms));
    return () => ids.forEach((id) => window.clearTimeout(id));
  }, [gen, reloadList]);

  useEffect(() => {
    if (gen.state === "idle" || gen.state === "working") return;
    const el = resultRef.current;
    if (!el) return;
    scrollToResult(el, "nearest");
    // A finished report: straight to its first download; any other outcome: its explanation.
    (el.querySelector<HTMLElement>(".rep-dl button") ?? el).focus({ preventScroll: true });
  }, [gen]);

  /** Send one attempt (a new submission or a retry of the same one, with the same Idempotency-Key). */
  const send = async (attempt: ReportAttempt) => {
    if (working || cooldown > 0) return; // one report at a time; respect the server's wait
    const startedAt = Date.now();
    // The first send of this submission; a retry with the same key keeps it (the in-progress notice names it).
    attempt.firstSentAt ??= startedAt;
    const firstSentAt = attempt.firstSentAt;
    const sentKind = attempt.body.kind as Kind;
    setSince({ ids: list.data ? new Set(list.data.items.map((r) => r.id)) : null, at: startedAt });
    setGen({ state: "working", kind: sentKind, startedAt });
    try {
      const r = await api<ReportRun>("/reports", { method: "POST", json: attempt.body, headers: { "Idempotency-Key": attempt.key } });
      // A definite outcome: the next submission is a new report with a new key.
      if (attemptRef.current === attempt) attemptRef.current = null;
      setCursor(null); list.reload();
      if (r.status === "succeeded") { setGen({ state: "done", run: r }); show(t("toast.reportReady"), "good"); }
      else setGen({ state: "failed", run: r });
    } catch (err) {
      const ae = err instanceof ApiError ? err : null;
      // No definite answer (browser timeout, gateway 504, broken connection): the report may have been
      // recorded either way. Keep the attempt, so a retry returns that report instead of a second one.
      const unknown = unknownOutcome(err);
      if (unknown) {
        setCursor(null); list.reload();
        // Still generating on the server: wait as asked before the same request may be sent again.
        if (unknown === "in_progress" && ae) setCooldown(retryAfterSeconds(ae) ?? IN_PROGRESS_WAIT_S);
        setGen({ state: "unknown", reason: unknown, kind: sentKind, startedAt, firstSentAt, seconds: Number(ae?.details?.timeout_seconds) || MAX_SECONDS });
        return;
      }
      if (ae?.code === "IDEMPOTENCY_KEY_REUSED" && attemptRef.current === attempt) attemptRef.current = null;
      if (ae && (ae.status === 429 || ae.code === "RATE_LIMITED")) { const wait = retryAfterSeconds(ae); if (wait) setCooldown(wait); }
      const rejected = !!ae && ae.status >= 400 && ae.status < 500;
      setGen(rejected ? { state: "rejected", error: err } : { state: "error", error: err });
    }
  };
  /** The form as a request: the same form as the pending attempt reuses its key, a changed one gets a new key. */
  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    if (working || cooldown > 0) return;
    const params: Record<string, unknown> = {};
    if (kind === "forecast" && forecastId) params.forecast_id = forecastId;
    if (kind === "scenario_comparison") params.scenario_ids = scenarioIds;
    const attempt = nextAttempt(attemptRef.current, reportBody(kind, params, chosenFormats));
    attemptRef.current = attempt;
    void send(attempt);
  };
  /** Retry the request that got no definite answer, exactly as sent (same body, same key). */
  const retry = () => {
    const pending = attemptRef.current;
    if (pending) void send(pending);
    else submit();
  };
  const dl = async (r: ReportRun, f: string) => {
    if (rows[r.id]?.busy) return;
    setRows((m) => ({ ...m, [r.id]: { ok: m[r.id]?.ok, busy: f } }));
    try {
      await downloadWithAuth(`/reports/${r.id}/download`, { format: f }, `icp-intelligence_${r.kind}_${r.created_at.slice(0, 10)}.${f}`);
      setRows((m) => ({ ...m, [r.id]: { ok: [...new Set([...(m[r.id]?.ok ?? []), f])] } }));
    } catch (err) { setRows((m) => ({ ...m, [r.id]: { ok: m[r.id]?.ok, error: err, failedFmt: f } })); }
  };
  const kindName = (k: string) => t(`rep.kind.${k}` as Key);
  /** The kind inside a sentence ("Membuat laporan pasar bulanan…"): sentence case keeps only the first word capitalised. */
  // A leading acronym ("ICP forecast report") stays as it is.
  const kindInText = (k: string) => {
    const n = kindName(k);
    return /^\p{Lu}\p{Ll}/u.test(n) ? n.charAt(0).toLocaleLowerCase(locale) + n.slice(1) : n;
  };
  const creator = (r: ReportRun) => {
    if (r.created_by.startsWith("apikey:")) return t("rep.byApiKey");
    if (r.created_by.startsWith("user:")) return memberNames.get(r.created_by.slice(5)) ?? t("rep.byMember");
    return r.org_id ? t("rep.byMember") : t("rep.byOperator");
  };
  // Only formats included in the plan are offered (platform reports may carry more formats). A report
  // that did not succeed has no files and therefore no buttons.
  const downloads = (r: ReportRun) => {
    if (r.status !== "succeeded") return null;
    const st = rows[r.id] ?? {};
    const fmts = r.formats.filter((f) => downloadable(f as Fmt));
    if (!fmts.length) return <span className="xs muted">{t("rep.noAllowedFiles")}</span>;
    return (
      <div className="rep-dl">
        <div className="rep-dl-buttons" role="group" aria-label={t("rep.downloadsFor", { kind: kindInText(r.kind), date: dateTime(r.created_at, intl) })}>
          {fmts.map((f) => {
            const done = st.ok?.includes(f);
            return (
              <Button key={f} size="sm" icon={done ? CircleCheck : Download} loading={st.busy === f} disabled={!!st.busy && st.busy !== f}
                className={done ? "done" : undefined} onClick={() => void dl(r, f)}
                aria-label={`${t("common.download")} ${f.toUpperCase()}: ${kindName(r.kind)} ${dateTime(r.created_at, intl)}`}>{f.toUpperCase()}</Button>
            );
          })}
        </div>
        <div aria-live="polite" className="rep-dl-status">
          {st.busy ? <span className="xs muted">{t("rep.downloadingFmt", { f: st.busy.toUpperCase() })}</span>
            : st.ok?.length && !st.error ? <span className="xs row-ok"><CircleCheck size={13} aria-hidden="true" /> {t("rep.downloaded", { f: st.ok.at(-1)!.toUpperCase() })}</span> : null}
        </div>
        {st.error ? <ErrorState error={st.error} compact titles={{ TIMEOUT: t("rep.downloadTimeout", { f: (st.failedFmt ?? "").toUpperCase() }) }} /> : null}
      </div>
    );
  };

  // What the report will contain, from data the page already has (no promises beyond that).
  const icpVersion = active.data?.datasets.icp?.active_version ?? null;
  const pickedForecast = forecasts.data?.items.find((r) => r.id === forecastId) ?? null;
  const preview: string[] = [t(`rep.kind.${kind}.help` as Key)];
  if (icpVersion && kind !== "forecast" && kind !== "scenario_comparison") {
    preview.push(kind === "data_provenance"
      ? t("rep.preview.coverage", { range: rangeText(period(icpVersion.period_start, locale), period(icpVersion.period_end, locale), locale) })
      : t("rep.preview.latest", { period: period(icpVersion.period_end, locale, true) }));
  }
  if (kind === "forecast") preview.push(pickedForecast
    ? t("rep.preview.forecast", { date: dateOnly(pickedForecast.issue_date ?? pickedForecast.created_at, intl), origin: period(pickedForecast.forecast_origin ?? pickedForecast.last_observed_period, locale) })
    : forecastId ? t("rep.forecastLinked") : t("rep.preview.forecastNew"));
  if (kind === "scenario_comparison") preview.push(scenarioIds.length
    ? t("rep.preview.scenarios", { list: scenarioIds.map((id) => names.get(id) ?? id).join(t("rep.vs")) })
    : t("rep.preview.noScenarios"));
  if (icpVersion) preview.push(t("rep.preview.dataAt", { date: dateTime(icpVersion.created_at, intl) }));
  preview.push(chosenFormats.length ? t("rep.preview.formats", { list: chosenFormats.map((f) => f.toUpperCase()).join(", ") }) : t("rep.preview.noFormats"));

  const blocked = !hasRole("viewer") || chosenFormats.length === 0 || (kind === "scenario_comparison" && !scenarioIds.length);
  const items = list.data?.items ?? [];
  const isNew = (r: ReportRun) => !!since && (gen.state === "unknown" || gen.state === "done" || gen.state === "failed")
    && (since.ids ? !since.ids.has(r.id) : Date.parse(r.created_at) >= since.at - NEW_TOLERANCE_MS);
  const unknownTitle = (g: Extract<Gen, { state: "unknown" }>) => g.reason === "client_timeout" ? t("rep.timeoutTitle", { n: g.seconds })
    : g.reason === "gateway_timeout" ? t("rep.unknownGatewayTitle")
    : g.reason === "in_progress" ? t("rep.inProgressTitle") : t("rep.unknownConnectionTitle");
  // One error per page: when the report list fails too, the service is down and the list says so.
  const listFailed = !!list.error && !list.data;

  const result = (() => {
    switch (gen.state) {
      case "idle": return null;
      case "working": return (
        <div className="rep-progress" role="status">
          <LoaderCircle size={18} className="spin" aria-hidden="true" />
          <div className="grow">
            <div className="callout-title">{t("rep.working", { kind: kindInText(gen.kind) })}</div>
            <div className="small">{t("rep.generatingBody")}</div>
            {elapsed >= 3 && <div className="xs muted rep-elapsed">{t("rep.workingElapsed", { n: elapsed })}</div>}
          </div>
        </div>
      );
      case "rejected": return (
        <div className="stack-sm">
          <ErrorState error={gen.error} titles={{ VALIDATION_ERROR: t("rep.rejectedValidation") }} />
          {/* Not live: a countdown inside the result's polite region would be read out every second. */}
          {cooldown > 0 && <p className="small muted" aria-live="off"><Clock size={13} aria-hidden="true" /> {t("rep.cooldown", { n: cooldown })}</p>}
        </div>
      );
      case "error": return <ErrorState error={gen.error} onRetry={retry} />;
      case "unknown": return (
        <Callout tone="warn" icon={Clock} title={unknownTitle(gen)}
          action={<div className="row rep-unknown-actions">
            <Button size="sm" icon={RefreshCw} loading={list.loading && !!list.data} onClick={() => { setCursor(null); list.reload(); }}>{t("rep.checkList")}</Button>
            <Button size="sm" variant="ghost" onClick={retry} disabled={cooldown > 0}>{t("rep.tryAgain")}</Button>
          </div>}>
          {/* In progress: the request still running on the server is the first one sent with this key. */}
          <p>{gen.reason === "in_progress" ? t("rep.inProgressBody", { time: dateTime(new Date(gen.firstSentAt).toISOString(), intl) })
            : t("rep.timeoutBody", { time: dateTime(new Date(gen.startedAt).toISOString(), intl) })}</p>
          <p>{t("rep.retrySame")}</p>
          {cooldown > 0 && <p className="small muted" aria-live="off"><Clock size={13} aria-hidden="true" /> {t("rep.retryCooldown", { n: cooldown })}</p>}
        </Callout>
      );
      case "failed": return (
        <Callout tone="bad" title={t("rep.failedTitle")} action={<Button size="sm" onClick={() => void submit()} disabled={blocked}>{t("rep.tryAgain")}</Button>}>
          <p>{t("rep.failedBody")}</p>
          <p className="xs muted">{t("rep.failedRef", { id: gen.run.id })} · {t("rep.noFiles")}</p>
        </Callout>
      );
      case "done": return (
        <Callout tone="good" title={t("rep.generated")}>
          <p>{t("rep.generatedBody", { kind: kindName(gen.run.kind), date: dateTime(gen.run.created_at, intl) })}</p>
          {(() => { const d = reportParams(gen.run, names, t, locale, intl); return d ? <p className="xs muted">{d}</p> : null; })()}
          <div className="mt-3">{downloads(gen.run)}</div>
        </Callout>
      );
    }
  })();

  return (
    <div className="layout-form-list">
      <Panel title={t("rep.generate")} icon={FileText}>
        <form onSubmit={submit} className="stack">
          <fieldset className="plain-fieldset">
            <legend className="field-label">{t("rep.kind")}</legend>
            <div className="rep-kinds">
              {KINDS.map((k) => {
                const viewerBlocked = k === "scenario_comparison" && !hasRole("analyst");
                return (
                  <label key={k} className={`rep-kind${kind === k ? " selected" : ""}${viewerBlocked ? " disabled" : ""}`}>
                    <input type="radio" name="kind" checked={kind === k} onChange={() => setKind(k)} disabled={viewerBlocked || working} />
                    <span><strong>{kindName(k)}</strong>{viewerBlocked && <span className="small muted rep-kind-note">{t("rep.viewerComparison")}</span>}</span>
                  </label>
                );
              })}
            </div>
          </fieldset>
          {kind === "forecast" && (
            <label className="field"><span className="field-label">{t("rep.forecastPick")}</span>
              <select className="select" value={forecastId} onChange={(e) => setForecastId(e.target.value)} disabled={working}>
                <option value="">{t("rep.forecastNew")}</option>
                {forecastId && forecasts.data && !forecasts.data.items.some((r) => r.id === forecastId) && <option value={forecastId}>{t("rep.forecastLinked")}</option>}
                {(forecasts.data?.items ?? []).filter((r) => r.status === "succeeded").map((r) => (
                  <option key={r.id} value={r.id}>{t("rep.forecastOption", { date: dateOnly(r.issue_date ?? r.created_at, intl), origin: period(r.forecast_origin ?? r.last_observed_period, locale), n: r.horizon })}</option>
                ))}
              </select>
              <span className="field-hint">{t("rep.forecastPickHelp")}</span></label>
          )}
          {kind === "scenario_comparison" && (
            <fieldset className="field plain-fieldset"><legend className="field-label">{t("rep.scenarioIds")}</legend>
              {initialScenarios.length > 0 && <span className="field-hint">{t("rep.scenarioPreselected")}</span>}
              {scenarios.loading && !scenarios.data ? <Skeleton height={60} /> : scenarios.data?.items.length ? (
                <div className="stack-sm">{scenarios.data.items.map((s) => (
                  <label key={s.id} className="check"><input type="checkbox" checked={scenarioIds.includes(s.id)} disabled={working || (!scenarioIds.includes(s.id) && scenarioIds.length >= 4)}
                    onChange={(e) => setScenarioIds(e.target.checked ? [...scenarioIds, s.id].slice(0, 4) : scenarioIds.filter((x) => x !== s.id))} /><span>{s.name}</span></label>
                ))}</div>
              ) : <span className="small muted">{t("scen.empty")}</span>}
              {!scenarioIds.length && <span className="field-error">{t("rep.preview.noScenarios")}</span>}
            </fieldset>
          )}
          <fieldset className="plain-fieldset">
            <legend className="field-label">{t("rep.formats")}</legend>
            <div className="rep-formats">
              {REPORT_FORMATS.map((f) => (
                <label key={f} className="check"><input type="checkbox" checked={formats.includes(f) && allowed(f)} disabled={!allowed(f) || working}
                  onChange={(e) => setFormats(e.target.checked ? [...formats, f] : formats.filter((x) => x !== f))} />
                  <span className="rep-format-name">{f.toUpperCase()}{planHas(f) === false && <Badge tone="neutral" icon={Lock}>{t("rep.formatNotInPlan")}</Badge>}</span></label>
              ))}
            </div>
            {entState === "failed" && !isOperator && (
              <div className="stack-sm">
                {!listFailed && <ErrorState error={entError} onRetry={onRetryEnt} compact titles={entError instanceof ApiError ? { [entError.code]: t("rep.planUnknownTitle") } : undefined} />}
                <span className="field-hint">{t("rep.planUnknownFormats")}</span>
              </div>
            )}
            {chosenFormats.length === 0 && <span className="field-error">{t("rep.preview.noFormats")}</span>}
          </fieldset>
          <section className="report-preview" aria-labelledby="rep-preview-h">
            <h3 id="rep-preview-h" className="report-preview-title"><ListChecks size={15} aria-hidden="true" />{t("rep.preview.title")}</h3>
            <ul>{preview.map((p, i) => <li key={i}>{p}</li>)}</ul>
          </section>
          <div className="row">
            <Button type="submit" variant="primary" icon={Play} loading={working} disabled={blocked || cooldown > 0}>{working ? t("rep.generating") : t("rep.generate")}</Button>
            {cooldown > 0 && gen.state !== "rejected" && <span className="small muted">{t("rep.cooldown", { n: cooldown })}</span>}
          </div>
        </form>
        <div ref={resultRef} tabIndex={-1} className={`rep-result${result ? "" : " empty-result"}`} aria-live="polite">{result}</div>
      </Panel>
      <Panel title={t("rep.list")} icon={Download} flush
        actions={<Button size="sm" variant="ghost" icon={RefreshCw} loading={list.loading && !!list.data} onClick={list.reload}>{t("common.refresh")}</Button>}>
        {list.error && !list.data ? <div className="panel-body"><ErrorState error={list.error} onRetry={list.reload} /></div>
          : !list.data ? <div className="panel-body" role="status" aria-label={t("common.loading")}><div className="stack-sm"><Skeleton height={56} /><Skeleton height={56} /><Skeleton height={56} /></div></div>
          : !items.length ? (
            <EmptyState illustration={<IllReports size={150} />} title={t("rep.empty")}>{t("rep.emptyBody")}</EmptyState>
          ) : (
            <div className="table-scroll" role="region" tabIndex={0} aria-label={t("rep.list")}>
              {list.error ? <div className="panel-body"><ErrorState error={list.error} onRetry={list.reload} compact /></div> : null}
              <table className="table rc rep-table">
                <caption className="sr-only">{t("rep.list")}</caption>
                {/* Three columns so the list fits beside the form in both languages: the status chip sits with the report. */}
                <thead><tr><th>{t("rep.reportStatus")}</th><th>{t("common.created")}</th><th>{t("rep.files")}</th></tr></thead>
                <tbody>{items.map((r) => {
                  const desc = reportParams(r, names, t, locale, intl);
                  const fresh = isNew(r) || (gen.state === "done" && gen.run.id === r.id);
                  return (
                    <tr key={r.id} className={fresh ? "selected" : ""}>
                      <td className="primary-cell rc-title">
                        <span className="rep-name">{kindName(r.kind)}{desc && <span className="sub">{desc}</span>}{r.org_id ? null : <span className="sub">{t("rep.platform")}</span>}</span>
                        <span className="chip-row"><StatusBadge status={r.status} />{fresh && <Badge tone="info">{t("rep.new")}</Badge>}</span>
                      </td>
                      <td data-label={t("common.created")} className="rc-full"><span className="rep-when">{dateTime(r.created_at, intl)}</span><span className="sub">{t("rep.byName", { name: creator(r) })}</span></td>
                      <td data-label={t("rep.files")} className="rc-full">{r.status === "succeeded" ? downloads(r) : <span className="xs muted">{t("rep.noFiles")}</span>}</td>
                    </tr>
                  );
                })}</tbody>
              </table>
            </div>
          )}
        <Pager hasPrev={!!cursor} hasNext={!!list.data?.next_cursor} onFirst={() => setCursor(null)} onNext={() => setCursor(list.data?.next_cursor ?? null)} />
      </Panel>
    </div>
  );
}

export default function ReportsPage() {
  const { t } = useT();
  const { isUser } = useAuth();
  const [params] = useSearchParams();
  const initialForecast = params.get("proyeksi") ?? "";
  const initialKind = KIND_PARAM[params.get("jenis") ?? ""] ?? (initialForecast ? "forecast" : null);
  const initialScenarios = (params.get("skenario") ?? "").split(",").map((s) => s.trim()).filter(Boolean).slice(0, 4);
  // Customer-only route (the router keeps guests and operators out).
  const org = useResource((s) => isUser ? api<OrganizationDetail>("/org", { signal: s }) : Promise.resolve(null), [isUser]);
  const ent = org.data?.entitlements ?? null;
  const entState: EntState = !isUser || ent ? "known" : org.error ? "failed" : "loading";
  return (
    <div className="page">
      <PageHeader eyebrow={t("rep.eyebrow")} icon={FileText} title={t("rep.title")} description={t("rep.desc")} />
      <ReportsTab ent={ent} entState={entState} entError={org.error} onRetryEnt={org.reload} initialKind={initialKind} initialScenarios={initialScenarios} initialForecast={initialForecast} />
    </div>
  );
}
