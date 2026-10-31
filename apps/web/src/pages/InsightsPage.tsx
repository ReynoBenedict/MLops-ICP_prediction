/**
 * Insight Pasar: an editorial feed of rule-generated insights. The most important item leads,
 * up to three follow as compact items, the rest are one-line rows grouped by category.
 * Importance is shown by position and a marker, never by alarm colours. The detail opens in a
 * sheet deep-linked by `?insight=<id>` (Back closes it); the category filter is `?kategori=`.
 * All wording about the data comes from the backend in the active language (`?lang=`).
 */
import { Lightbulb } from "lucide-react";
import { useMemo, useRef } from "react";
import { Link, useLocation, useNavigate, useSearchParams } from "react-router-dom";
import type { FeedInsight } from "../api/product-types";
import { useResource } from "../api/hooks";
import { useAuth } from "../auth";
import { IllNoData } from "../components/illustrations";
import { ImportanceMarker, InsightCompact, InsightDetail, InsightLead, InsightRow, useKindLabel } from "../components/insight";
import { Button, Callout, EmptyState, ErrorState, GateNotice, PageHeader, Skeleton } from "../components/ui";
import { dateTime, period } from "../format";
import { useT } from "../i18n";
import "../i18n/workspace"; // workspace strings, registered when this lazy chunk loads
import "../styles/flows.css"; // page styles, loaded with this lazy chunk (not by the public site)
import { loadInsights, NOT_IN_PLAN } from "../lib/plan";

function FeedSkeleton() {
  const { t } = useT();
  return (
    <div className="ins-feed" role="status" aria-label={t("ins.loading")}>
      <div className="ins-top"><Skeleton height={260} /></div>
      <div className="stack-sm">{[0, 1, 2, 3].map((k) => <Skeleton key={k} height={44} />)}</div>
    </div>
  );
}

export default function InsightsPage() {
  const { t, locale, intl } = useT();
  const { principal } = useAuth();
  const kindLabel = useKindLabel();
  const res = useResource((s) => loadInsights(principal, s, locale), [principal.kind, principal.plan, locale]);
  const notInPlan = res.data === NOT_IN_PLAN;
  const data = res.data && res.data !== NOT_IN_PLAN ? res.data : null;
  const all = useMemo(() => (data?.insights ?? []) as unknown as FeedInsight[], [data]);

  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const location = useLocation();
  const kategori = params.get("kategori");
  const openId = params.get("insight");
  // True when this page pushed the `?insight=` entry, so closing can simply go Back.
  const pushed = useRef(false);

  const kinds = useMemo(() => [...new Set(all.map((i) => i.kind))], [all]);
  const counts = useMemo(() => all.reduce((m, i) => m.set(i.kind, (m.get(i.kind) ?? 0) + 1), new Map<string, number>()), [all]);
  const items = useMemo(() => (kategori ? all.filter((i) => i.kind === kategori) : all), [all, kategori]);
  const lead = items[0] ?? null;
  const compact = items.slice(1, 4);
  const rest = items.slice(4);
  const groups = useMemo(() => {
    const m = new Map<string, FeedInsight[]>();
    for (const i of rest) m.set(i.kind, [...(m.get(i.kind) ?? []), i]);
    return [...m.entries()];
  }, [rest]);
  const opened = openId ? all.find((i) => i.id === openId) ?? null : null;

  const open = (id: string) => {
    const next = new URLSearchParams(params);
    next.set("insight", id);
    pushed.current = true;
    navigate({ pathname: location.pathname, search: `?${next.toString()}` });
  };
  const close = () => {
    if (pushed.current) { pushed.current = false; navigate(-1); return; }
    const next = new URLSearchParams(params);
    next.delete("insight");
    setParams(next, { replace: true });
  };
  const setKategori = (k: string | null) => {
    const next = new URLSearchParams(params);
    if (k) next.set("kategori", k); else next.delete("kategori");
    next.delete("insight");
    setParams(next, { replace: true });
  };
  const basisPeriod = data?.basis?.icp_period ?? null;

  return (
    <div className="page">
      <PageHeader eyebrow={t("ins.eyebrow")} icon={Lightbulb} title={t("ins.title")} description={t("ins.desc")} />
      {data && (
        <p className="ins-meta small muted">
          <span>{t("ins.updated", { date: dateTime(data.generated_at, intl) })}{basisPeriod ? ` · ${t("ins.dataThrough", { period: period(basisPeriod, locale, true) })}` : ""}</span>
          <span> · {t("ins.generatedNote")} · <Link to="/metodologi">{t("ins.methodLink")}</Link></span>
        </p>
      )}

      {notInPlan ? <GateNotice title={t("ins.notInPlanTitle")} upgrade>{t("ins.notInPlanBody")}</GateNotice>
        : res.error && !data ? <ErrorState error={res.error} onRetry={res.reload} />
        : res.loading && !data ? <FeedSkeleton />
        : !all.length ? (
          <div className="panel"><EmptyState illustration={<IllNoData size={160} />} title={t("ins.emptyPeriod")}
            action={<Link className="btn sm" to="/harga">{t("nav.explorer")}</Link>}>{t("ins.emptyBody")}</EmptyState></div>
        ) : (<>
          {kinds.length > 1 && (
            // One row of filters (it scrolls sideways when it does not fit); the count is visual only,
            // so each button keeps the category name as its accessible name.
            <div className="ins-filter" role="group" aria-label={t("ins.filter")}>
              <button type="button" aria-pressed={!kategori} onClick={() => setKategori(null)}>{t("ins.filterAll")}<span className="count" aria-hidden="true">{all.length}</span></button>
              {kinds.map((k) => <button key={k} type="button" aria-pressed={kategori === k} onClick={() => setKategori(k)}>{kindLabel(k)}<span className="count" aria-hidden="true">{counts.get(k) ?? 0}</span></button>)}
            </div>
          )}
          {openId && !opened && <Callout tone="info">{t("ins.notFound")}</Callout>}
          {!lead ? (
            <div className="panel"><EmptyState title={t("ins.emptyFilter")} action={<Button size="sm" onClick={() => setKategori(null)}>{t("ins.clearFilter")}</Button>} /></div>
          ) : (
            <div className="ins-feed">
              <div className={`ins-top${compact.length ? "" : " solo"}`}>
                <InsightLead i={lead} onOpen={() => open(lead.id)} />
                {compact.length > 0 && (
                  <aside className="ins-side" aria-label={t("ins.compactTitle")}>
                    <h2 className="ins-side-title">{t("ins.compactTitle")}</h2>
                    <ul className="ins-compact-list">{compact.map((i) => <InsightCompact key={i.id} i={i} onOpen={() => open(i.id)} />)}</ul>
                  </aside>
                )}
              </div>
              {groups.map(([kind, list]) => (
                <section key={kind} className="ins-group" aria-labelledby={`ins-g-${kind}`}>
                  <h2 className="ins-group-title" id={`ins-g-${kind}`}>{kindLabel(kind)}</h2>
                  <ul className="ins-rows">{list.map((i) => <InsightRow key={i.id} i={i} onOpen={() => open(i.id)} />)}</ul>
                </section>
              ))}
              <p className="ins-legend xs muted">
                <ImportanceMarker level="high" /> <ImportanceMarker level="medium" /> <span className="imp imp-low"><span className="imp-label">{t("ins.imp.low")}</span></span>
              </p>
            </div>
          )}
          {data?.disclaimer && <p className="ins-disclaimer xs muted">{data.disclaimer}</p>}
          {/* A refresh failure after data was shown: one notice, the feed stays readable. */}
          {res.error && data && <ErrorState error={res.error} onRetry={res.reload} compact />}
        </>)}

      {opened && data && (
        <InsightDetail i={opened} open onClose={close} generatedAt={data.generated_at} engineVersion={data.engine_version}
          disclaimer={data.disclaimer} key={opened.id} />
      )}
    </div>
  );
}
