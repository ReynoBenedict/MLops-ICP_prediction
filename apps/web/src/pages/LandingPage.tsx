/**
 * Public landing page: positioning, the public market snapshot (official values only), a sign-in
 * teaser for the latest forecast, how it works, honesty, plans, sources. Forecast values are a
 * signed-in feature: the teaser never renders predictions, even if an older API still sends them.
 */
import type { MarketSnapshot, PlanInfo } from "../api/types";
import { ArrowRight, Check, CircleHelp, Lock, Minus } from "lucide-react";
import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { api } from "../api/client";
import { useResource } from "../api/hooks";
import { IllPipeline, IllScenarios, IllTimeline } from "../components/illustrations";
import { PublicLayout } from "../components/layout";
import { Delta, ErrorState, Kpi, Skeleton, StatusBadge } from "../components/ui";
import { dateOnly, fmtIdr, fmtNum, fmtUsd, period } from "../format";
import { useT } from "../i18n";

/** Teaser data from either snapshot shape: v4 `{available, issue_date, forecast_origin, horizon}` or the older one with points. */
export function forecastTeaser(raw: unknown): { issueDate: string | null; origin: string | null; horizon: number | null } | null {
  if (!raw || typeof raw !== "object") return null;
  const f = raw as { available?: unknown; issue_date?: unknown; forecast_origin?: unknown; horizon?: unknown; points?: unknown };
  const available = f.available === true || (f.available === undefined && Array.isArray(f.points) && f.points.length > 0);
  if (!available) return null;
  const horizon = typeof f.horizon === "number" ? f.horizon : Array.isArray(f.points) ? f.points.length : null;
  return {
    issueDate: typeof f.issue_date === "string" ? f.issue_date : null,
    origin: typeof f.forecast_origin === "string" ? f.forecast_origin : null,
    horizon: horizon && horizon > 0 ? horizon : null,
  };
}

function Photo({ base, alt, caption, eager }: { base: string; alt: string; caption?: string; eager?: boolean }) {
  const [failed, setFailed] = useState(false);
  if (failed) {
    return <img src="/brand/hero-composite.svg" alt={alt} loading={eager ? "eager" : "lazy"} />;
  }
  return (
    <>
      <picture>
        <source srcSet={`/img/${base}.webp`} type="image/webp" />
        <img src={`/img/${base}.jpg`} alt={alt} loading={eager ? "eager" : "lazy"} decoding="async" onError={() => setFailed(true)} />
      </picture>
      {caption && <figcaption>{caption}</figcaption>}
    </>
  );
}

function PlanCard({ p, t, intl }: { p: PlanInfo; t: ReturnType<typeof useT>["t"]; intl: string }) {
  const feats: Array<[boolean, string]> = [
    [true, t("landing.plan.seats", { n: p.seats })],
    [true, t("landing.plan.horizon", { n: p.forecast_horizon_max })],
    [p.scenarios, t("landing.plan.scenarios", { n: p.saved_scenarios_max })],
    [p.reports_pdf, t("landing.plan.reportsPdf")],
    [p.reports_excel, t("landing.plan.reportsExcel")],
    [p.alerts, t("landing.plan.alerts", { n: p.alert_rules_max })],
    [p.api_access, p.api_access ? t("landing.plan.api", { n: fmtNum(p.api_requests_per_month, intl, 0) }) : t("landing.plan.noApi")],
    [true, p.history_years ? t("landing.plan.history", { n: p.history_years }) : t("landing.plan.historyAll")],
  ];
  return (
    <div className="plan">
      <h3>{p.label_id}</h3>
      <div className="price-note">{p.price_note_id}</div>
      <ul>{feats.map(([on, label], i) => <li key={i} className={on ? "" : "off"}>{on ? <Check size={15} aria-hidden="true" /> : <Minus size={15} aria-hidden="true" />}{label}</li>)}</ul>
    </div>
  );
}

export default function LandingPage() {
  const { t, locale, intl } = useT();
  const snap = useResource((s) => api<MarketSnapshot>("/public/market-snapshot", { signal: s, anonymous: true }), []);
  const plans = useResource((s) => api<PlanInfo[]>("/plans", { signal: s, anonymous: true }), []);
  const d = snap.data;
  const teaser = forecastTeaser(d?.latest_forecast);
  const { hash } = useLocation();
  // In-page anchors (/#paket from other pages) once the target section has rendered.
  useEffect(() => {
    if (!hash) return;
    const el = document.getElementById(decodeURIComponent(hash.slice(1)));
    try { el?.scrollIntoView({ block: "start" }); } catch { /* jsdom */ }
  }, [hash, plans.data]);

  return (
    <PublicLayout credits>
      <section className="landing-hero">
        <div>
          <div className="eyebrow">{t("landing.eyebrow")}</div>
          <h1>{t("landing.title")}</h1>
          <p className="lead">{t("landing.lead")}</p>
          <div className="hero-cta">
            <Link className="btn primary lg" to="/daftar">{t("landing.ctaPrimary")} <ArrowRight size={16} aria-hidden="true" /></Link>
            <Link className="btn lg" to="/metodologi">{t("landing.ctaSecondary")}</Link>
          </div>
        </div>
        <figure className="hero-media">
          <Photo base="hero-refinery" alt={t("landing.photoHero")} caption={`${t("landing.photoHero")} · Nafisathallah, CC BY 4.0`} eager />
        </figure>
      </section>

      <div className="section-alt">
        <section className="section" aria-labelledby="snap">
          <h2 id="snap">{t("landing.snapshotTitle")}</h2>
          <p className="lead">{d?.source ?? ""}</p>
          {snap.error && !snap.loading && <ErrorState error={snap.error} onRetry={snap.reload} compact />}
          <div className="snapshot">
            <Kpi label={t("landing.snapshotIcp")} iconTone="icp"
              value={d ? fmtUsd(d.icp_usd, intl) : snap.loading ? <Skeleton height={34} width={140} /> : "-"}
              meta={d ? <>{period(d.icp_period, locale, true)} · <StatusBadge status={String(d.freshness.status ?? "unknown")} /></> : undefined} />
            <Kpi label={t("landing.snapshotMom")}
              value={d ? <Delta value={d.mom_change_pct} /> : snap.loading ? <Skeleton height={34} width={100} /> : "-"}
              meta={d?.previous_usd != null ? `${period(d.previous_period, locale)}: ${fmtUsd(d.previous_usd, intl)}` : undefined} />
            <Kpi label={t("landing.snapshotBrent")} iconTone="wti"
              value={d ? fmtUsd(d.brent_usd, intl) : snap.loading ? <Skeleton height={34} width={120} /> : "-"}
              meta={d?.brent_period ? period(d.brent_period, locale, true) : undefined} />
            <Kpi label={t("landing.snapshotIdr")} iconTone="amber"
              value={d ? fmtIdr(d.icp_idr, intl) : snap.loading ? <Skeleton height={34} width={150} /> : "-"}
              meta={d?.usd_idr ? `${t("landing.snapshotFx")}: ${fmtIdr(d.usd_idr, intl)}` : undefined} />
          </div>
          <p className="small muted mt-3">{t("landing.snapshotFootnote")}</p>
          {teaser ? (
            <div className="teaser mt-4">
              <span className="teaser-icon" aria-hidden="true"><Lock size={18} /></span>
              <div className="grow">
                <h3 className="teaser-title">{teaser.issueDate ? t("landing.teaserTitle", { date: dateOnly(teaser.issueDate, intl) }) : t("landing.teaserTitleNoDate")}</h3>
                <p className="small muted">{teaser.horizon && teaser.origin ? t("landing.teaserBody", { n: teaser.horizon, origin: period(teaser.origin, locale, true) }) : t("landing.teaserBodyShort")}</p>
              </div>
              <div className="row">
                <Link className="btn primary" to="/masuk?next=%2Fproyeksi">{t("shell.signin")}</Link>
                <Link className="btn" to="/daftar?next=%2Fproyeksi">{t("shell.register")}</Link>
              </div>
            </div>
          ) : d ? <p className="mt-3 small muted">{t("landing.snapshotNoForecast")}</p> : null}
        </section>
      </div>

      <section className="section" id="fitur" aria-labelledby="fitur-h">
        <h2 id="fitur-h">{t("landing.featuresTitle")}</h2>
        <div className="feature-grid">
          <div className="feature"><div className="feature-ill"><IllPipeline size={150} /></div><h3>{t("landing.f1.title")}</h3><p>{t("landing.f1.body")}</p></div>
          <div className="feature"><div className="feature-ill"><IllTimeline size={150} /></div><h3>{t("landing.f2.title")}</h3><p>{t("landing.f2.body")}</p></div>
          <div className="feature"><div className="feature-ill"><IllScenarios size={150} /></div><h3>{t("landing.f3.title")}</h3><p>{t("landing.f3.body")}</p></div>
        </div>
      </section>

      <div className="section-alt">
        <section className="section editorial" aria-label={t("landing.editorialTitle")}>
          <figure><Photo base="editorial-port" alt={t("landing.photoPort")} caption={`${t("landing.photoPort")} · Andrew Thomas, CC BY-SA 2.0`} /></figure>
          <div>
            <h2>{t("landing.editorialTitle")}</h2>
            <p className="lead">{t("landing.editorialBody")}</p>
            <div id="metode" style={{ marginTop: 22 }}>
              <h3 style={{ fontSize: 18, color: "var(--navy)" }}>{t("landing.howTitle")}</h3>
              <ol className="steps-list">
                <li>{t("landing.how1")}</li><li>{t("landing.how2")}</li><li>{t("landing.how3")}</li>
              </ol>
            </div>
          </div>
        </section>
      </div>

      <section className="section" aria-labelledby="honest-h">
        <h2 id="honest-h"><CircleHelp size={22} aria-hidden="true" style={{ verticalAlign: -3, marginRight: 8 }} />{t("landing.honestTitle")}</h2>
        <div className="honest">
          <div className="item">{t("landing.honest1")}</div><div className="item">{t("landing.honest2")}</div><div className="item">{t("landing.honest3")}</div>
        </div>
      </section>

      <div className="section-alt">
        <section className="section" id="paket" aria-labelledby="paket-h">
          <h2 id="paket-h">{t("landing.plansTitle")}</h2>
          <p className="lead">{t("landing.plansLead")}</p>
          <div className="plan-grid">
            {plans.data ? plans.data.map((p) => <PlanCard key={p.code} p={p} t={t} intl={intl} />) : plans.loading ? [0, 1, 2, 3].map((i) => <Skeleton key={i} height={260} />) : null}
          </div>
          {plans.error && !plans.loading && <ErrorState error={plans.error} onRetry={plans.reload} compact />}
          <div className="mt-4"><Link className="btn primary" to="/daftar">{t("landing.plan.cta")} <ArrowRight size={16} aria-hidden="true" /></Link></div>
        </section>
      </div>

      <section className="section editorial" id="sumber" aria-labelledby="sumber-h">
        <div>
          <h2 id="sumber-h">{t("landing.sourcesTitle")}</h2>
          <ul className="steps-list" style={{ counterReset: "s" }}>
            <li>{t("landing.sources.esdm")}</li><li>{t("landing.sources.eia")}</li><li>{t("landing.sources.wb")}</li><li>{t("landing.sources.bi")}</li>
          </ul>
        </div>
        <figure><Photo base="editorial-tanker" alt={t("landing.photoTanker")} caption={`${t("landing.photoTanker")} · Sabung.hamster, CC BY-SA 4.0`} /></figure>
      </section>

    </PublicLayout>
  );
}
