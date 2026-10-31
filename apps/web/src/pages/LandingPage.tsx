/**
 * Public landing page. The first screen shows the product itself: the live market panel with the
 * latest official ICP and its 24-month history from the public snapshot. Then: how a figure gets
 * from the official documents to the workspace, a tour of the workspace (real screenshots), how
 * forecasts treat uncertainty, plans, limits and sources, and a closing call to action.
 * Forecast values are a signed-in feature (policy v4): the page only ever reads the teaser's
 * horizon and issue date, even when an older API still sends predicted values.
 */
import type { MarketSnapshot } from "../api/types";
import { ArrowRight, Ban } from "lucide-react";
import { useEffect, useRef, useState, type RefObject } from "react";
import { Link, useLocation } from "react-router-dom";
import { MarketPanel } from "../components/public/MarketPanel";
import { PlansBlock } from "../components/public/plans";
import { ProductTour } from "../components/public/ProductTour";
import { docLabel, usePublicSnapshot } from "../components/public/snapshot";
import { PublicLayout, useSectionLink } from "../components/public-layout";
import { KindChip, prefersReducedMotion } from "../components/ui";
import { fmtNum, period } from "../format";
import { useT, type Key } from "../i18n";
import { keepAnchored } from "../lib/anchor";

// Kept here for existing importers (account page, tests).
export { PlanCard, planFeatures } from "../components/public/plans";
export { forecastTeaser } from "../components/public/snapshot";

/**
 * Backtest of the production model (registered version 5, spread_brent_6m, evaluated 17 October
 * 2026: 69 rolling origins 2020-12..2026-08, one month ahead, MAE in USD/bbl), with the target
 * month's benchmarks observed ("observed h1": metrics.mae 0.8376) and without them ("hold_last h1":
 * 6.1072), against repeating last month's ICP on the same 69 cases (6.0567: recomputed from v5's stored
 * backtest predictions, the figure the workspace's forecast accuracy notes show). The registry metric
 * baseline_naive_last_mae (6.0938) is persistence on the 66 cases common to all candidate methods,
 * like the methodology document's table (0.85 / 6.12), so it is not the same-case comparison; the
 * operator model page shows it as stored. Guests cannot read the model registry, so the landing keeps
 * these as constants; `meth.accuracy.period` says they are a dated snapshot.
 */
const BACKTEST = { known: { model: 0.84, naive: 6.06 }, future: { model: 6.11, naive: 6.06 } } as const;
const BACKTEST_MAX = 6.5;

function ErrorBars({ model, naive, sentence }: { model: number; naive: number; sentence: string }) {
  const { t, intl } = useT();
  const row = (label: string, v: number, cls: string) => (
    <div className="eb-row">
      <span className="eb-label">{label}</span>
      <span className="eb-track"><span className={`eb-bar ${cls}`} style={{ width: `${(v / BACKTEST_MAX) * 100}%` }} /></span>
      <span className="eb-value">{fmtNum(v, intl, 2)}</span>
    </div>
  );
  return (
    <figure className="eb" role="img" aria-label={sentence}>
      <figcaption className="eb-title" aria-hidden="true">{t("landing.honesty.metric")}</figcaption>
      <div aria-hidden="true">
        {row(t("landing.honesty.model"), model, "model")}
        {row(t("landing.honesty.naive"), naive, "naive")}
      </div>
    </figure>
  );
}

const STEPS = ["s1", "s2", "s3", "s4"] as const;
const SOURCES = ["esdm", "eia", "wb", "bi"] as const;

/**
 * Behind the closing call to action: a navy-toned photograph of three ships on the horizon (CC0, Wikimedia
 * Commons; generic, no place or company implied; docs/ASSET_INVENTORY.md). Decorative and lazy; the band
 * keeps its navy background when the image is missing. One 1600 px rendition: the horizon is kept in the
 * band's bottom padding (public.css), so on a phone the photograph is drawn several times wider than the
 * screen and only its middle shows; a smaller rendition would only be enlarged.
 */
function ClosingPhoto() {
  return (
    <picture className="closing-photo" aria-hidden="true">
      <source type="image/avif" srcSet="/img/photo/ships-on-horizon-1600.avif" />
      <img src="/img/photo/ships-on-horizon-1600.webp" width={1600} height={800} alt="" aria-hidden="true" loading="lazy" decoding="async" />
    </picture>
  );
}

/** The latest month's source documents in words ("Kepmen ESDM 395.K/... dan Pengumuman ..., 13 Oktober 2026"). */
function evidenceDocs(ev: NonNullable<MarketSnapshot["icp_evidence"]>, t: ReturnType<typeof useT>["t"], intl: string): string {
  const names = ev.documents.map((d) => docLabel(d, t, intl));
  try { return new Intl.ListFormat(intl, { type: "conjunction" }).format(names); } catch { return names.join(", "); }
}

/**
 * In-page anchors (/#paket from another page or a shared link): every section renders immediately,
 * so the target is scrolled to when the hash changes. The content above it still grows after that
 * (the market panel, the step facts and the plan table arrive with their data, at most ~10 s later),
 * so the target is kept aligned while sections above it change size, until the visitor scrolls or
 * interacts, or 12 s have passed (lib/anchor.ts).
 */
function useHashScroll(hash: string) {
  useEffect(() => {
    if (!hash) return;
    const el = document.getElementById(decodeURIComponent(hash.slice(1)));
    if (!el) return;
    try { el.scrollIntoView({ block: "start", behavior: prefersReducedMotion() ? "auto" : "smooth" }); } catch { /* jsdom */ }
    return keepAnchored(el);
  }, [hash]);
}

/**
 * True once the element is within 200 px of the viewport (and from then on). The step illustrations'
 * sprite is requested only then: below the first screen (1440 x 900 and phones) the landing's first
 * second is left to the heading font and the market panel's data, which on slow connections share
 * the bandwidth with any image. Without IntersectionObserver the images load at once.
 */
function useNearViewport(ref: RefObject<HTMLElement | null>): boolean {
  const [near, setNear] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof IntersectionObserver === "undefined") { setNear(true); return; }
    const io = new IntersectionObserver((entries) => {
      if (entries.some((e) => e.isIntersecting)) { setNear(true); io.disconnect(); }
    }, { rootMargin: "200px 0px" });
    io.observe(el);
    return () => io.disconnect();
  }, [ref]);
  return near;
}

export default function LandingPage() {
  const { t, intl, locale } = useT();
  const snap = usePublicSnapshot();
  const coverage = snap.data?.data.coverage ?? null;
  const evidence = snap.data?.data.icp_evidence ?? null;
  const { hash } = useLocation();
  const section = useSectionLink();
  const flowRef = useRef<HTMLOListElement>(null);
  const flowNear = useNearViewport(flowRef);
  useHashScroll(hash);

  return (
    <PublicLayout className="home">
      <section className="hero" aria-labelledby="hero-h">
        <div className="wrap hero-grid">
          <div className="hero-copy">
            <h1 id="hero-h">{t("landing.title")}</h1>
            <p className="hero-lead">{t("landing.lead")}</p>
            <div className="hero-cta">
              <Link className="btn primary lg" to="/daftar">{t("landing.ctaPrimary")}</Link>
              <Link className="btn lg" to="/#cara-kerja" onClick={section("cara-kerja")}>{t("landing.ctaSecondary")}</Link>
            </div>
            <p className="hero-sources">{t("landing.heroSources")}</p>
          </div>
          <div className="hero-panel"><MarketPanel res={snap} /></div>
        </div>
      </section>

      <section id="cara-kerja" className="band band-plain" aria-labelledby="how-h">
        <div className="wrap">
          <header className="band-head">
            <h2 id="how-h">{t("landing.how.title")}</h2>
            <p className="band-lead">{t("landing.how.lead")}</p>
          </header>
          <ol className={flowNear ? "flow flow-near" : "flow"} ref={flowRef}>
            {STEPS.map((s, i) => (
              <li key={s} className="flow-step">
                <span className="flow-num" aria-hidden="true">{i + 1}</span>
                {/* The step's original illustration: one cell of the CSS sprite /img/illus/steps.svg (public.css),
                    one request for all four, made when the list nears the viewport (useNearViewport).
                    Decorative: the heading and text carry the meaning. */}
                <span className={`flow-art flow-art-${s}`} aria-hidden="true" />
                <h3>{t(`landing.how.${s}.title` as Key)}</h3>
                <p>{t(`landing.how.${s}.body` as Key)}</p>
                {s === "s1" && evidence && evidence.documents.length > 0 && (
                  <p className="flow-fact">
                    <strong>{t("landing.how.s1.fact", { period: period(evidence.period, locale, true) })}</strong>
                    <span>{evidenceDocs(evidence, t, intl)}</span>
                  </p>
                )}
                {s === "s2" && coverage && (
                  <p className="flow-fact">
                    <strong>{t("landing.how.s2.count", { confirmed: fmtNum(coverage.confirmed_months, intl, 0), months: fmtNum(coverage.months, intl, 0) })}</strong>
                    <span>{t("landing.how.s2.countNote", { from: period(coverage.first_period, locale), to: period(coverage.last_period, locale) })}</span>
                  </p>
                )}
                {s === "s3" && (
                  <ul className="flow-fact flow-list">
                    {(["eia", "wb", "bi"] as const).map((k) => <li key={k}>{t(`landing.how.s3.${k}` as Key)}</li>)}
                  </ul>
                )}
                {s === "s4" && (
                  <Link className="flow-link" to="/#produk" onClick={section("produk")}>{t("landing.how.s4.link")} <ArrowRight size={15} aria-hidden="true" /></Link>
                )}
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section id="produk" className="band" aria-labelledby="tour-h">
        <div className="wrap">
          <header className="band-head">
            <h2 id="tour-h">{t("landing.tour.title")}</h2>
            <p className="band-lead">{t("landing.tour.lead")}</p>
          </header>
          <ProductTour />
        </div>
      </section>

      <section id="ketidakpastian" className="band band-plain" aria-labelledby="honesty-h">
        <div className="wrap">
          <header className="band-head">
            <h2 id="honesty-h">{t("landing.honesty.title")}</h2>
            <p className="band-lead">{t("landing.honesty.lead")}</p>
          </header>
          <div className="honesty">
            <div className="honesty-col">
              <KindChip kind="estimate_awaiting_publication" />
              <h3>{t("landing.honesty.known.title")}</h3>
              <p>{t("landing.honesty.known.body")}</p>
              <ErrorBars model={BACKTEST.known.model} naive={BACKTEST.known.naive}
                sentence={t("landing.honesty.chartKnown", { model: fmtNum(BACKTEST.known.model, intl, 2), naive: fmtNum(BACKTEST.known.naive, intl, 2) })} />
            </div>
            <div className="honesty-col">
              <KindChip kind="future_forecast" />
              <h3>{t("landing.honesty.future.title")}</h3>
              <p>{t("landing.honesty.future.body")}</p>
              <ErrorBars model={BACKTEST.future.model} naive={BACKTEST.future.naive}
                sentence={t("landing.honesty.chartFuture", { model: fmtNum(BACKTEST.future.model, intl, 2), naive: fmtNum(BACKTEST.future.naive, intl, 2) })} />
            </div>
          </div>
          <p className="honesty-period">{t("meth.accuracy.period")}</p>
          <div className="honesty-kinds">
            <p className="honesty-kinds-title">{t("landing.honesty.kinds")}</p>
            <ul>
              <li><KindChip kind="observed" /><span>{t("landing.honesty.kind.observed")}</span></li>
              <li><KindChip kind="estimate_awaiting_publication" /><span>{t("kind.estimate.short")}</span></li>
              <li><KindChip kind="future_forecast" /><span>{t("kind.forecast.short")}</span></li>
              <li><KindChip kind="scenario" /><span>{t("landing.honesty.kind.scenario")}</span></li>
            </ul>
          </div>
          <Link className="link-inline honesty-link" to="/metodologi#akurasi">{t("landing.honesty.link")} <ArrowRight size={15} aria-hidden="true" /></Link>
        </div>
      </section>

      <section id="paket" className="band" aria-labelledby="paket-h">
        <div className="wrap">
          <header className="band-head">
            <h2 id="paket-h">{t("landing.plansTitle")}</h2>
            <p className="band-lead">{t("landing.plansLead")}</p>
          </header>
          <PlansBlock />
          <div className="plans-foot">
            <div className="plans-notes">
              <p>{t("landing.plans.note")}</p>
              <p>{t("landing.plans.apiNote")}</p>
            </div>
            <Link className="btn primary" to="/daftar">{t("landing.ctaPrimary")}</Link>
          </div>
        </div>
      </section>

      <section id="sumber" className="band band-plain" aria-label={t("landing.sourcesTitle")}>
        <div className="wrap limits">
          <div>
            <h2>{t("landing.honestTitle")}</h2>
            <ul className="not-list">
              {(["landing.honest1", "landing.honest2", "landing.honest3"] as const).map((k) => <li key={k}><Ban size={16} aria-hidden="true" /><span>{t(k)}</span></li>)}
            </ul>
          </div>
          <div>
            <h2>{t("landing.sourcesTitle")}</h2>
            <dl className="sources">
              {SOURCES.map((s) => (
                <div key={s}>
                  <dt>{t(`landing.src.${s}.name` as Key)}</dt>
                  <dd>{t(`landing.src.${s}.data` as Key)}</dd>
                  <dd className="sources-terms">{t(`landing.src.${s}.terms` as Key)}</dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </section>

      <section className="closing" aria-labelledby="closing-h">
        <ClosingPhoto />
        <div className="wrap closing-inner">
          <div>
            <h2 id="closing-h">{t("landing.closing.title")}</h2>
            <p>{t("landing.closing.body")}</p>
          </div>
          <div className="closing-cta">
            <Link className="btn primary lg" to="/daftar">{t("landing.ctaPrimary")}</Link>
            <Link className="btn lg on-dark" to="/metodologi">{t("landing.nav.data")}</Link>
          </div>
        </div>
      </section>
    </PublicLayout>
  );
}
