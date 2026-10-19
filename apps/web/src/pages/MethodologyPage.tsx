/**
 * Metodologi & Data (public): what ICP is and when it is published, estimate vs forecast, data
 * sources with attribution, how values are checked against official documents, uncertainty and
 * accuracy, limitations. Static copy only: no API calls, no model versions, run ids, dataset
 * version ids or series codes. The accuracy figures are the documented evaluation
 * (docs/FORECASTING_METHODOLOGY.md, section 5) with its evaluation period stated next to them.
 */
import { ArrowRight, BookOpen } from "lucide-react";
import { Link } from "react-router-dom";
import { useAuth } from "../auth";
import { ProcessExplainer } from "../components/process";
import { PublicLayout } from "../components/layout";
import { KindChip, StatusBadge } from "../components/ui";
import { useT, type Key } from "../i18n";

const SECTIONS = [
  { id: "icp", title: "meth.icp.title" },
  { id: "jenis-nilai", title: "meth.kinds.title" },
  { id: "sumber", title: "meth.sources.title" },
  { id: "verifikasi", title: "meth.recon.title" },
  { id: "akurasi", title: "meth.accuracy.title" },
  { id: "batasan", title: "meth.limits.title" },
] as const satisfies ReadonlyArray<{ id: string; title: Key }>;

const STATUSES = ["verified", "single_source", "machine_extracted", "legacy_unverified"] as const;

export default function MethodologyPage() {
  const { t } = useT();
  const { principal } = useAuth();
  const signedIn = principal.kind !== "public";
  return (
    <PublicLayout className="method-page">
      <article className="prose prose-wide">
        <header className="legal-head">
          <div className="eyebrow"><BookOpen size={14} aria-hidden="true" />{t("meth.eyebrow")}</div>
          <h1>{t("meth.title")}</h1>
          <p className="lead">{t("meth.lead")}</p>
        </header>
        <nav className="toc" aria-label={t("meth.toc")}>
          <ol>{SECTIONS.map((s) => <li key={s.id}><a href={`#${s.id}`}>{t(s.title)}</a></li>)}</ol>
        </nav>

        <section id="icp" aria-labelledby="icp-h">
          <h2 id="icp-h">{t("meth.icp.title")}</h2>
          <p>{t("meth.icp.body")}</p>
          <ProcessExplainer />
        </section>

        <section id="jenis-nilai" aria-labelledby="jenis-h">
          <h2 id="jenis-h">{t("meth.kinds.title")}</h2>
          <p>{t("meth.kinds.body")}</p>
          <dl className="def-list">
            <dt><KindChip kind="observed" /></dt><dd>{t("forecast.limits1")}</dd>
            <dt><KindChip kind="estimate_awaiting_publication" /></dt><dd>{t("kind.estimate.help")}</dd>
            <dt><KindChip kind="future_forecast" /></dt><dd>{t("kind.forecast.help")}</dd>
          </dl>
        </section>

        <section id="sumber" aria-labelledby="sumber-h">
          <h2 id="sumber-h">{t("meth.sources.title")}</h2>
          <ul>
            <li>{t("landing.sources.esdm")}</li>
            <li>{t("landing.sources.eia")}</li>
            <li>{t("landing.sources.wb")}</li>
            <li>{t("landing.sources.bi")}</li>
          </ul>
          <p>{t("explorer.aggregationNote")}</p>
          <p>{t("landing.snapshotFootnote")}</p>
        </section>

        <section id="verifikasi" aria-labelledby="verifikasi-h">
          <h2 id="verifikasi-h">{t("meth.recon.title")}</h2>
          <p>{t("meth.recon.body")}</p>
          <dl className="def-list">
            {STATUSES.map((st) => <span key={st} style={{ display: "contents" }}><dt><StatusBadge status={st} /></dt><dd>{t(`meth.status.${st}` as Key)}</dd></span>)}
          </dl>
        </section>

        <section id="akurasi" aria-labelledby="akurasi-h">
          <h2 id="akurasi-h">{t("meth.accuracy.title")}</h2>
          <p>{t("meth.accuracy.known")}</p>
          <p>{t("meth.accuracy.future")}</p>
          <p>{t("forecast.intervalNote")}</p>
          <p className="legal-meta">{t("meth.accuracy.period")}</p>
        </section>

        <section id="batasan" aria-labelledby="batasan-h">
          <h2 id="batasan-h">{t("meth.limits.title")}</h2>
          <ul>
            <li>{t("forecast.limits2")}</li>
            <li>{t("forecast.limits3")}</li>
            <li>{t("forecast.limits4")}</li>
            <li>{t("landing.honest1")}</li>
            <li>{t("landing.honest2")}</li>
            <li>{t("landing.honest3")}</li>
          </ul>
        </section>

        <p className="mt-4">
          {signedIn
            ? <Link className="btn primary" to="/proyeksi">{t("meth.ctaUser")} <ArrowRight size={16} aria-hidden="true" /></Link>
            : <Link className="btn primary" to="/masuk?next=%2Fproyeksi">{t("meth.cta")} <ArrowRight size={16} aria-hidden="true" /></Link>}
        </p>
      </article>
    </PublicLayout>
  );
}
