/**
 * Data & methodology (public): what ICP is and when it is published, estimate vs forecast, data
 * sources with attribution, how values are checked against official documents, uncertainty and
 * accuracy, limitations. Static copy only: no API calls, no model versions, run ids, dataset
 * version ids or series codes. The accuracy figures are the documented evaluation
 * (docs/FORECASTING_METHODOLOGY.md, section 5) with its evaluation period stated next to them.
 * Layout: components/public/DocLayout (sticky contents column from 1100 px, 68ch article).
 */
import { ArrowRight } from "lucide-react";
import { Link } from "react-router-dom";
import { useAuth } from "../auth";
import { ProcessExplainer } from "../components/process";
import { DocLayout } from "../components/public/DocLayout";
import { PublicLayout } from "../components/public-layout";
import { KindChip, STABLE_PCT } from "../components/ui";
import { EvidenceBadge } from "../lib/verification";
import type { Basis } from "../api/product-types";
import { useT, type Key } from "../i18n";

const SECTIONS = [
  { id: "icp", title: "meth.icp.title" },
  { id: "jenis-nilai", title: "meth.kinds.title" },
  { id: "sumber", title: "meth.sources.title" },
  { id: "verifikasi", title: "meth.recon.title" },
  { id: "akurasi", title: "meth.accuracy.title" },
  { id: "batasan", title: "meth.limits.title" },
] as const satisfies ReadonlyArray<{ id: string; title: Key }>;

/** Every evidence state a month can show in Riwayat Harga (same vocabulary as the server labels). */
const BASES: Basis[] = ["two_documents", "decree_text", "single_document", "document_plus_legacy", "ocr_single", "conflict", "no_evidence", "provider_published"];

export default function MethodologyPage() {
  const { t, locale } = useT();
  const { principal } = useAuth();
  const signedIn = principal.kind !== "public";
  return (
    <PublicLayout className="method-page">
      <DocLayout title={t("meth.title")} lead={t("meth.lead")} tocLabel={t("meth.toc")}
        // Decorative original illustration (source document, check, chart with its range); lazy, so
        // narrow screens, where the column is not shown, never download it.
        art={<img src="/img/illus/meth-evidence.svg" width={320} height={240} alt="" aria-hidden="true" loading="lazy" decoding="async" />}
        toc={SECTIONS.map((s) => ({ id: s.id, label: t(s.title) }))}>
        <section id="icp" aria-labelledby="icp-h">
          <h2 id="icp-h">{t("meth.icp.title")}</h2>
          <p>{t("meth.icp.body")}</p>
          <ProcessExplainer />
        </section>

        <section id="jenis-nilai" aria-labelledby="jenis-h">
          <h2 id="jenis-h">{t("meth.kinds.title")}</h2>
          <p>{t("meth.kinds.body")}</p>
          {/* Each term/description pair is wrapped in a <div> (valid <dl> content, B-P2-6). */}
          <dl className="def-list">
            <div><dt><KindChip kind="observed" /></dt><dd>{t("meth.limits.1")}</dd></div>
            <div><dt><KindChip kind="estimate_awaiting_publication" /></dt><dd>{t("kind.estimate.help")}</dd></div>
            <div><dt><KindChip kind="future_forecast" /></dt><dd>{t("kind.forecast.help")}</dd></div>
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
          <p>{t("meth.sources.aggregationNote")}</p>
          <p>{t("landing.snapshotFootnote")}</p>
        </section>

        <section id="verifikasi" aria-labelledby="verifikasi-h">
          <h2 id="verifikasi-h">{t("meth.recon.title")}</h2>
          <p>{t("meth.recon.body")}</p>
          <dl className="def-list">
            {BASES.map((b) => (
              <div key={b}><dt><EvidenceBadge v={{ basis: b, label: t(`verify.${b}` as Key), explanation: null }} /></dt><dd>{t(`verify.${b}.help` as Key)}</dd></div>
            ))}
          </dl>
          <h3 className="meth-h3">{t("meth.direction.title")}</h3>
          <p>{t("meth.direction.body", { pct: String(STABLE_PCT).replace(".", locale === "en" ? "." : ",") })}</p>
        </section>

        <section id="akurasi" aria-labelledby="akurasi-h">
          <h2 id="akurasi-h">{t("meth.accuracy.title")}</h2>
          <p>{t("meth.accuracy.known")}</p>
          <p>{t("meth.accuracy.future")}</p>
          <p>{t("meth.accuracy.intervalNote")}</p>
          <p className="doc-note">{t("meth.accuracy.period")}</p>
        </section>

        <section id="batasan" aria-labelledby="batasan-h">
          <h2 id="batasan-h">{t("meth.limits.title")}</h2>
          <ul>
            <li>{t("meth.limits.2")}</li>
            <li>{t("meth.limits.3")}</li>
            <li>{t("meth.limits.4")}</li>
            <li>{t("landing.honest1")}</li>
            <li>{t("landing.honest2")}</li>
            <li>{t("landing.honest3")}</li>
          </ul>
        </section>

        <div className="doc-cta">
          {signedIn
            ? <Link className="btn primary" to="/proyeksi">{t("meth.ctaUser")} <ArrowRight size={16} aria-hidden="true" /></Link>
            : <Link className="btn primary" to="/masuk?next=%2Fproyeksi">{t("meth.cta")} <ArrowRight size={16} aria-hidden="true" /></Link>}
        </div>
      </DocLayout>
    </PublicLayout>
  );
}
