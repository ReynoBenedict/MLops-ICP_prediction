/**
 * Terms of Use and Privacy Policy. The text (pages/legal-content.ts) describes only what the product
 * verifiably does; decisions only the owner can make (legal entity, address, contact, governing
 * law, retention, legal basis) are rendered as visibly marked placeholders, never invented.
 * Layout: components/public/DocLayout (one h1, version line, draft note, table of contents with
 * anchors in a sticky column from 1100 px, numbered h2 sections, reading width about 68 characters).
 */
import { Fragment, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { DocLayout } from "../components/public/DocLayout";
import { PublicLayout } from "../components/public-layout";
import { useT, type Locale } from "../i18n";
import { PLACEHOLDERS, PRIVACY, TERMS, type L, type LegalDoc } from "./legal-content";

/** Render `[[key]]` placeholders and `**bold**` inside a text. */
function Rich({ text, locale }: { text: L; locale: Locale }) {
  const { t } = useT();
  const s = locale === "en" ? text.en : text.id;
  const parts = s.split(/(\[\[\w+\]\]|\*\*[^*]+\*\*)/g).filter(Boolean);
  return (
    <>
      {parts.map((part, i) => {
        const ph = /^\[\[(\w+)\]\]$/.exec(part);
        if (ph) {
          const def = PLACEHOLDERS[ph[1]!];
          return (
            <span key={i} className="legal-placeholder" data-placeholder={ph[1]} title={t("legal.placeholderHint")}>
              [{t("legal.placeholder")}: {def ? (locale === "en" ? def.en : def.id) : ph[1]}]
            </span>
          );
        }
        const b = /^\*\*([^*]+)\*\*$/.exec(part);
        if (b) return <strong key={i}>{b[1]}</strong>;
        return <Fragment key={i}>{part}</Fragment>;
      })}
    </>
  );
}

function LegalDocument({ title, doc, related }: { title: string; doc: LegalDoc; related: ReactNode }) {
  const { t, locale } = useT();
  const meta = (
    <>
      <p className="legal-meta"><span>{t("legal.updatedLabel")}</span><time dateTime="2026-10-19">{t("legal.updatedDate")}</time></p>
      <p className="legal-draft" role="note"><Rich text={doc.draftNote} locale={locale} /></p>
    </>
  );
  return (
    <PublicLayout className="legal-page">
      <DocLayout className="legal-doc" title={title} lead={<Rich text={doc.intro} locale={locale} />} meta={meta}
        tocLabel={t("legal.contents")} tocId="daftar-isi"
        toc={doc.sections.map((s, i) => ({ id: s.id, label: <><span className="legal-num">{i + 1}.</span> {locale === "en" ? s.title.en : s.title.id}</> }))}>
        {doc.sections.map((s, i) => (
          <section key={s.id} id={s.id} className="legal-section" aria-labelledby={`${s.id}-h`}>
            <h2 id={`${s.id}-h`}><span className="legal-num">{i + 1}.</span> {locale === "en" ? s.title.en : s.title.id}</h2>
            {s.blocks.map((b, j) => "p" in b
              ? <p key={j}><Rich text={b.p} locale={locale} /></p>
              : <ul key={j}>{b.ul.map((li, k) => <li key={k}><Rich text={li} locale={locale} /></li>)}</ul>)}
          </section>
        ))}
        <footer className="legal-contact">
          <a href="#daftar-isi">{t("legal.backToTop")}</a>
          {related}
        </footer>
      </DocLayout>
    </PublicLayout>
  );
}

export function TermsPage() {
  const { t } = useT();
  return <LegalDocument title={t("legal.termsTitle")} doc={TERMS} related={<Link to="/privasi">{t("legal.related.terms")}</Link>} />;
}

export function PrivacyPage() {
  const { t } = useT();
  return <LegalDocument title={t("legal.privacyTitle")} doc={PRIVACY} related={<Link to="/syarat">{t("legal.related.privacy")}</Link>} />;
}
