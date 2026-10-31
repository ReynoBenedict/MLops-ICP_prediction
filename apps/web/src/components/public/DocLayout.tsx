/**
 * Document layout for the methodology and legal pages: title block, table of contents and the
 * article column (68 characters). From 1100 px the contents list sits in a sticky left column and
 * marks the section being read; below that it stays at the top, under the title. An optional
 * decorative `art` (an illustration beside the title) takes the free column to the right of the
 * article from 1281 px and is not shown on narrower screens; it is hidden from assistive technology.
 */
import { useEffect, useState, type ReactNode } from "react";
import { useLocation } from "react-router-dom";

/** Id of the section currently being read (first one intersecting the reading band). */
function useScrollSpy(ids: string[]): string | null {
  const [active, setActive] = useState<string | null>(null);
  const key = ids.join("|");
  useEffect(() => {
    if (typeof IntersectionObserver === "undefined") return;
    const list = key.split("|");
    const visible = new Map<string, boolean>();
    const io = new IntersectionObserver((entries) => {
      for (const e of entries) visible.set(e.target.id, e.isIntersecting);
      const first = list.find((id) => visible.get(id));
      if (first) setActive(first);
    }, { rootMargin: "-88px 0px -55% 0px" });
    for (const id of list) { const el = document.getElementById(id); if (el) io.observe(el); }
    return () => io.disconnect();
  }, [key]);
  return active;
}

export interface DocSection { id: string; label: ReactNode }

export function DocLayout({ title, lead, meta, art, toc, tocLabel, tocId, className = "", children }:
  { title: ReactNode; lead?: ReactNode; meta?: ReactNode; art?: ReactNode; toc: DocSection[]; tocLabel: string; tocId?: string; className?: string; children: ReactNode }) {
  const active = useScrollSpy(toc.map((s) => s.id));
  // A deep link (/metodologi#akurasi) arrives before this lazily loaded page has rendered: scroll once it has.
  const { hash } = useLocation();
  useEffect(() => {
    if (!hash) return;
    try { document.getElementById(decodeURIComponent(hash.slice(1)))?.scrollIntoView({ block: "start" }); } catch { /* jsdom */ }
  }, [hash]);
  return (
    <article className={`doc${art ? " has-art" : ""} ${className}`.trim()}>
      <header className="doc-head">
        <h1>{title}</h1>
        {lead && <p className="doc-lead">{lead}</p>}
        {meta}
      </header>
      {art && <div className="doc-art" aria-hidden="true">{art}</div>}
      <nav className="doc-toc" id={tocId} aria-label={tocLabel}>
        <p className="doc-toc-title" aria-hidden="true">{tocLabel}</p>
        <ol>
          {toc.map((s) => (
            <li key={s.id}><a href={`#${s.id}`} aria-current={active === s.id ? "location" : undefined}>{s.label}</a></li>
          ))}
        </ol>
      </nav>
      <div className="doc-body">{children}</div>
    </article>
  );
}
