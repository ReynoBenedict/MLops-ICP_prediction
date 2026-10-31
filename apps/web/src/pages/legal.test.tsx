/**
 * Legal pages: structure (one h1, TOC anchors that exist), honesty (owner decisions are visible
 * placeholders, no invented contact or entity), and the typography rule (no em dashes).
 */
import { cleanup, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AuthProvider } from "../auth";
import { I18nProvider, type Locale } from "../i18n";
import { PLACEHOLDERS, PRIVACY, TERMS, placeholdersIn, type LegalDoc } from "./legal-content";
import { PrivacyPage, TermsPage } from "./LegalPages";

afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

const mount = (page: React.ReactNode, locale: Locale) => {
  vi.stubGlobal("fetch", async () => new Response("{}", { status: 404, headers: { "content-type": "application/json" } }));
  return render(<MemoryRouter><I18nProvider initial={locale}><AuthProvider>{page}</AuthProvider></I18nProvider></MemoryRouter>);
};

const allText = (doc: LegalDoc): string[] => [doc.intro.id, doc.intro.en, doc.draftNote.id, doc.draftNote.en,
  ...doc.sections.flatMap((s) => [s.title.id, s.title.en, ...s.blocks.flatMap((b) => ("p" in b ? [b.p] : b.ul).flatMap((l) => [l.id, l.en]))])];

describe("legal pages", () => {
  const cases = [
    { name: "terms", page: <TermsPage />, doc: TERMS, title: { id: "Syarat Penggunaan", en: "Terms of Use" } },
    { name: "privacy", page: <PrivacyPage />, doc: PRIVACY, title: { id: "Kebijakan Privasi", en: "Privacy Policy" } },
  ];
  for (const c of cases) {
    for (const locale of ["id", "en"] as const) {
      it(`${c.name} (${locale}): one h1, version line, TOC links to existing numbered sections`, () => {
        const { container } = mount(c.page, locale);
        const h1s = screen.getAllByRole("heading", { level: 1 });
        expect(h1s).toHaveLength(1);
        expect(h1s[0]).toHaveTextContent(c.title[locale]);
        expect(container.querySelector("time")?.getAttribute("dateTime")).toBe("2026-10-19");
        const toc = screen.getByRole("navigation", { name: locale === "id" ? "Daftar isi" : "Contents" });
        const links = within(toc).getAllByRole("link");
        expect(links).toHaveLength(c.doc.sections.length);
        for (const a of links) {
          const id = a.getAttribute("href")!.slice(1);
          expect(container.querySelector(`section#${id} h2`), id).not.toBeNull();
        }
        // Placeholders are visibly marked, never silent.
        const ph = container.querySelectorAll(".legal-placeholder");
        expect(ph.length).toBeGreaterThan(0);
        for (const el of ph) expect(el.textContent).toMatch(locale === "id" ? /^\[Diisi pemilik: .+\]$/ : /^\[Owner to complete: .+\]$/);
        // No invented contact details: no e-mail address, phone number or contact URL.
        const text = container.querySelector("article")!.textContent ?? "";
        expect(text).not.toMatch(/[\w.+-]+@[\w-]+\.\w+/);
        expect(text).not.toMatch(/https?:\/\//);
        expect(text).not.toMatch(/\+62|\(0\d{1,3}\)/);
      });
    }
  }

  it("every placeholder used is defined, and the entity, contact and retention decisions are left to the owner", () => {
    const used = new Set([...placeholdersIn(TERMS), ...placeholdersIn(PRIVACY)]);
    for (const k of used) expect(PLACEHOLDERS[k], k).toBeDefined();
    for (const k of ["entity", "address", "contact", "law", "basis", "retentionAccount", "retentionAudit"]) expect(used.has(k), k).toBe(true);
  });

  it("uses no em dashes or spaced en dashes, and both languages carry the same placeholders", () => {
    for (const doc of [TERMS, PRIVACY]) {
      for (const s of allText(doc)) expect(s).not.toMatch(/—| – /);
      const pairs = [doc.intro, ...doc.sections.flatMap((s) => s.blocks.flatMap((b) => ("p" in b ? [b.p] : b.ul)))];
      const ph = (s: string) => [...s.matchAll(/\[\[(\w+)\]\]/g)].map((m) => m[1]).sort();
      for (const l of pairs) expect(ph(l.id), l.id.slice(0, 40)).toEqual(ph(l.en));
    }
    for (const l of Object.values(PLACEHOLDERS)) { expect(l.id).not.toMatch(/—| – /); expect(l.en).not.toMatch(/—| – /); }
  });

  it("states only verified data practices (no cookies, hashed credentials, no IP in the audit log)", () => {
    const en = allText(PRIVACY).join(" ");
    expect(en).toContain("The app sets no cookies.");
    expect(en).toContain("scrypt");
    expect(en).toContain("The audit log contains no passwords, tokens or IP addresses.");
    // v6: self-service export (owner) and deletion requests (operator-executed anonymisation) exist now.
    expect(en).not.toContain("Self-service account deletion is not available yet.");
    expect(en).toContain("request deletion of their own account");
    expect(en).toContain("generated on request and not kept on the server");
    expect(en).toContain("keyed by a SHA-256 hash of the normalised e-mail address combined with the client IP address");
  });
});
