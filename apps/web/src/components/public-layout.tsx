/**
 * Public website layout (guests, and anyone on the public pages): top navigation, main, footer;
 * no app sidebar. The signed-in application shell lives in components/layout.tsx.
 */
import { Menu, X } from "lucide-react";
import { useEffect, useRef, useState, type MouseEvent, type ReactNode } from "react";
import { Link, NavLink, useLocation } from "react-router-dom";
import { getOperatorToken, getSessionToken } from "../api/client";
import { useAuth } from "../auth";
import { useT } from "../i18n";
import { Wordmark } from "./brand";
import { LanguageToggle, useRouteFocus, useStickyTop } from "./chrome";
import { ExternalA } from "./public/ExternalA";
import { ESDM_ICP_URL } from "./public/snapshot";
import { prefersReducedMotion } from "./ui";

/**
 * In-page links of the landing (`/#produk`, `/#paket`, ...). The router updates the hash; when the
 * hash is already the same (a second click) nothing would move, so the section is scrolled to and
 * its heading focused here as well.
 */
export function useSectionLink() {
  const { pathname } = useLocation();
  return (id: string) => (e: MouseEvent) => {
    if (pathname !== "/" || e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
    const el = document.getElementById(id);
    if (!el) return;
    try { el.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" }); } catch { /* jsdom */ }
    const h = el.querySelector<HTMLElement>("h2");
    if (h) { if (!h.hasAttribute("tabindex")) h.setAttribute("tabindex", "-1"); h.focus({ preventScroll: true }); }
  };
}

export function PublicFooter() {
  const { t } = useT();
  const { principal } = useAuth();
  const signedIn = principal.kind !== "public";
  const section = useSectionLink();
  return (
    <footer className="landing-footer">
      <div className="inner">
        <div className="footer-brand">
          <Wordmark dark size={30} />
          <p>{t("landing.footer.operator")}</p>
          <p>{t("landing.footer.noAdvice")}</p>
        </div>
        <nav aria-label={t("landing.footer.product")}>
          <h2 className="footer-h">{t("landing.footer.product")}</h2>
          <ul>
            <li><Link to="/#produk" onClick={section("produk")}>{t("landing.footer.tour")}</Link></li>
            <li><Link to="/#cara-kerja" onClick={section("cara-kerja")}>{t("landing.footer.how")}</Link></li>
            {principal.kind === "public" && <li><Link to="/#paket" onClick={section("paket")}>{t("nav.plans")}</Link></li>}
            <li>{signedIn ? <Link to={principal.kind === "operator" ? "/operasi" : "/ringkasan"}>{t("nav.openWorkspace")}</Link> : <Link to="/masuk">{t("shell.signin")}</Link>}</li>
          </ul>
        </nav>
        <nav aria-label={t("landing.footer.data")}>
          <h2 className="footer-h">{t("landing.footer.data")}</h2>
          <ul>
            <li><Link to="/metodologi">{t("landing.nav.data")}</Link></li>
            <li><Link to="/#sumber" onClick={section("sumber")}>{t("landing.footer.sources")}</Link></li>
            <li><ExternalA href={ESDM_ICP_URL}>{t("landing.footer.esdm")}</ExternalA></li>
          </ul>
        </nav>
        <nav aria-label={t("landing.footer.legal")}>
          <h2 className="footer-h">{t("landing.footer.legal")}</h2>
          <ul><li><Link to="/syarat">{t("landing.footer.terms")}</Link></li><li><Link to="/privasi">{t("landing.footer.privacy")}</Link></li></ul>
        </nav>
      </div>
    </footer>
  );
}

/**
 * Header + main + footer for the public pages. Below 900 px the links collapse behind a menu
 * button (nothing is hidden without a replacement): the open menu overlays the page and closes on
 * Escape (focus returns to the button), on an outside click or tap, and on navigation.
 */
export function PublicLayout({ children, className = "" }: { children: ReactNode; className?: string }) {
  const { t } = useT();
  const { principal, ready, sessionCheckError } = useAuth();
  const [open, setOpen] = useState(false);
  const location = useLocation();
  const headerRef = useRef<HTMLElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const section = useSectionLink();
  useEffect(() => { setOpen(false); }, [location.pathname, location.hash]);
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      setOpen(false);
      if (headerRef.current?.contains(document.activeElement)) buttonRef.current?.focus();
    };
    const onPointer = (e: PointerEvent) => { if (!headerRef.current?.contains(e.target as Node)) setOpen(false); };
    window.addEventListener("keydown", onKey);
    document.addEventListener("pointerdown", onPointer);
    return () => { window.removeEventListener("keydown", onKey); document.removeEventListener("pointerdown", onPointer); };
  }, [open]);
  const signedIn = ready && principal.kind !== "public";
  // A stored credential that is still being checked offers neither sign-in nor the workspace yet; one
  // that could not be checked (server unreachable) is kept, so it offers the workspace, whose page
  // explains the situation, instead of "Masuk" and "Buat akun".
  const checking = !ready && !!(getSessionToken() || getOperatorToken());
  const unconfirmed = ready && principal.kind === "public" && !!sessionCheckError;
  const workspace = principal.kind === "operator" ? "/operasi" : "/ringkasan";
  const mainRef = useRef<HTMLElement>(null);
  const announce = useRouteFocus(mainRef);
  useStickyTop(headerRef);
  return (
    <div className={`landing ${className}`.trim()}>
      <a href="#main" className="skip-link">{t("shell.skip")}</a>
      <div className="sr-only" role="status" aria-live="polite">{announce}</div>
      <header className="landing-nav" ref={headerRef}>
        <div className="nav-inner">
          <Link to="/" className="brand-link" aria-label={t("brand.name")}><Wordmark size={30} /></Link>
          <button type="button" ref={buttonRef} className="btn sm public-menu-btn" aria-expanded={open} aria-controls="public-menu" onClick={() => setOpen((o) => !o)}>
            {open ? <X size={16} aria-hidden="true" /> : <Menu size={16} aria-hidden="true" />}{t("nav.menu")}
          </button>
          <nav id="public-menu" className={`public-menu${open ? " open" : ""}`} aria-label={t("nav.site")}>
            <div className="links">
              <Link to="/#produk" onClick={section("produk")}>{t("landing.nav.product")}</Link>
              <NavLink to="/metodologi" className={({ isActive }) => (isActive ? "active" : undefined)}>{t("landing.nav.data")}</NavLink>
              {principal.kind === "user" ? <Link to="/akun#paket">{t("nav.plans")}</Link>
                : principal.kind === "public" ? <Link to="/#paket" onClick={section("paket")}>{t("nav.plans")}</Link> : null}
            </div>
            <div className="public-actions">
              <LanguageToggle />
              {checking ? null : signedIn || unconfirmed ? <Link className="btn sm primary" to={workspace}>{t("nav.openWorkspace")}</Link> : (<>
                <Link className="btn sm ghost" to="/masuk">{t("shell.signin")}</Link>
                <Link className="btn sm primary" to="/daftar">{t("landing.nav.register")}</Link>
              </>)}
            </div>
          </nav>
        </div>
      </header>
      <main id="main" ref={mainRef} tabIndex={-1} className="public-main">{children}</main>
      <PublicFooter />
    </div>
  );
}
