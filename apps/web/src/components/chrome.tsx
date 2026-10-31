/**
 * Chrome shared by the public layout (components/public-layout.tsx) and the signed-in shell
 * (components/layout.tsx): the language toggle, route-change focus management and the sticky
 * header offset.
 */
import { Globe } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState, type RefObject } from "react";
import { useLocation } from "react-router-dom";
import { useT } from "../i18n";

/** ID / EN switch. `dark` styles it for dark brand surfaces (operator sidebar, auth side panel). */
export function LanguageToggle({ dark }: { dark?: boolean }) {
  const { locale, setLocale, t } = useT();
  return (
    <div className={`lang-toggle${dark ? " dark" : ""}`} role="group" aria-label={t("shell.language")}>
      <Globe size={14} aria-hidden="true" />
      <button type="button" aria-pressed={locale === "id"} onClick={() => setLocale("id")}>ID</button>
      <button type="button" aria-pressed={locale === "en"} onClick={() => setLocale("en")}>EN</button>
    </div>
  );
}

/**
 * Keeps `--sticky-top` on the root element equal to the height of the layout's sticky header, so
 * the root's `scroll-padding-top` (styles/base.css) stops focused elements and in-page links just
 * below the header instead of under it (WCAG 2.2 SC 2.4.11). The height is measured, not assumed:
 * the public header is 57-71 px at the default text size depending on the width and the pointer,
 * and grows with the reader's text size (about 89 px at 200 % text).
 */
export function useStickyTop(header: RefObject<HTMLElement | null>): void {
  useLayoutEffect(() => {
    const el = header.current;
    if (!el) return;
    const root = document.documentElement;
    const update = () => root.style.setProperty("--sticky-top", `${Math.ceil(el.getBoundingClientRect().height)}px`);
    update();
    const ro = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(update);
    ro?.observe(el);
    return () => { ro?.disconnect(); root.style.removeProperty("--sticky-top"); };
  }, [header]);
}

/**
 * After a client-side route change, move focus to the new page heading (h1, made focusable) and
 * announce the page title politely, so keyboard and screen-reader users know a new page loaded.
 * The first render is left alone (the browser already starts at the top of the document).
 */
export function useRouteFocus(main: RefObject<HTMLElement | null>, onNavigate?: () => void): string {
  const { pathname } = useLocation();
  // Previous path, not a "first render" flag: StrictMode runs effects twice on mount.
  const prev = useRef(pathname);
  const [announce, setAnnounce] = useState("");
  const onNavRef = useRef(onNavigate);
  onNavRef.current = onNavigate;
  useEffect(() => {
    if (prev.current === pathname) return;
    prev.current = pathname;
    onNavRef.current?.();
    const root = main.current;
    if (!root) return;
    const tryFocus = (): boolean => {
      const h = root.querySelector<HTMLElement>("h1");
      if (!h) return false;
      if (!h.hasAttribute("tabindex")) h.setAttribute("tabindex", "-1");
      h.focus({ preventScroll: true });
      return true;
    };
    const say = window.setTimeout(() => setAnnounce(document.title), 120);
    if (tryFocus()) return () => window.clearTimeout(say);
    // Lazy pages render their heading a moment later: wait for it (bounded).
    const mo = new MutationObserver(() => { if (tryFocus()) mo.disconnect(); });
    mo.observe(root, { childList: true, subtree: true });
    const stop = window.setTimeout(() => mo.disconnect(), 5000);
    return () => { mo.disconnect(); window.clearTimeout(stop); window.clearTimeout(say); };
  }, [pathname, main]);
  return announce;
}
