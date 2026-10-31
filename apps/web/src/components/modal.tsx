/**
 * Modal behaviour shared by every overlay (navigation drawer, confirmation dialog, insight detail
 * sheet): focus moves in, Tab stays inside, the rest of the page is `inert` (not focusable, not
 * read by screen readers), the page behind does not scroll, Escape closes, and focus returns to
 * the element that opened the overlay. Nothing here animates; motion lives in CSS and is switched
 * off by `prefers-reduced-motion`.
 */
import { X } from "lucide-react";
import { useEffect, useId, useLayoutEffect, useRef, type ReactNode, type RefObject } from "react";
import { createPortal } from "react-dom";
import { useT } from "../i18n";

const FOCUSABLE = "a[href], area[href], button:not([disabled]), input:not([disabled]):not([type=hidden]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1']), summary";

export function focusables(root: HTMLElement): HTMLElement[] {
  // Hidden/inert subtrees are skipped; layout is not consulted (jsdom has none, and CSS-hidden
  // controls inside overlays are always removed from the DOM instead of hidden).
  return [...root.querySelectorAll<HTMLElement>(FOCUSABLE)].filter((el) => !el.closest("[inert],[hidden]"));
}

export interface ModalOptions {
  onClose: () => void;
  /** Element focused on open (default: first focusable inside the container, else the container). */
  initialFocus?: RefObject<HTMLElement | null>;
  /** Elements made inert while open (default: every <body> child that does not contain the container). */
  inertTargets?: () => Array<HTMLElement | null | undefined>;
  /** Return focus to the opener on close (default true). Read at close time, so a ref-backed getter works. */
  returnFocus?: () => boolean;
  /** Escape is ignored while true (e.g. a request is in flight). */
  locked?: boolean;
}

/** Apply modal behaviour to `container` while `open` is true. */
export function useModal(open: boolean, container: RefObject<HTMLElement | null>, opts: ModalOptions): void {
  const optsRef = useRef(opts);
  optsRef.current = opts;
  // Keys first, in a layout effect: Escape/Tab handling is attached in the same commit that shows the
  // overlay, so a key pressed right after it appears is never lost (a passive effect can run later).
  useLayoutEffect(() => {
    if (!open) return;
    const box = container.current;
    if (!box) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !optsRef.current.locked) { e.preventDefault(); e.stopPropagation(); optsRef.current.onClose(); return; }
      if (e.key !== "Tab") return;
      const f = focusables(box);
      if (!f.length) { e.preventDefault(); box.focus(); return; }
      const a = f[0]!, z = f[f.length - 1]!;
      const active = document.activeElement as HTMLElement | null;
      if (!active || !box.contains(active)) { e.preventDefault(); (e.shiftKey ? z : a).focus(); return; }
      if (e.shiftKey && active === a) { e.preventDefault(); z.focus(); }
      else if (!e.shiftKey && active === z) { e.preventDefault(); a.focus(); }
    };
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, [open, container]);

  useEffect(() => {
    if (!open) return;
    const box = container.current;
    if (!box) return;
    const opener = document.activeElement as HTMLElement | null;

    // Inert background.
    const targets = (optsRef.current.inertTargets?.() ?? [...document.body.children].filter((el) => !el.contains(box)) as HTMLElement[])
      .filter((el): el is HTMLElement => !!el && !el.hasAttribute("inert") && !el.contains(box));
    for (const el of targets) el.setAttribute("inert", "");

    // Scroll lock on the document (the only page scroller).
    const html = document.documentElement;
    const prevOverflow = html.style.overflow;
    html.style.overflow = "hidden";

    const first = optsRef.current.initialFocus?.current ?? focusables(box)[0] ?? box;
    if (first === box && !box.hasAttribute("tabindex")) box.setAttribute("tabindex", "-1");
    first.focus({ preventScroll: true });

    return () => {
      for (const el of targets) el.removeAttribute("inert");
      html.style.overflow = prevOverflow;
      const back = optsRef.current.returnFocus?.() ?? true;
      if (back && opener && opener.isConnected) opener.focus({ preventScroll: true });
    };
  }, [open, container]);
}

/**
 * Side drawer (≥ 561 px, right edge) / bottom sheet (≤ 560 px) dialog with a pinned header and
 * footer; only the body scrolls. Used for detail views that keep the list visible behind them.
 */
export function Sheet({ open, title, eyebrow, onClose, children, footer, className = "", labelClose }:
  { open: boolean; title: ReactNode; eyebrow?: ReactNode; onClose: () => void; children: ReactNode; footer?: ReactNode; className?: string; labelClose?: string }) {
  const { t } = useT();
  const titleId = useId();
  const boxRef = useRef<HTMLDivElement>(null);
  const headingRef = useRef<HTMLHeadingElement>(null);
  useModal(open, boxRef, { onClose, initialFocus: headingRef });
  if (!open) return null;
  return createPortal(
    <div className="sheet-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div ref={boxRef} className={`sheet ${className}`} role="dialog" aria-modal="true" aria-labelledby={titleId}>
        <div className="sheet-handle" aria-hidden="true" />
        <div className="sheet-head">
          <div className="grow">
            {eyebrow && <div className="sheet-eyebrow">{eyebrow}</div>}
            <h2 id={titleId} ref={headingRef} tabIndex={-1} className="sheet-title">{title}</h2>
          </div>
          <button type="button" className="icon-close" onClick={onClose} aria-label={labelClose ?? t("common.close")}><X size={18} aria-hidden="true" /></button>
        </div>
        <div className="sheet-body">{children}</div>
        {footer && <div className="sheet-foot">{footer}</div>}
      </div>
    </div>,
    document.body,
  );
}
