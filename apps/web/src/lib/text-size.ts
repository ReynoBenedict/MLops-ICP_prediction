/**
 * The reader's text size, for drawings that place their own text (the SVG charts: label widths,
 * axis room, the distance between two end labels). Type tokens are in rem (DESIGN_SYSTEM.md,
 * Typography), so chart text follows the browser's text-size setting (WCAG 1.4.4); geometry computed
 * in px must follow it as well, or labels collide at 200 % text.
 *
 * `useMetaPx()` is the px size of `--fs-meta` (the chart tick and label size): 12.5 at the default
 * 16 px browser text size, 25 at 200 %. It is read from the root element (the token's rem value times
 * the root font size), and a hidden 10rem probe watched by a ResizeObserver re-reads it when the
 * setting changes while the page is open. Without layout (tests) it is the default 12.5.
 */
import { useSyncExternalStore } from "react";

/** `--fs-meta` at the default browser text size (px). */
export const META_PX = 12.5;

let current = META_PX;
let started = false;
const listeners = new Set<() => void>();

function read(): number {
  try {
    const cs = getComputedStyle(document.documentElement);
    const rem = parseFloat(cs.fontSize);
    const remPx = Number.isFinite(rem) && rem > 0 ? rem : 16;
    const token = cs.getPropertyValue("--fs-meta").trim();
    const v = parseFloat(token);
    if (Number.isFinite(v) && v > 0) {
      if (token.endsWith("rem")) return v * remPx;
      if (token.endsWith("px")) return v;
    }
    return META_PX * (remPx / 16);
  } catch {
    return META_PX;
  }
}

function start(): void {
  if (started || typeof document === "undefined" || !document.body) return;
  started = true;
  current = read();
  if (typeof ResizeObserver === "undefined") return;
  const probe = document.createElement("div");
  probe.setAttribute("aria-hidden", "true");
  probe.style.cssText = "position:absolute;left:0;top:0;width:10rem;height:0;overflow:hidden;visibility:hidden;pointer-events:none";
  document.body.appendChild(probe);
  new ResizeObserver(() => {
    const next = read();
    if (Math.abs(next - current) < 0.01) return;
    current = next;
    listeners.forEach((l) => l());
  }).observe(probe);
}

function subscribe(listener: () => void): () => void {
  start();
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}

function snapshot(): number {
  if (!started) start();
  return current;
}

/** px size of `--fs-meta` (12.5 at the default text size); see the module comment. */
export function useMetaPx(): number {
  return useSyncExternalStore(subscribe, snapshot, () => META_PX);
}
