/**
 * Deep links into pages whose sections load one after another (/#paket, /akun#paket): the target is
 * scrolled to as soon as it exists, but content above it often grows afterwards (a list or a panel
 * that arrives with its data) and pushes it down, out of view. `keepAnchored` keeps the target's top
 * at the top of the viewport (below the sticky header: the root's scroll-padding-top, base.css)
 * while anything above it changes size, until the reader scrolls or interacts, or `maxMs` passes.
 */

/** Interactions that mean the reader has taken over scrolling (the alignment stops). */
const TAKEOVER = ["wheel", "touchstart", "keydown", "pointerdown"] as const;

/** Starts keeping `el` aligned; returns the function that stops it (safe to call twice). */
export function keepAnchored(el: HTMLElement, maxMs = 12_000): () => void {
  if (typeof ResizeObserver === "undefined") return () => {};
  const docTop = () => el.getBoundingClientRect().top + window.scrollY;
  let top = docTop();
  const ro = new ResizeObserver(() => {
    const now = docTop();
    if (Math.abs(now - top) < 1) return;
    top = now;
    try { el.scrollIntoView({ block: "start", behavior: "auto" }); } catch { /* jsdom */ }
  });
  // Everything that can push the target down: the earlier siblings of the target and of each of
  // its ancestors (a section nested in a wrapper is moved by the wrapper's earlier siblings too).
  for (let n: Element | null = el; n && n !== document.body; n = n.parentElement) {
    for (let s = n.previousElementSibling; s; s = s.previousElementSibling) ro.observe(s);
  }
  let timer = 0;
  const stop = () => {
    ro.disconnect();
    window.clearTimeout(timer);
    for (const ev of TAKEOVER) window.removeEventListener(ev, stop);
  };
  timer = window.setTimeout(stop, maxMs);
  for (const ev of TAKEOVER) window.addEventListener(ev, stop, { passive: true });
  return stop;
}
