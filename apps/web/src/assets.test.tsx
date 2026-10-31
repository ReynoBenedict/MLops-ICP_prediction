/**
 * Images of the public site and the not-found page (wave E assets). Every <img> has a text alternative
 * or is marked decorative (alt="" together with aria-hidden), carries its intrinsic size (no layout
 * shift while it loads), images below the first screen load lazily, nothing is drawn over the market
 * panel, and every image URL the app references exists in public/, so a renamed or deleted file cannot
 * turn into a broken image. Each file's licence is checked by scripts/check-asset-licences.mjs against
 * the register in docs/ASSET_INVENTORY.md.
 */
import { act, cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { AuthProvider } from "./auth";
import { I18nProvider } from "./i18n";

// Node's fs, loaded at run time: the web tsconfig has no Node types, and vitest runs in Node. The
// folder is derived from this file's URL as a string (jsdom's URL resolves relative URLs against its
// own document address, not against a file: URL).
const FS = "node:fs";
const { existsSync, readFileSync, readdirSync } = (await import(/* @vite-ignore */ FS)) as {
  existsSync(path: string): boolean; readFileSync(path: string, encoding: "utf8"): string; readdirSync(path: string): string[];
};
const SRC_DIR = decodeURIComponent(import.meta.url.replace(/^file:\/\//, "")).replace(/^\/([A-Za-z]:)/, "$1").replace(/\/[^/]+$/, "");
const PUBLIC_DIR = SRC_DIR.replace(/\/src$/, "/public");
const inPublic = (url: string) => url.startsWith("/") && existsSync(PUBLIC_DIR + url);

/** Application sources (tests excluded) and the HTML entry, keyed by path. */
const SOURCES = import.meta.glob<string>(["/src/**/*.{ts,tsx}", "!/src/**/*.test.{ts,tsx}", "!/src/test/**", "/index.html"], {
  query: "?raw", import: "default", eager: true,
});
/** The stylesheets, read from disk (vitest serves CSS modules as empty strings, even with ?raw). */
const STYLES: Record<string, string> = Object.fromEntries(readdirSync(`${SRC_DIR}/styles`).filter((f) => f.endsWith(".css"))
  .map((f) => [f, readFileSync(`${SRC_DIR}/styles/${f}`, "utf8")]));

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const notFound = () => json({ error: { code: "NOT_FOUND", message: "nope", request_id: null, details: {} } }, 404);
const mount = (path: string) => render(<MemoryRouter initialEntries={[path]}><I18nProvider initial="id"><AuthProvider><App /></AuthProvider></I18nProvider></MemoryRouter>);
/**
 * A browser window at least 1101 px wide (the sign-in brand panel's breakpoint); other queries do not
 * match, unless `forcedColors` adds a Windows contrast theme.
 */
const wideWindow = (forcedColors = false) => vi.stubGlobal("matchMedia", (q: string) => ({
  matches: q.includes("min-width: 1101px") || (forcedColors && q.includes("forced-colors: active")), media: q,
  addEventListener: () => undefined, removeEventListener: () => undefined,
}));

/** Every URL an <img> or <source> can load: its src and each srcset candidate. */
function imageUrls(root: ParentNode): string[] {
  const urls: string[] = [];
  for (const el of root.querySelectorAll("img, source")) {
    const src = el.getAttribute("src");
    if (src) urls.push(src);
    for (const candidate of (el.getAttribute("srcset") ?? "").split(",")) {
      const url = candidate.trim().split(/\s+/)[0];
      if (url) urls.push(url);
    }
  }
  return urls;
}

/** Alt text or decorative marking, and an intrinsic size, on every <img>; every file it can load exists. */
function checkImages(root: ParentNode): HTMLImageElement[] {
  const imgs = [...root.querySelectorAll("img")];
  for (const img of imgs) {
    expect(img.hasAttribute("alt"), img.outerHTML).toBe(true);
    if (img.getAttribute("alt") === "") expect(img, img.outerHTML).toHaveAttribute("aria-hidden", "true");
    expect(img.hasAttribute("width") && img.hasAttribute("height"), `intrinsic size: ${img.outerHTML}`).toBe(true);
  }
  expect(imageUrls(root).filter((url) => !inPublic(url))).toEqual([]);
  return imgs;
}

afterEach(() => { cleanup(); vi.unstubAllGlobals(); localStorage.clear(); sessionStorage.clear(); });

describe("images", () => {
  it("landing: decorative step illustrations (one sprite) and a lazy closing photograph; the hero stays the market panel", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    const { container } = mount("/");
    expect(await screen.findByRole("heading", { level: 1 })).toHaveTextContent("ICP resmi, lengkap dengan dokumen sumbernya.");
    const imgs = checkImages(container);
    // One tile per step, hidden from assistive technology; the drawing is a cell of one CSS sprite.
    const tiles = [...container.querySelectorAll("#cara-kerja .flow-step > .flow-art")];
    expect(tiles.map((tile) => tile.className)).toEqual(["flow-art flow-art-s1", "flow-art flow-art-s2", "flow-art flow-art-s3", "flow-art flow-art-s4"]);
    for (const tile of tiles) expect(tile).toHaveAttribute("aria-hidden", "true");
    // The sprite is attached once the list nears the viewport; jsdom has no IntersectionObserver, so at once.
    expect(container.querySelector("#cara-kerja ol.flow")).toHaveClass("flow-near");
    expect(STYLES["public.css"]).toMatch(/\.flow-near \.flow-art \{[^}]*url\("\/img\/illus\/steps\.svg"\)/);
    const photo = container.querySelector(".closing picture")!;
    expect(photo).toHaveAttribute("aria-hidden", "true");
    expect(photo.querySelector("source")).toHaveAttribute("type", "image/avif");
    expect(photo.querySelector("img")).toHaveAttribute("loading", "lazy");
    // The horizon and its ships stay in the band's bottom padding, clear of the text and the buttons: the
    // picture box is twice the band's height and starts 24 px above it (its centre is the photo's horizon).
    // That draws the photograph several times wider than a phone screen, so it ships one 1600 px rendition.
    expect(STYLES["public.css"]).toMatch(/\.closing-photo \{[^}]*top: calc\(-1 \* var\(--s-6\)\);[^}]*height: 200%;/);
    expect(photo.querySelector("source")).toHaveAttribute("srcset", "/img/photo/ships-on-horizon-1600.avif");
    // Nothing in the hero but the copy and the market panel (no image can become the first screen's largest paint).
    expect(container.querySelector(".hero img, .hero picture")).toBeNull();
    // The product tour screenshot is the one informative image: it keeps its description.
    expect(imgs.filter((img) => img.getAttribute("alt") !== "").map((img) => img.getAttribute("src"))).toEqual(["/img/product/riwayat-harga.webp"]);
  });

  it("landing: the step sprite is attached only once the steps come within 200 px of the viewport", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    const observers: Array<{ margin: string | undefined; fire: (hit: boolean) => void }> = [];
    vi.stubGlobal("IntersectionObserver", class {
      constructor(cb: (entries: Array<{ isIntersecting: boolean }>) => void, options?: { rootMargin?: string }) {
        observers.push({ margin: options?.rootMargin, fire: (hit) => cb([{ isIntersecting: hit }]) });
      }
      observe() { /* fired by hand */ }
      unobserve() { /* nothing to do */ }
      disconnect() { /* nothing to do */ }
    });
    const { container } = mount("/");
    await screen.findByRole("heading", { level: 1 });
    const list = container.querySelector("#cara-kerja ol.flow")!;
    const io = observers.find((o) => o.margin === "200px 0px")!;
    expect(io).toBeDefined();
    expect(list).not.toHaveClass("flow-near");
    act(() => io.fire(false));
    expect(list).not.toHaveClass("flow-near");
    act(() => io.fire(true));
    expect(list).toHaveClass("flow-near");
  });

  it("sign-in: the brand panel's photograph is decorative, keeps normal priority, and is only requested while the panel is shown", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    wideWindow();
    const { container, unmount } = mount("/masuk");
    expect(await screen.findByRole("heading", { level: 1, name: "Masuk ke ICP Intelligence" })).toBeInTheDocument();
    checkImages(container);
    const img = container.querySelector(".auth-side .auth-photo img")!;
    expect(img).toHaveAttribute("alt", "");
    // On a wide window it is the page's largest paint, so it is not deprioritised (wave E review P2-1).
    expect(img).not.toHaveAttribute("fetchpriority");
    expect(container.querySelector(".auth-photo source")).toHaveAttribute("type", "image/avif");
    unmount();
    vi.unstubAllGlobals();

    // Windows contrast theme: the shade's gradients are dropped there, so the photograph is left out
    // (and the stylesheet hides both photographs); the live card stays.
    vi.stubGlobal("fetch", async () => notFound());
    wideWindow(true);
    const forced = mount("/masuk");
    expect(await screen.findByRole("heading", { level: 1, name: "Masuk ke ICP Intelligence" })).toBeInTheDocument();
    expect(forced.container.querySelector(".auth-side .auth-card")).not.toBeNull();
    expect(forced.container.querySelector(".auth-photo")).toBeNull();
    expect(STYLES["public.css"]).toMatch(/@media \(forced-colors: active\) \{\s*\.closing-photo, \.auth-photo \{ display: none; \}/);
    forced.unmount();
    vi.unstubAllGlobals();

    // Narrow window (no matchMedia match): the panel is hidden, so neither the card nor the photograph is mounted.
    vi.stubGlobal("fetch", async () => notFound());
    const narrow = mount("/daftar");
    expect(await screen.findByRole("heading", { level: 1, name: "Buat organisasi baru" })).toBeInTheDocument();
    expect(narrow.container.querySelector(".auth-photo")).toBeNull();
  });

  it("methodology has a decorative, lazy illustration beside its title; the legal pages stay without one", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    const { container, unmount } = mount("/metodologi");
    await screen.findByRole("heading", { level: 1, name: "Dari mana angka berasal dan seberapa jauh dapat dipercaya" });
    checkImages(container);
    const art = container.querySelector(".doc.has-art .doc-art")!;
    expect(art).toHaveAttribute("aria-hidden", "true");
    expect(art.querySelector("img")).toHaveAttribute("loading", "lazy");
    unmount();
    const legal = mount("/syarat");
    await screen.findByRole("heading", { level: 1, name: "Syarat Penggunaan" });
    expect(legal.container.querySelector(".doc-art, .doc img")).toBeNull();
  });

  it("not-found page: decorative illustration and a way back home", async () => {
    vi.stubGlobal("fetch", async () => notFound());
    const { container } = mount("/halaman-yang-tidak-ada");
    expect(await screen.findByRole("heading", { level: 1, name: "Halaman tidak ditemukan" })).toBeInTheDocument();
    const [img] = checkImages(container);
    expect(img).toHaveAttribute("src", "/img/illus/not-found.svg");
    expect(img).toHaveAttribute("alt", "");
    expect(screen.getByRole("link", { name: "Kembali ke beranda" })).toHaveAttribute("href", "/");
  });

  it("every image path in the source, the stylesheets and index.html exists in public/", () => {
    const refs = new Set<string>(["/favicon.svg"]);
    for (const code of [...Object.values(SOURCES), ...Object.values(STYLES)]) {
      for (const m of code.matchAll(/\/(?:img|brand)\/[\w./-]+?\.(?:svg|png|jpe?g|webp|avif|gif)\b/g)) refs.add(m[0]);
    }
    expect(refs.size).toBeGreaterThan(20);
    expect([...refs].filter((url) => !inPublic(url))).toEqual([]);
  });
});
