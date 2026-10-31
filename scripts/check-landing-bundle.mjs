#!/usr/bin/env node
// Landing bundle guard (CI). The public site must not load the charting library: Recharts (with d3 and
// its redux stack, ~385 kB minified) belongs to the workspace chart pages only; the public site draws its
// own small SVG charts (docs/DESIGN_SYSTEM.md, "Data series"). Two checks:
//  1. the JavaScript that `index.html` loads before anything is lazily imported (the entry module, its
//     modulepreloads and every chunk they import statically) contains no Recharts code;
//  2. neither does the chunk of any lazily loaded public route (PUBLIC_ROUTE_MODULES: methodology, legal
//     and auth pages, and the landing should it become lazy) nor any chunk it imports statically. Rollup
//     names the chunk of a dynamically imported module after the module (`MethodologyPage-<hash>.js`);
//     against the repository's own build the source is cross-checked, so a renamed page or a lazy page
//     without a recognisable chunk fails instead of silently dropping out of the check.
// Prints the JavaScript size of each, raw and gzip (level 9), file by file.
//
// Usage:  node scripts/check-landing-bundle.mjs [distDir] [--src dir] [--max-gzip-bytes N] [--json]
//         node scripts/check-landing-bundle.mjs --self-test
// distDir defaults to apps/web/dist; --src (the web app's src/, with App.tsx and pages/) defaults to
// apps/web/src when distDir is apps/web/dist and is skipped for any other build unless given.
// --max-gzip-bytes applies to the landing JavaScript (check 1). Exit codes: 0 ok, 1 guard failed,
// 2 bad input (no build).
//
// Budget (CI, .github/workflows/ci.yml): --max-gzip-bytes 160000. Set on 2026-10-26 when the signed-in
// shell, the sign-in pages, the workspace strings and the workspace stylesheets left the entry bundle:
// the landing JavaScript measured 152366 B gzip (entry 84551 + shared chunk 67815; before the split
// 183578 B), and the budget is that plus about 5 %. Raise it only together with a reason in this comment.
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_DIST = path.join(ROOT, "apps", "web", "dist");
const DEFAULT_SRC = path.join(ROOT, "apps", "web", "src");

/**
 * Strings that only Recharts output contains. Minifiers keep them because they are CSS class names
 * written into the DOM (`recharts-wrapper`, `recharts-surface`, ...) or the library's display names.
 */
export const RECHARTS_MARKERS = [
  /recharts-wrapper/,
  /recharts-surface/,
  /recharts-cartesian-(?:grid|axis)/,
  /recharts-legend-wrapper/,
  /recharts-tooltip-wrapper/,
];

/**
 * Page modules (apps/web/src/pages/<name>.tsx) of the public routes in App.tsx: `/` (LandingPage),
 * `/metodologi`, `/masuk` and `/daftar` (AuthPages), `/syarat` and `/privasi` (LegalPages). A page that is
 * imported statically has no chunk of its own and is part of the landing closure (check 1).
 */
export const PUBLIC_ROUTE_MODULES = ["LandingPage", "MethodologyPage", "LegalPages", "AuthPages"];

/** Static ES module imports and re-exports in built code; `import("...")` (lazy) is deliberately not matched. */
const STATIC_IMPORT = /(?:\bfrom\s*|\bimport\s*)["']([^"'()\s]+\.m?js)["']/g;

function attr(tag, name) {
  const m = tag.match(new RegExp(`\\s${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, "i"));
  return m ? (m[1] ?? m[2] ?? m[3]) : null;
}

function inputError(message) {
  return Object.assign(new Error(message), { code: 2 });
}

/** Script sources and modulepreload targets referenced by the HTML, in document order. */
export function initialScripts(html) {
  const found = [];
  for (const tag of html.match(/<script\b[^>]*>/gi) ?? []) {
    const src = attr(tag, "src");
    if (src) found.push(src);
  }
  for (const tag of html.match(/<link\b[^>]*>/gi) ?? []) {
    const rel = (attr(tag, "rel") ?? "").toLowerCase().split(/\s+/);
    const href = attr(tag, "href");
    if (href && rel.includes("modulepreload")) found.push(href);
  }
  return found;
}

/** Resolve a URL from index.html or an import specifier to a file inside dist (null when external). */
function resolveInDist(dist, spec, fromFile) {
  if (/^[a-z][a-z0-9+.-]*:/i.test(spec) || spec.startsWith("//")) return null; // absolute URL: not ours
  const clean = spec.split(/[?#]/)[0];
  const file = clean.startsWith("/") ? path.join(dist, clean) : path.resolve(fromFile ? path.dirname(fromFile) : dist, clean);
  const rel = path.relative(dist, file);
  if (rel.startsWith("..") || path.isAbsolute(rel)) return null;
  return file;
}

/** The given files plus every file they import statically (transitively), in discovery order. */
function staticClosure(dist, roots) {
  const queue = [...roots];
  const seen = new Map();
  while (queue.length) {
    const file = queue.shift();
    if (seen.has(file)) continue;
    if (!existsSync(file)) throw inputError(`missing file referenced by the build: ${path.relative(dist, file)}`);
    const code = readFileSync(file);
    seen.set(file, code);
    for (const m of code.toString("utf8").matchAll(STATIC_IMPORT)) {
      const next = resolveInDist(dist, m[1], file);
      if (next && !seen.has(next)) queue.push(next);
    }
  }
  return [...seen.entries()].map(([file, code]) => ({ file, code }));
}

/** The landing JS closure: entry + modulepreloads + their static imports (transitively). */
export function landingClosure(dist) {
  const indexHtml = path.join(dist, "index.html");
  if (!existsSync(indexHtml)) throw inputError(`no build found: ${indexHtml} does not exist`);
  const html = readFileSync(indexHtml, "utf8");
  const roots = [];
  const external = [];
  for (const spec of initialScripts(html)) {
    const file = resolveInDist(dist, spec, null);
    if (file) roots.push(file);
    else external.push(spec);
  }
  if (roots.length === 0) throw inputError("index.html loads no local JavaScript module");
  return { files: staticClosure(dist, roots), external };
}

function jsFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...jsFiles(full));
    else if (/\.m?js$/.test(entry.name)) out.push(full);
  }
  return out;
}

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** For each public page module, the chunk files Rollup named after it (`<name>-<hash>.js`, anywhere in dist). */
export function publicRouteChunks(dist, names = PUBLIC_ROUTE_MODULES) {
  const files = jsFiles(dist);
  return names.map((name) => {
    const re = new RegExp(`^${escapeRegExp(name)}-[A-Za-z0-9_-]+\\.m?js$`);
    return { name, chunks: files.filter((f) => re.test(path.basename(f))).sort() };
  });
}

/**
 * Source cross-check (only against a build of that source): every listed page module exists, and a page
 * App.tsx imports lazily has a chunk named after it, so it cannot drop out of the check unnoticed.
 */
function sourceProblems(src, routes) {
  const problems = [];
  const pages = path.join(src, "pages");
  const appFile = path.join(src, "App.tsx");
  const app = existsSync(appFile) ? readFileSync(appFile, "utf8") : null;
  for (const { name, chunks } of routes) {
    const exists = [".tsx", ".ts", ".jsx", ".js"].some((ext) => existsSync(path.join(pages, name + ext)));
    if (!exists) {
      problems.push(`public route module ${name} not found in ${path.relative(ROOT, pages) || pages}: update PUBLIC_ROUTE_MODULES in scripts/check-landing-bundle.mjs`);
      continue;
    }
    const lazy = app !== null && new RegExp(`\\bimport\\(\\s*["'\`]\\./pages/${escapeRegExp(name)}["'\`]\\s*\\)`).test(app);
    if (lazy && chunks.length === 0) {
      problems.push(`${name} is imported lazily in App.tsx but the build has no ${name}-<hash>.js chunk, so it cannot be checked (chunk naming changed?)`);
    }
  }
  return problems;
}

function sizeRows(dist, files) {
  return files.map(({ file, code }) => {
    const text = code.toString("utf8");
    return {
      file: path.relative(dist, file).split(path.sep).join("/"),
      raw: code.length,
      gzip: gzipSync(code, { level: 9 }).length,
      recharts: RECHARTS_MARKERS.filter((re) => re.test(text)).map((re) => re.source),
    };
  });
}

function totals(rows) {
  return { raw: rows.reduce((s, r) => s + r.raw, 0), gzip: rows.reduce((s, r) => s + r.gzip, 0) };
}

export function checkLanding(dist, { maxGzipBytes = null, src = null, routeModules = PUBLIC_ROUTE_MODULES } = {}) {
  const { files, external } = landingClosure(dist);
  const rows = sizeRows(dist, files);
  const total = totals(rows);
  const problems = [];
  for (const r of rows) if (r.recharts.length) problems.push(`${r.file} contains Recharts code (${r.recharts.join(", ")})`);
  if (external.length) problems.push(`index.html loads external scripts: ${external.join(", ")}`);
  if (maxGzipBytes !== null && total.gzip > maxGzipBytes) problems.push(`landing JS is ${total.gzip} B gzip, above the ${maxGzipBytes} B budget`);

  const routes = publicRouteChunks(dist, routeModules);
  const landingFiles = new Set(rows.map((r) => r.file));
  const publicRoutes = routes.map(({ name, chunks }) => {
    const none = { raw: 0, gzip: 0 };
    if (chunks.length === 0) return { name, lazy: false, rows: [], total: none, extra: [], extraTotal: none };
    const routeRows = sizeRows(dist, staticClosure(dist, chunks));
    for (const r of routeRows) {
      if (r.recharts.length && !landingFiles.has(r.file)) {
        problems.push(`${r.file} (loaded by the public route module ${name}) contains Recharts code (${r.recharts.join(", ")})`);
      }
    }
    // what the route loads on top of the landing JavaScript (shared chunks are already counted there)
    const extra = routeRows.filter((r) => !landingFiles.has(r.file));
    return { name, lazy: true, rows: routeRows, total: totals(routeRows), extra, extraTotal: totals(extra) };
  });
  if (src) problems.push(...sourceProblems(src, routes));
  return { rows, total, publicRoutes, problems };
}

function kb(n) {
  return `${(n / 1024).toFixed(1)} KiB`;
}

function printRows(rows, total, indent = "  ", label = "total") {
  for (const r of rows) {
    console.log(`${indent}${r.file.padEnd(40)} ${String(r.raw).padStart(9)} B  ${String(r.gzip).padStart(8)} B gzip${r.recharts.length ? "  <- Recharts" : ""}`);
  }
  console.log(`${indent}${label.padEnd(40)} ${String(total.raw).padStart(9)} B  ${String(total.gzip).padStart(8)} B gzip  (${kb(total.raw)}, ${kb(total.gzip)} gzip; ${rows.length} files)`);
}

function report(dist, result) {
  console.log(`Landing JavaScript loaded by ${path.relative(ROOT, path.join(dist, "index.html")) || "index.html"} (entry, modulepreloads and their static imports):`);
  printRows(result.rows, result.total);
  console.log("Lazily loaded public routes (each route's chunk and its static imports, all checked; listed: what it loads on top of the landing JavaScript):");
  for (const route of result.publicRoutes) {
    if (!route.lazy) {
      console.log(`  ${route.name}: no chunk of its own (imported statically, so part of the landing JavaScript above)`);
      continue;
    }
    console.log(`  ${route.name} (${route.rows.length} files checked):`);
    printRows(route.extra, route.extraTotal, "    ", "added to the landing");
  }
}

function selfTest() {
  const dir = mkdtempSync(path.join(os.tmpdir(), "landing-guard-"));
  const cases = [];
  const build = (name, files) => {
    const dist = path.join(dir, name);
    for (const [rel, content] of Object.entries(files)) {
      const file = path.join(dist, rel);
      mkdirSync(path.dirname(file), { recursive: true });
      writeFileSync(file, content);
    }
    return dist;
  };
  const html = (body) => `<!doctype html><html><head>${body}</head><body></body></html>`;
  const chart = 'const k="recharts-wrapper";export const C=()=>k;';
  const entry = '<script type="module" src="/assets/index-a.js"></script>';
  try {
    // 1. clean landing: the chart chunk is only imported lazily
    let dist = build("clean", {
      "index.html": html('<script type="module" crossorigin src="/assets/index-a.js"></script><link rel="modulepreload" crossorigin href="/assets/ui-b.js">'),
      "assets/index-a.js": 'import{u}from"./ui-b.js";const l=()=>import("./charts-c.js");export{u,l};',
      "assets/ui-b.js": "export const u=1;",
      "assets/charts-c.js": chart,
    });
    let r = checkLanding(dist);
    cases.push(["lazy chart chunk is allowed", r.problems.length === 0 && r.rows.length === 2]);
    // 2. Recharts in a modulepreloaded chunk
    dist = build("preloaded", {
      "index.html": html("<script type=module src=/assets/index-a.js></script><link rel=\"modulepreload\" href=\"/assets/charts-c.js\">"),
      "assets/index-a.js": "export const a=1;",
      "assets/charts-c.js": chart,
    });
    cases.push(["modulepreloaded Recharts fails", checkLanding(dist).problems.some((p) => p.includes("charts-c.js"))]);
    // 3. Recharts reached through a static import chain (no modulepreload link)
    dist = build("chain", {
      "index.html": html('<script type="module" src="./assets/index-a.js"></script>'),
      "assets/index-a.js": 'import"./mid-b.js";',
      "assets/mid-b.js": 'export*from"./charts-c.js";',
      "assets/charts-c.js": chart,
    });
    cases.push(["statically imported Recharts fails", checkLanding(dist).problems.some((p) => p.includes("charts-c.js"))]);
    // 4. Recharts inlined into the entry itself
    dist = build("inline", {
      "index.html": html(entry),
      "assets/index-a.js": 'const c="recharts-surface";export{c};',
    });
    cases.push(["Recharts in the entry fails", checkLanding(dist).problems.length === 1]);
    // 5. sizes and the optional budget
    dist = build("budget", {
      "index.html": html(entry),
      "assets/index-a.js": "x".repeat(5000),
    });
    r = checkLanding(dist, { maxGzipBytes: 10 });
    cases.push(["sizes are raw and gzip; budget enforced", r.total.raw === 5000 && r.total.gzip < 200 && r.problems.length === 1]);
    // 6. no build
    let threw = false;
    try { checkLanding(path.join(dir, "missing")); } catch (err) { threw = err.code === 2; }
    cases.push(["missing build is an input error", threw]);
    // 7. a lazily loaded public route that imports the chart chunk statically (Vite's template-literal import)
    const lazyRoutes = {
      "index.html": html(entry),
      "assets/index-a.js": "const m=()=>import(`./MethodologyPage-Ab1_x.js`),o=()=>import(`./OverviewPage-Cd2.js`);export{m,o};",
      "assets/DocLayout-Ef3.js": "export const d=1;",
      "assets/OverviewPage-Cd2.js": 'import{C}from"./charts-c.js";export default C;',
      "assets/charts-c.js": chart,
    };
    dist = build("public-route-chart", {
      ...lazyRoutes,
      "assets/MethodologyPage-Ab1_x.js": 'import{d}from"./DocLayout-Ef3.js";import{C}from"./charts-c.js";export default()=>[d,C];',
    });
    r = checkLanding(dist);
    cases.push(["public route chunk importing Recharts fails",
      r.problems.length === 1 && r.problems[0].includes("charts-c.js") && r.problems[0].includes("MethodologyPage")]);
    // 8. the same build with a clean public route: a lazy workspace page may use Recharts
    dist = build("public-route-clean", {
      ...lazyRoutes,
      "assets/MethodologyPage-Ab1_x.js": 'import{d}from"./DocLayout-Ef3.js";export default()=>d;',
    });
    r = checkLanding(dist);
    const methodology = r.publicRoutes.find((p) => p.name === "MethodologyPage");
    cases.push(["clean public route passes; workspace chart pages are not public routes",
      r.problems.length === 0 && methodology.lazy && methodology.rows.length === 2
        && r.publicRoutes.filter((p) => p.lazy).length === 1]);
    // 9. Recharts inside the public route chunk itself, in a nested directory
    dist = build("public-route-inline", {
      "index.html": html(entry),
      "assets/index-a.js": "export const a=()=>import(`./js/LegalPages-Zz9.js`);",
      "assets/js/LegalPages-Zz9.js": 'const s="recharts-cartesian-grid";export{s};',
    });
    cases.push(["Recharts inside a public route chunk fails", checkLanding(dist).problems.some((p) => p.includes("LegalPages-Zz9.js"))]);
    // 10. source cross-check: a lazy public page without a recognisable chunk, and a renamed page module
    const src = path.join(dir, "src");
    mkdirSync(path.join(src, "pages"), { recursive: true });
    for (const name of ["LandingPage", "MethodologyPage", "AuthPages"]) writeFileSync(path.join(src, "pages", `${name}.tsx`), "export {};");
    writeFileSync(path.join(src, "App.tsx"), 'const M = lazy(() => import("./pages/MethodologyPage"));\nimport { LoginPage } from "./pages/AuthPages";');
    dist = build("renamed-chunk", {
      "index.html": html(entry),
      "assets/index-a.js": "export const m=()=>import(`./methodology-Ab1.js`);",
      "assets/methodology-Ab1.js": "export default 1;",
    });
    r = checkLanding(dist, { src });
    cases.push(["source cross-check: lazy page without its chunk and a missing page module fail",
      r.problems.length === 2
        && r.problems.some((p) => p.includes("MethodologyPage is imported lazily"))
        && r.problems.some((p) => p.includes("LegalPages not found"))
        && checkLanding(dist).problems.length === 0]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  let failed = 0;
  for (const [name, ok] of cases) {
    console.log(`${ok ? "ok  " : "FAIL"} ${name}`);
    if (!ok) failed++;
  }
  console.log(failed ? `${failed} self-test case(s) failed` : `${cases.length} self-test cases OK`);
  return failed ? 1 : 0;
}

function main(argv) {
  if (argv.includes("--self-test")) return selfTest();
  let dist = DEFAULT_DIST;
  let src;
  let maxGzipBytes = null;
  let json = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--max-gzip-bytes") {
      maxGzipBytes = Number(argv[++i]);
      if (!Number.isSafeInteger(maxGzipBytes) || maxGzipBytes <= 0) {
        console.error("--max-gzip-bytes needs a positive integer");
        return 2;
      }
    } else if (a === "--src") {
      if (!argv[i + 1]) {
        console.error("--src needs a directory");
        return 2;
      }
      src = path.resolve(argv[++i]);
    } else if (a === "--json") json = true;
    else if (a.startsWith("--")) {
      console.error(`unknown option ${a}`);
      return 2;
    } else dist = path.resolve(a);
  }
  // the source cross-check is meaningful only against a build of that source
  if (src === undefined) src = path.resolve(dist) === DEFAULT_DIST && existsSync(DEFAULT_SRC) ? DEFAULT_SRC : null;
  else if (!existsSync(src)) {
    console.error(`check-landing-bundle: --src ${src} does not exist`);
    return 2;
  }
  let result;
  try {
    result = checkLanding(dist, { maxGzipBytes, src });
  } catch (err) {
    console.error(`check-landing-bundle: ${err.message}`);
    return err.code === 2 ? 2 : 1;
  }
  if (json) console.log(JSON.stringify(result, null, 2));
  else report(dist, result);
  if (result.problems.length) {
    for (const p of result.problems) console.error(`FAIL ${p}`);
    console.error("The public site must not load the charting library: keep components/charts.tsx out of public pages (lazy-load chart pages).");
    return 1;
  }
  console.log(`OK: no Recharts code in the landing JavaScript or in the lazily loaded public routes${src ? " (source cross-checked)" : ""}.`);
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = main(process.argv.slice(2));
}
