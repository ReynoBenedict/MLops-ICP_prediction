#!/usr/bin/env node
// Asset licence guard (`npm run asset-licences`: part of `npm run check` and of the CI workflow,
// .github/workflows/ci.yml, step "Asset licence guard"). Every image the web app serves
// must be listed in the licence register of docs/ASSET_INVENTORY.md with a licence that needs no
// attribution, so the public site never ships a picture whose credit it does not show. Checks:
//  1. every image file under apps/web/public/ (any folder; any extension in IMAGE_EXT below, such as
//     .svg .png .jpg .webp .avif .gif .ico, and also .apng .jfif .svgz .jxl .heic) has exactly one row in
//     the table under the heading "## Licence register";
//  2. every row names an existing file under apps/web/public/ (no stale rows);
//  3. the licence is one of: original, CC0, public domain, Unsplash License, Pexels License (any other
//     licence, such as CC BY or CC BY-SA, needs a visible credit and is refused here);
//  4. every row gives the SHA-256 of the file as shipped and that checksum matches the file, so no image
//     (original or third-party) can be replaced or relabelled without a deliberate register edit (an
//     SVG is hashed with CRLF line endings read as LF: Git may convert a text file's line endings on a
//     Windows checkout, and the checksum must be the same there and in CI);
//  5. a third-party file (any licence but "original") links its source page (http or https), and an
//     "original" file cites no external source page (a third-party file cannot pass as original by
//     changing only its licence cell);
//  6. no image reaches the bundle another way: no image file inside apps/web/src (Vite would publish it
//     under a hashed name the register cannot list), no `data:image/` URI, and no import, `new URL()` or
//     CSS `url()` of an image that is not a root-relative public path (apps/web/src, tests excluded, and
//     apps/web/index.html).
// Fonts are not images and are listed in their own inventory section (SIL OFL); they are not checked here.
//
// Usage:  node scripts/check-asset-licences.mjs [--public dir] [--src dir] [--html file] [--inventory file] [--json]
//         node scripts/check-asset-licences.mjs --self-test
// Defaults: apps/web/public, apps/web/src, apps/web/index.html and docs/ASSET_INVENTORY.md of this
// repository. A failure prints the actual SHA-256 of a changed file, for the register row. Exit codes:
// 0 ok, 1 guard failed, 2 bad input (missing folder or inventory).
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_PUBLIC = path.join(ROOT, "apps", "web", "public");
const DEFAULT_SRC = path.join(ROOT, "apps", "web", "src");
const DEFAULT_HTML = path.join(ROOT, "apps", "web", "index.html");
const DEFAULT_INVENTORY = path.join(ROOT, "docs", "ASSET_INVENTORY.md");
/** Every image format a browser may display (including the JPEG, SVG and animated-PNG aliases). */
export const IMAGE_EXTENSIONS = [
  "svg", "svgz", "png", "apng", "jpg", "jpeg", "jpe", "jfif", "pjpeg", "pjp", "webp", "avif", "gif",
  "ico", "cur", "bmp", "tif", "tiff", "jxl", "heic", "heif",
];
const IMAGE_EXT = new Set(IMAGE_EXTENSIONS.map((e) => `.${e}`));
const IMG = `(?:${IMAGE_EXTENSIONS.join("|")})`;
/** Licences that require no attribution, in the spelling the register uses (compared case-insensitively). */
export const ALLOWED_LICENCES = ["original", "CC0", "public domain", "Unsplash License", "Pexels License"];
const ALLOWED = new Set(ALLOWED_LICENCES.map((l) => l.toLowerCase()));
const HEADING = /^##\s+Licence register\s*$/;
/** Source files whose image references are checked (tests and test helpers are not shipped). */
const CODE_EXT = new Set([".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".css", ".html"]);
const isTestFile = (rel) => /\.test\.[cm]?[jt]sx?$/.test(rel) || /(^|\/)test\//.test(rel);
/** Ways an image enters the bundle without being a file under public/: imports, new URL(), CSS url(). */
const IMAGE_REFS = [
  new RegExp(String.raw`\bimport\s+(?:[\w$*{}\s,]+?\s+from\s+)?["']([^"'\n]+\.${IMG}(?:\?[^"'\n]*)?)["']`, "gi"),
  new RegExp(String.raw`\bimport\s*\(\s*["']([^"'\n]+\.${IMG}(?:\?[^"'\n]*)?)["']\s*\)`, "gi"),
  new RegExp(String.raw`\bnew\s+URL\s*\(\s*["']([^"'\n]+\.${IMG}(?:\?[^"'\n]*)?)["']`, "gi"),
  new RegExp(String.raw`\burl\(\s*["']?([^"')\s]+\.${IMG}(?:[?#][^"')\s]*)?)["']?\s*\)`, "gi"),
];
/** A root-relative public path such as /img/photo/a.avif (not a protocol-relative //host/a.png). */
const isPublicPath = (ref) => ref.startsWith("/") && !ref.startsWith("//");

const slashes = (p) => p.split(path.sep).join("/");
const norm = (s) => s.trim().replace(/\s+/g, " ").toLowerCase();
const sha256 = (buf) => createHash("sha256").update(buf).digest("hex");
/** Image formats that are text, so Git may convert their line endings on checkout (core.autocrlf). */
const TEXT_IMAGE_EXT = new Set([".svg"]);
/**
 * SHA-256 of an image file as the register records it: the bytes as they are, except that an SVG's
 * CRLF line endings count as LF, so a Windows checkout with core.autocrlf and the CI checkout (LF) give
 * the same value. (latin1 maps every byte to one character and back, so nothing else changes.)
 */
export function fileSha256(file) {
  const buf = readFileSync(file);
  if (!TEXT_IMAGE_EXT.has(path.extname(file).toLowerCase())) return sha256(buf);
  return sha256(Buffer.from(buf.toString("latin1").replace(/\r\n/g, "\n"), "latin1"));
}
const lineOf = (text, index) => text.slice(0, index).split("\n").length;

class InputError extends Error {
  constructor(message) { super(message); this.code = 2; }
}

/** Files under `dir` (recursively), as paths relative to `root` with forward slashes. */
function walk(dir, root) {
  const out = [];
  const visit = (d) => {
    for (const entry of readdirSync(d, { withFileTypes: true })) {
      const full = path.join(d, entry.name);
      if (entry.isDirectory()) visit(full);
      else out.push(slashes(path.relative(root, full)));
    }
  };
  visit(dir);
  return out.sort();
}

const isImage = (rel) => IMAGE_EXT.has(path.extname(rel).toLowerCase());

/** Rows of the first table after the "## Licence register" heading: { line, file, licence, source, url, sha }. */
export function parseRegister(markdown) {
  const lines = markdown.split(/\r?\n/);
  const start = lines.findIndex((l) => HEADING.test(l));
  if (start < 0) return null;
  const rows = [];
  let inTable = false;
  for (let i = start + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (/^#{1,2}\s/.test(line)) break; // the next section
    if (!line.startsWith("|")) {
      if (inTable) break;
      continue;
    }
    inTable = true;
    const cells = line.replace(/^\|/, "").replace(/\|$/, "").split("|").map((c) => c.trim());
    if (cells.every((c) => /^:?-{3,}:?$/.test(c))) continue; // separator
    if (/^file$/i.test(cells[0] ?? "")) continue; // header
    const file = (cells[0] ?? "").replace(/^`|`$/g, "").trim();
    const source = cells[2] ?? "";
    rows.push({
      line: i + 1,
      file,
      licence: cells[1] ?? "",
      source,
      url: (/https?:\/\/[^\s)>\]]+/.exec(source) ?? [null])[0],
      sha: (cells[3] ?? "").replace(/`/g, "").trim(),
    });
  }
  return rows;
}

/** Images that would be bundled from the source tree or inlined, instead of served from public/. */
function bundledImages(srcDir, htmlFile, root) {
  const problems = [];
  const files = [];
  if (srcDir) {
    for (const rel of walk(srcDir, root)) {
      const inSrc = slashes(path.relative(srcDir, path.join(root, rel)));
      if (isImage(rel)) {
        problems.push(`${rel} is an image inside the source tree: Vite would publish it under a hashed name the licence register cannot list; move it to apps/web/public/ and give it a register row`);
      } else if (CODE_EXT.has(path.extname(rel).toLowerCase()) && !isTestFile(inSrc)) files.push(rel);
    }
  }
  if (htmlFile) files.push(slashes(path.relative(root, htmlFile)));
  for (const rel of files) {
    const text = readFileSync(path.join(root, ...rel.split("/")), "utf8");
    for (const m of text.matchAll(/data:image\//gi)) {
      problems.push(`${rel}:${lineOf(text, m.index)}: a data:image URI inlines an image that the licence register cannot list; serve it as a file under apps/web/public/`);
    }
    for (const re of IMAGE_REFS) {
      for (const m of text.matchAll(re)) {
        const ref = m[1];
        if (isPublicPath(ref)) continue; // a public file: covered by the register check above
        problems.push(`${rel}:${lineOf(text, m.index)}: ${ref} would be bundled or fetched from elsewhere; reference an image under apps/web/public/ by its root-relative path (e.g. /img/...)`);
      }
    }
  }
  return problems;
}

/**
 * Compare the image files under `publicDir` with the register in `inventory`, and look for images that
 * would reach the bundle from `srcDir` or `htmlFile` (optional). Paths in the register are relative to
 * `root` (the repository), e.g. `apps/web/public/img/photo/a.webp`.
 */
export function checkAssets({ root = ROOT, publicDir = DEFAULT_PUBLIC, srcDir = DEFAULT_SRC, htmlFile = DEFAULT_HTML, inventory = DEFAULT_INVENTORY } = {}) {
  if (!existsSync(publicDir) || !statSync(publicDir).isDirectory()) throw new InputError(`no public folder at ${publicDir}`);
  if (srcDir && (!existsSync(srcDir) || !statSync(srcDir).isDirectory())) throw new InputError(`no source folder at ${srcDir}`);
  if (htmlFile && !existsSync(htmlFile)) throw new InputError(`no HTML entry at ${htmlFile}`);
  if (!existsSync(inventory)) throw new InputError(`no asset inventory at ${inventory}`);
  const files = walk(publicDir, root).filter(isImage);
  const rows = parseRegister(readFileSync(inventory, "utf8"));
  const problems = [];
  const inv = slashes(path.relative(root, inventory));
  if (rows === null) problems.push(`${inv} has no "## Licence register" section`);
  const publicPrefix = `${slashes(path.relative(root, publicDir))}/`;
  const seen = new Map();
  for (const row of rows ?? []) {
    const where = `${inv}:${row.line}`;
    if (!row.file.startsWith(publicPrefix)) {
      problems.push(`${where}: ${row.file || "(empty)"} is not a file under ${publicPrefix}`);
      continue;
    }
    if (seen.has(row.file)) problems.push(`${where}: ${row.file} is listed twice (also line ${seen.get(row.file)})`);
    seen.set(row.file, row.line);
    const full = path.join(root, ...row.file.split("/"));
    if (!existsSync(full)) {
      problems.push(`${where}: ${row.file} does not exist (remove the row or restore the file)`);
      continue;
    }
    const licence = norm(row.licence);
    if (!ALLOWED.has(licence)) {
      problems.push(`${where}: ${row.file} has licence "${row.licence}"; allowed without a visible credit: ${ALLOWED_LICENCES.join(", ")}`);
      continue;
    }
    if (licence === "original" && row.url) {
      problems.push(`${where}: ${row.file} is marked original but cites ${row.url}; a third-party file keeps its own licence and source page`);
    }
    if (licence !== "original" && !row.url) problems.push(`${where}: ${row.file} (${row.licence}) needs its source page as an http(s) link`);
    const actual = fileSha256(full);
    if (!/^[0-9a-f]{64}$/i.test(row.sha)) problems.push(`${where}: ${row.file} needs the SHA-256 of the shipped file (it is ${actual})`);
    else if (actual !== row.sha.toLowerCase()) problems.push(`${where}: ${row.file} changed: SHA-256 is ${actual}, the register says ${row.sha}`);
  }
  for (const file of files) {
    if (rows && !seen.has(file)) problems.push(`${file} is not in the licence register of ${inv}`);
  }
  problems.push(...bundledImages(srcDir, htmlFile, root));
  return { files, rows: rows ?? [], problems };
}

function selfTest() {
  const dir = mkdtempSync(path.join(os.tmpdir(), "asset-licences-"));
  const cases = [];
  const setup = (name, files, register, src = {}) => {
    const root = path.join(dir, name);
    const web = path.join(root, "apps", "web");
    const all = { "index.html": '<link rel="icon" href="/favicon.svg" />', "src/main.tsx": 'const a = "/img/a.svg";', ...src };
    for (const [rel, content] of [...Object.entries(files).map(([r, c]) => [`public/${r}`, c]), ...Object.entries(all)]) {
      const file = path.join(web, rel);
      mkdirSync(path.dirname(file), { recursive: true });
      writeFileSync(file, content);
    }
    mkdirSync(path.join(root, "docs"), { recursive: true });
    const inventory = path.join(root, "docs", "ASSET_INVENTORY.md");
    writeFileSync(inventory, register === null ? "# Asset inventory\n" : `# Asset inventory\n\n## Licence register\n\nChecked by a script.\n\n| File | Licence | Source | SHA-256 |\n|---|---|---|---|\n${register}\n\n## Fonts\n\n| File | Licence |\n|---|---|\n| \`x.woff2\` | OFL |\n`);
    return checkAssets({ root, publicDir: path.join(web, "public"), srcDir: path.join(web, "src"), htmlFile: path.join(web, "index.html"), inventory });
  };
  const svg = "<svg/>";
  const photo = "photo-bytes";
  const files = { "img/a.svg": svg, "img/photo/b.webp": photo, "fonts/f.woff2": "font", "_headers": "x" };
  const commons = "[Commons](https://commons.wikimedia.org/wiki/File:B.jpg)";
  const rowA = `| \`apps/web/public/img/a.svg\` | original | this project | ${sha256(svg)} |`;
  const rowB = `| \`apps/web/public/img/photo/b.webp\` | CC0 | ${commons} | ${sha256(photo)} |`;
  const ok = `${rowA}\n${rowB}`;
  const one = (r, text) => r.problems.length === 1 && r.problems[0].includes(text);
  try {
    let r = setup("ok", files, ok, { "src/app.css": '.x { background: url("/img/a.svg"); }' });
    cases.push(["listed files with allowed licences and matching checksums pass; fonts and other files are not images", r.problems.length === 0 && r.files.length === 2]);
    r = setup("unlisted", { ...files, "brand/c.png": "png" }, ok);
    cases.push(["an image without a register row fails", one(r, "brand/c.png")]);
    r = setup("formats", { ...files, "img/d.jxl": "1", "img/e.heic": "2", "img/f.apng": "3", "img/g.svgz": "4", "img/h.JFIF": "5", "img/i.heif": "6" }, ok);
    cases.push(["unlisted .jxl, .heic, .heif, .apng, .svgz and .jfif files fail too", r.problems.length === 6]);
    r = setup("stale", files, `${ok}\n| \`apps/web/public/img/gone.svg\` | original | this project | ${sha256("x")} |`);
    cases.push(["a row for a missing file fails", one(r, "does not exist")]);
    r = setup("ccby", files, ok.replace("| CC0 |", "| CC BY 4.0 |"));
    cases.push(["a licence that needs a credit fails", one(r, "CC BY 4.0")]);
    r = setup("nosource", files, ok.replace(commons, "Commons"));
    cases.push(["a third-party file without a source link fails", one(r, "source page")]);
    r = setup("changed", files, ok.replace(sha256(photo), sha256("other")));
    cases.push(["a third-party file whose checksum differs fails", one(r, "changed")]);
    r = setup("nosha", files, ok.replace(sha256(photo), "-"));
    cases.push(["a third-party file without a checksum fails", one(r, "SHA-256")]);
    r = setup("original-nosha", files, ok.replace(sha256(svg), "-"));
    cases.push(["an original file without a checksum fails", one(r, "img/a.svg needs the SHA-256")]);
    r = setup("original-swapped", { ...files, "img/a.svg": "<svg><path/></svg>" }, ok);
    cases.push(["an original file replaced by other content fails", one(r, "img/a.svg changed")]);
    r = setup("crlf-svg", { ...files, "img/a.svg": "<svg>\r\n<path/>\r\n</svg>\r\n" }, ok.replace(sha256(svg), sha256("<svg>\n<path/>\n</svg>\n")));
    cases.push(["an SVG checked out with CRLF line endings (Windows, core.autocrlf) keeps the checksum of its LF form", r.problems.length === 0]);
    r = setup("crlf-binary", { ...files, "img/photo/b.webp": "x\r\ny" }, ok.replace(sha256(photo), sha256("x\ny")));
    cases.push(["only SVG text is normalised: a binary image is hashed byte for byte", one(r, "b.webp changed")]);
    r = setup("relabelled", files, ok.replace("| CC0 |", "| original |"));
    cases.push(["a third-party file relabelled original (still citing its source page) fails", one(r, "marked original")]);
    r = setup("twice", files, `${ok}\n${rowA}`);
    cases.push(["a file listed twice fails", one(r, "listed twice")]);
    r = setup("outside", files, `${ok}\n| \`docs/assets/photos/x.jpg\` | CC BY 4.0 | [x](https://example.org) | - |`);
    cases.push(["a row outside the public folder fails", one(r, "not a file under")]);
    r = setup("spelling", files, ok.replace("| original |", "| Original |").replace("| CC0 |", "| public  domain |"));
    cases.push(["licence names compare case- and space-insensitively", r.problems.length === 0]);
    r = setup("noregister", files, null);
    cases.push(["an inventory without the register fails", one(r, "Licence register")]);
    r = setup("src-file", files, ok, { "src/assets/logo.png": "png" });
    cases.push(["an image file inside the source tree fails", one(r, "src/assets/logo.png is an image inside the source tree")]);
    r = setup("src-import", files, ok, { "src/hero.tsx": 'import hero from "./hero.webp";\nimport icon from "pkg/icon.svg?url";\nconst u = new URL("./x.avif", import.meta.url);' });
    cases.push(["image imports, package image imports and new URL() images fail", r.problems.length === 3 && r.problems.every((p) => p.includes("would be bundled"))]);
    r = setup("src-css", files, ok, { "src/a.css": ".a { background: url(../img/b.jpg); }\n.b { background-image: url('https://example.org/c.png'); }" });
    cases.push(["a relative or external CSS url() image fails", r.problems.length === 2]);
    r = setup("data-uri", files, ok, { "src/b.css": '.c { mask: url("data:image/svg+xml,<svg/>"); }', "index.html": '<link rel="icon" href="data:image/png;base64,AAAA" />' });
    cases.push(["a data:image URI in a stylesheet or index.html fails", r.problems.length === 2 && r.problems.every((p) => p.includes("data:image"))]);
    r = setup("tests", files, ok, { "src/x.test.tsx": 'import y from "./y.png";', "src/test/setup.ts": 'const z = "data:image/png;base64,AAAA";' });
    cases.push(["tests and test helpers are not shipped and are not checked", r.problems.length === 0]);
    let threw = false;
    try { checkAssets({ root: dir, publicDir: path.join(dir, "missing"), inventory: path.join(dir, "missing.md") }); } catch (err) { threw = err.code === 2; }
    cases.push(["a missing public folder is an input error", threw]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  let failed = 0;
  for (const [name, passed] of cases) {
    console.log(`${passed ? "ok  " : "FAIL"} ${name}`);
    if (!passed) failed++;
  }
  console.log(failed ? `${failed} self-test case(s) failed` : `${cases.length} self-test cases OK`);
  return failed ? 1 : 0;
}

function main(argv) {
  if (argv.includes("--self-test")) return selfTest();
  const paths = { "--public": DEFAULT_PUBLIC, "--src": DEFAULT_SRC, "--html": DEFAULT_HTML, "--inventory": DEFAULT_INVENTORY };
  let json = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a in paths) {
      if (!argv[i + 1]) {
        console.error(`${a} needs a path`);
        return 2;
      }
      paths[a] = path.resolve(argv[++i]);
    } else if (a === "--json") json = true;
    else {
      console.error(`unknown argument ${a}`);
      return 2;
    }
  }
  let result;
  try {
    result = checkAssets({ publicDir: paths["--public"], srcDir: paths["--src"], htmlFile: paths["--html"], inventory: paths["--inventory"] });
  } catch (err) {
    console.error(`check-asset-licences: ${err.message}`);
    return err.code === 2 ? 2 : 1;
  }
  if (json) console.log(JSON.stringify(result, null, 2));
  if (result.problems.length) {
    for (const p of result.problems) console.error(`FAIL ${p}`);
    console.error(`Every image the web app serves is a file under apps/web/public/ with a row in the licence register of docs/ASSET_INVENTORY.md (its SHA-256 and one of: ${ALLOWED_LICENCES.join(", ")}).`);
    return 1;
  }
  const thirdParty = result.rows.filter((r) => norm(r.licence) !== "original").length;
  if (!json) console.log(`OK: ${result.files.length} image files under apps/web/public, each in the licence register (${result.files.length - thirdParty} original, ${thirdParty} third-party with a licence that needs no credit), every checksum matches; no image is bundled from apps/web/src or inlined.`);
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = main(process.argv.slice(2));
}
