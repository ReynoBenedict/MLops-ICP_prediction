/**
 * Lazy UI strings and the entry bundle: a regression guard (i18n/dictionary.ts, App.tsx).
 *
 * Workspace strings (i18n/workspace.ts) and operator-console strings (i18n/ops.ts) are registered when
 * the lazily loaded chunks that import them run. A module that renders before that must not use their
 * keys: the reader would see a fallback word instead of the text. This test reads the source the way
 * the bundler does: the static import graph from `src/main.tsx` (`import()` starts a new chunk, type-only
 * imports are dropped), and for the entry and every lazily imported module the keys that its chunk can
 * use (dictionaries in the entry closure plus those in its own closure). In every module of a closure it
 * collects literal keys (any string literal equal to a key, so keys kept in tables count too) and the
 * static prefixes of dynamic keys (`td("status", …)`, `` t(`verify.${basis}`) ``, `"acct.data.past." + status`);
 * a prefix needs every key that starts with it. It also keeps the signed-in shell, the auth pages, the lazy dictionaries and
 * the area stylesheets out of the entry closure.
 */
import ts from "typescript";
import { describe, expect, it } from "vitest";

/** Application sources (tests excluded), keyed "/src/…". */
const SOURCES = import.meta.glob<string>(["/src/**/*.{ts,tsx}", "!/src/**/*.test.{ts,tsx}", "!/src/test/**"], {
  query: "?raw", import: "default", eager: true,
});
const ENTRY = "/src/main.tsx";
/** A string literal that ends in a dot and looks like the start of a key ("acct.data.past."). */
const KEY_PREFIX = /^[a-z]\w*(?:\.\w+)*\.$/;

interface ModuleFacts {
  /** Static imports and re-exports of other source modules (resolved), plus raw specifiers of side-effect imports. */
  deps: string[];
  rawImports: string[];
  /** `import("…")` targets (resolved). */
  lazy: string[];
  literals: Set<string>;
  prefixes: Set<string>;
  /** Keys this module defines (dictionary modules in src/i18n/: `"key": [id, en]`). */
  defines: string[];
  registers: boolean;
}

function resolve(sources: Record<string, string>, from: string, spec: string): string | null {
  if (!spec.startsWith(".")) return null;
  const parts = from.split("/").slice(0, -1);
  for (const seg of spec.split("/")) {
    if (seg === "..") parts.pop();
    else if (seg !== ".") parts.push(seg);
  }
  const base = parts.join("/");
  for (const ext of ["", ".ts", ".tsx", "/index.ts", "/index.tsx"]) if (sources[base + ext] !== undefined) return base + ext;
  return null;
}

function analyse(sources: Record<string, string>, file: string): ModuleFacts {
  const text = sources[file]!;
  const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, file.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS);
  const dictionary = file.startsWith("/src/i18n/");
  const facts: ModuleFacts = { deps: [], rawImports: [], lazy: [], literals: new Set(), prefixes: new Set(), defines: [], registers: false };
  const addDep = (spec: string) => {
    facts.rawImports.push(spec);
    const target = resolve(sources, file, spec);
    if (target) facts.deps.push(target);
  };
  const visit = (node: ts.Node): void => {
    if (ts.isImportDeclaration(node)) {
      const clause = node.importClause;
      const named = clause?.namedBindings;
      const typeOnly = !!clause && (clause.isTypeOnly
        || (!clause.name && !!named && ts.isNamedImports(named) && named.elements.length > 0 && named.elements.every((e) => e.isTypeOnly)));
      if (!typeOnly && ts.isStringLiteral(node.moduleSpecifier)) addDep(node.moduleSpecifier.text);
      return;
    }
    if (ts.isExportDeclaration(node)) {
      if (!node.isTypeOnly && node.moduleSpecifier && ts.isStringLiteral(node.moduleSpecifier)) addDep(node.moduleSpecifier.text);
      return;
    }
    if (ts.isCallExpression(node)) {
      const arg0 = node.arguments[0];
      if (node.expression.kind === ts.SyntaxKind.ImportKeyword) {
        const target = arg0 && ts.isStringLiteralLike(arg0) ? resolve(sources, file, arg0.text) : null;
        if (target) facts.lazy.push(target);
        return;
      }
      const callee = node.expression;
      const name = ts.isIdentifier(callee) ? callee.text : ts.isPropertyAccessExpression(callee) ? callee.name.text : "";
      if (name === "registerStrings") facts.registers = true;
      const prefixArg = name === "td" ? arg0 : name === "translateDynamic" ? node.arguments[1] : undefined;
      if (prefixArg && ts.isStringLiteralLike(prefixArg)) facts.prefixes.add(`${prefixArg.text}.`);
    }
    if (dictionary && ts.isPropertyAssignment(node) && ts.isStringLiteral(node.name) && ts.isArrayLiteralExpression(node.initializer)) {
      facts.defines.push(node.name.text);
      ts.forEachChild(node.initializer, visit);
      return;
    }
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
      facts.literals.add(node.text);
      // a key built by concatenation: `("acct.data.past." + status) as Key`
      if (KEY_PREFIX.test(node.text)) facts.prefixes.add(node.text);
    }
    if (ts.isTemplateExpression(node) && node.head.text.includes(".")) facts.prefixes.add(node.head.text);
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return facts;
}

function closure(facts: Map<string, ModuleFacts>, root: string): Set<string> {
  const seen = new Set<string>();
  const queue = [root];
  while (queue.length) {
    const file = queue.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    queue.push(...(facts.get(file)?.deps ?? []));
  }
  return seen;
}

/** Every key used where its dictionary is not loaded: "<chunk root> > <module>: <key> (<dictionary>)". */
function lazyKeyViolations(sources: Record<string, string>, entry: string) {
  const facts = new Map(Object.keys(sources).map((f) => [f, analyse(sources, f)]));
  const owner = new Map<string, string>();
  for (const [file, f] of facts) for (const key of f.defines) owner.set(key, file);
  const allKeys = [...owner.keys()];
  const keysOf = (files: Iterable<string>) => {
    const out = new Set<string>();
    for (const file of files) for (const key of facts.get(file)?.defines ?? []) out.add(key);
    return out;
  };
  const eager = closure(facts, entry);
  const eagerKeys = keysOf(eager);
  // the entry plus every lazily imported module reachable from it (through other lazy modules too)
  const roots: string[] = [entry];
  for (let i = 0; i < roots.length; i++) {
    for (const file of closure(facts, roots[i]!)) for (const target of facts.get(file)!.lazy) if (!roots.includes(target)) roots.push(target);
  }
  const violations: string[] = [];
  for (const root of roots) {
    const members = closure(facts, root);
    const available = new Set([...eagerKeys, ...keysOf(members)]);
    for (const file of members) {
      const f = facts.get(file)!;
      if (f.defines.length) continue; // a dictionary names its own keys
      const missing = new Set<string>();
      for (const lit of f.literals) if (owner.has(lit) && !available.has(lit)) missing.add(lit);
      for (const prefix of f.prefixes) for (const key of allKeys) if (key.startsWith(prefix) && !available.has(key)) missing.add(`${prefix}* (${key})`);
      for (const key of missing) {
        const plain = key.split(" ")[0]!.replace(/\*$/, "");
        const dict = owner.get(plain) ?? owner.get(allKeys.find((k) => k.startsWith(plain)) ?? "") ?? "?";
        violations.push(`${root} > ${file}: ${key} (${dict})`);
      }
    }
  }
  return { violations, eager, roots, registering: [...facts].filter(([, f]) => f.registers).map(([file]) => file), facts };
}

describe("lazy UI strings and the entry bundle", () => {
  const result = lazyKeyViolations(SOURCES, ENTRY);

  it("finds the application graph", () => {
    expect(SOURCES[ENTRY]).toBeTruthy();
    expect(result.eager.has("/src/App.tsx")).toBe(true);
    expect(result.eager.has("/src/pages/LandingPage.tsx")).toBe(true);
    expect(result.roots).toEqual(expect.arrayContaining(["/src/pages/OverviewPage.tsx", "/src/pages/ops/HealthPage.tsx", "/src/components/layout.tsx", "/src/pages/AuthPages.tsx"]));
    expect(result.registering.sort()).toEqual(["/src/i18n/ops.ts", "/src/i18n/workspace.ts"]);
  });

  it("uses no key before the dictionary that defines it is loaded", () => {
    expect(result.violations).toEqual([]);
  });

  it("keeps the shell, the auth pages, the lazy dictionaries and the area stylesheets out of the entry bundle", () => {
    const eager = [...result.eager];
    const lazyOnly = ["/src/components/layout.tsx", "/src/components/data-status.tsx", "/src/pages/AuthPages.tsx", ...result.registering];
    for (const file of result.registering) lazyOnly.push(...result.facts.get(file)!.deps.filter((d) => result.facts.get(d)!.defines.length > 0));
    expect(eager.filter((f) => lazyOnly.includes(f))).toEqual([]);
    const areaCss = eager.flatMap((f) => result.facts.get(f)!.rawImports.filter((s) => /styles\/(shell|market|flows|ops)\.css$/.test(s)).map((s) => `${f}: ${s}`));
    expect(areaCss).toEqual([]);
  });

  it("detects a key used ahead of its lazy dictionary (checker self-test)", () => {
    const fake = {
      "/src/main.tsx": 'import { App } from "./App"; import { Footer } from "./Footer"; import "./i18n/dictionary";',
      "/src/App.tsx": 'import { t } from "./i18n/index"; const Page = () => import("./Page"); export const App = () => [t("core.title"), t("ws.title"), td("ws", x), Page];',
      "/src/Footer.tsx": 'import { t } from "./i18n/index"; export const Footer = (s: string) => t("ws.sub." + s);',
      "/src/Page.tsx": 'import "./i18n/workspace"; import { t } from "./i18n/index"; export default () => [t("ws.title"), t(`ws.${x}`), t("ops.only")];',
      "/src/i18n/index.ts": "export const t = (k: string) => k; export function registerStrings(s: object) { return s; }",
      "/src/i18n/dictionary.ts": 'import type { WS } from "./workspace"; export const CORE = { "core.title": ["a", "b"] };',
      "/src/i18n/workspace.ts": 'import { registerStrings } from "./index"; export const WS = { "ws.title": ["a", "b"], "ws.body": ["a", "b"], "ws.sub.x": ["a", "b"] }; registerStrings(WS);',
      "/src/i18n/ops.ts": 'import { registerStrings } from "./index"; export const OPS = { "ops.only": ["a", "b"] }; registerStrings(OPS);',
    };
    const r = lazyKeyViolations(fake, "/src/main.tsx");
    expect(r.violations.sort()).toEqual([
      "/src/Page.tsx > /src/Page.tsx: ops.only (/src/i18n/ops.ts)",
      "/src/main.tsx > /src/App.tsx: ws.* (ws.body) (/src/i18n/workspace.ts)",
      "/src/main.tsx > /src/App.tsx: ws.* (ws.sub.x) (/src/i18n/workspace.ts)",
      "/src/main.tsx > /src/App.tsx: ws.* (ws.title) (/src/i18n/workspace.ts)",
      "/src/main.tsx > /src/App.tsx: ws.title (/src/i18n/workspace.ts)",
      "/src/main.tsx > /src/Footer.tsx: ws.sub.* (ws.sub.x) (/src/i18n/workspace.ts)",
    ]);
    expect(r.eager.has("/src/i18n/workspace.ts")).toBe(false);
  });
});
