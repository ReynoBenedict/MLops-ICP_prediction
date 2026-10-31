import react from "@vitejs/plugin-react";
import type { Plugin } from "vite";
import { defineConfig } from "vitest/config";

// In development the Vite server proxies /api to the FastAPI backend, so the browser talks to one
// origin exactly as it does in production (where the Cloudflare Worker plays this role).
const env = (globalThis as { process?: { env: Record<string, string | undefined> } }).process?.env ?? {};
const backend = env.VITE_DEV_BACKEND ?? "http://127.0.0.1:8000";

/**
 * Chunking of the signed-in shell (App.tsx loads components/layout.tsx lazily). Left to automatic
 * splitting, every icon, helper and string module that the shell shares with a different set of
 * workspace pages becomes a chunk of its own (about twenty requests of a few hundred bytes each before
 * the first workspace page can render). The "layout" group keeps them in one chunk with the shell: the
 * shell, the data status, the workspace strings, the small `lib/` helpers and the lucide icons, but only
 * modules that neither the entry nor a public page (methodology, legal, sign-in and registration)
 * imports statically, so the public site never downloads the shell. `includeDependenciesRecursively:
 * false` leaves their dependencies where they are (the entry bundle). `checkShellChunk` fails the build
 * if a public chunk would load the shell chunk or the shell chunk imports itself through another chunk.
 */
const SHELL_MEMBERS = /\/src\/(components\/(layout|data-status)\.tsx|lib\/[\w-]+\.tsx?|i18n\/(workspace|market|forecast|scenario|insights|reports|account)\.ts)$|\/node_modules\/lucide-react\/dist\/esm\/icons\//;
const PUBLIC_ROOTS = /\/src\/(main\.tsx|pages\/(MethodologyPage|LegalPages|AuthPages)\.tsx)$/;
const ICON = /\/node_modules\/lucide-react\/dist\/esm\/icons\/([\w-]+)\.m?js$/;
const LUCIDE_INDEX = /\/node_modules\/lucide-react\/dist\/esm\/lucide-react\.m?js$/;
const SHELL_ROOTS = /\/src\/(components\/layout\.tsx|i18n\/workspace\.ts)$/;
const slashes = (id: string) => id.replaceAll("\\", "/");

interface ModuleGraph { getModuleInfo(id: string): { importers: readonly string[]; code: string | null } | null }

function shellChunk() {
  const reached = new Map<string, boolean>();
  /** True when a chain of static imports leads from the entry or a public page to `id`. */
  const reachedByPublic = (id: string, graph: ModuleGraph): boolean => {
    const known = reached.get(id);
    if (known !== undefined) return known;
    const seen = new Set([id]);
    const queue = [id];
    let found = false;
    while (queue.length && !found) {
      const current = queue.pop()!;
      if (PUBLIC_ROOTS.test(slashes(current))) found = true;
      for (const importer of graph.getModuleInfo(current)?.importers ?? []) {
        if (!seen.has(importer)) { seen.add(importer); queue.push(importer); }
      }
    }
    reached.set(id, found);
    return found;
  };

  /**
   * Icons are imported through the lucide-react index module, which every module that uses an icon
   * imports, so the import chain does not tell which icons public code uses. This reads it from the
   * named imports of the public modules instead. When it cannot (the reason is returned as a string),
   * the group is switched off and the build falls back to automatic chunks: more requests, never wrong.
   * Grouping without the icons is not an option: an icon that only the shell uses would stay in the
   * chunk of components/layout.tsx, which would then import the group chunk that imports it.
   */
  const iconsUsedByPublic = (indexId: string | undefined, graph: ModuleGraph): Set<string> | string => {
    if (!indexId) return "the lucide-react index module was not seen";
    const index = graph.getModuleInfo(indexId);
    if (!index?.code) return "the lucide-react index module has no code";
    const fileOf = new Map<string, string>();
    for (const m of index.code.matchAll(/export\s*\{([^}]*)\}\s*from\s*["']\.\/icons\/([\w-]+)\.m?js["']/g)) {
      for (const spec of m[1]!.split(",")) fileOf.set(spec.trim().split(/\s+as\s+/).pop()!, m[2]!);
    }
    if (!fileOf.size) return "no icon re-exports found in the lucide-react index module";
    const used = new Set<string>();
    for (const importer of index.importers) {
      if (!reachedByPublic(importer, graph)) continue;
      const code = graph.getModuleInfo(importer)?.code ?? "";
      const imports = [...code.matchAll(/import\s*\{([^}]*)\}\s*from\s*["']lucide-react["']/g)];
      if (!imports.length) return `no named lucide-react import found in ${importer}`;
      for (const m of imports) {
        for (const spec of m[1]!.split(",")) {
          const name = spec.trim().split(/\s+as\s+/)[0];
          if (!name) continue;
          const file = fileOf.get(name);
          if (!file) return `unknown icon ${name} in ${importer}`;
          used.add(file);
        }
      }
    }
    return used;
  };

  let anIcon: string | undefined;
  let publicIcons: Set<string> | null | undefined; // null: the group is off
  const test = (id: string): boolean => {
    const path = slashes(id);
    if (ICON.test(path)) anIcon ??= id;
    return SHELL_MEMBERS.test(path);
  };
  // Rolldown calls `test` for every candidate module before the first `name` call. The index module is no
  // candidate (it only re-exports), so it is found through the importers of an icon.
  const name = (id: string, graph: ModuleGraph): string | null => {
    if (publicIcons === undefined) {
      const index = anIcon ? graph.getModuleInfo(anIcon)?.importers.find((i) => LUCIDE_INDEX.test(slashes(i))) : undefined;
      const icons = anIcon ? iconsUsedByPublic(index, graph) : new Set<string>();
      if (typeof icons === "string") console.warn(`[vite.config] shell chunk group off (${icons}); automatic chunks instead`);
      publicIcons = typeof icons === "string" ? null : icons;
    }
    if (publicIcons === null) return null;
    const icon = ICON.exec(slashes(id));
    if (icon) return publicIcons.has(icon[1]!) ? null : "layout";
    return reachedByPublic(id, graph) ? null : "layout";
  };
  return { test, name };
}

/**
 * Build-time check of the shell chunk (see above): no public chunk (the entry, the methodology, legal and
 * sign-in pages) may load it statically, and it must not be part of an import cycle with a chunk that
 * runs code (Rolldown links the shell chunk and the re-export-only chunk of components/layout.tsx both
 * ways; that cycle is harmless because the latter has no code to run).
 */
function checkShellChunk(): Plugin {
  return {
    name: "icp:check-shell-chunk",
    apply: "build",
    generateBundle(_options, bundle) {
      const chunks = new Map<string, { code: string; imports: string[]; isEntry: boolean; facadeModuleId: string | null; moduleIds: string[] }>();
      for (const [file, item] of Object.entries(bundle)) if (item.type === "chunk") chunks.set(file, item);
      const shell = [...chunks].filter(([, c]) => c.moduleIds.some((m) => SHELL_ROOTS.test(slashes(m)))).map(([file]) => file);
      /** Only import and export statements: the chunk runs no code of its own. */
      const reExportOnly = (code: string) => !code
        .replace(/\/\*[\s\S]*?\*\/|\/\/[^\n]*/g, "")
        .replace(/\b(?:import|export)\s*(?:\*\s*as\s*[\w$]+|\*|\{[^}]*\})?\s*(?:from\s*)?["'`][^"'`]+["'`]\s*;?/g, "")
        .replace(/\bexport\s*\{[^}]*\}\s*;?/g, "")
        .trim();
      /** Every chunk that `from` loads statically, each mapped to the chunk that imports it. */
      const reach = (from: string) => {
        const via = new Map<string, string>();
        const queue = [from];
        while (queue.length) {
          const file = queue.shift()!;
          for (const next of chunks.get(file)?.imports ?? []) if (!via.has(next)) { via.set(next, file); queue.push(next); }
        }
        return via;
      };
      const chain = (via: Map<string, string>, from: string, to: string) => {
        const path = [to];
        for (let f = via.get(to); f !== undefined && path.length <= chunks.size; f = f === from ? undefined : via.get(f)) path.unshift(f);
        return path.join(" > ");
      };
      for (const [file, chunk] of chunks) {
        if (!chunk.isEntry && !(chunk.facadeModuleId !== null && PUBLIC_ROOTS.test(slashes(chunk.facadeModuleId)))) continue;
        const via = reach(file);
        const hit = shell.find((f) => via.has(f));
        if (hit) this.error(`a public chunk loads the signed-in shell chunk: ${chain(via, file, hit)} (vite.config.ts, shellChunk)`);
      }
      for (const file of shell) {
        if (reExportOnly(chunks.get(file)!.code)) continue;
        const via = reach(file);
        for (const other of via.keys()) {
          if (other === file || reExportOnly(chunks.get(other)!.code)) continue;
          const back = reach(other);
          if (back.has(file)) this.error(`the shell chunk is in an import cycle: ${chain(via, file, other)} > ${chain(back, other, file).split(" > ").slice(1).join(" > ")} (vite.config.ts, shellChunk)`);
        }
      }
    },
  };
}

export default defineConfig({
  plugins: [react(), checkShellChunk()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/api": { target: backend, changeOrigin: false } },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    chunkSizeWarningLimit: 900,
    rolldownOptions: {
      output: { codeSplitting: { groups: [{ ...shellChunk(), debugName: "layout", includeDependenciesRecursively: false }] } },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
