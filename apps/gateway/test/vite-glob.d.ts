// Vite/Vitest `import.meta.glob` (resilience.test.ts lists apps/web/public with it; the thunks are never called).
interface ImportMeta {
  glob(pattern: string | string[], options?: { eager?: false; query?: string; import?: string }): Record<string, () => Promise<unknown>>;
}
