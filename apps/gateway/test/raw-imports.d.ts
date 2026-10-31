// Vite/Vitest `?raw` imports: the file's text as a string (used by entrypoint.test.ts for wrangler.jsonc).
declare module "*?raw" {
  const content: string;
  export default content;
}
