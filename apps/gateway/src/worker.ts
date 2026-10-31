/**
 * Worker entry point (`main` in wrangler.jsonc). The Workers runtime treats every named export of the
 * main module as an entrypoint and refuses to start when one is not a handler: the string export
 * `CLIENT_IP_HEADER` in src/index.ts made workerd fail with "Incorrect type for map entry" (found
 * 2026-10-25 with `wrangler dev`; `wrangler deploy --dry-run` does not run the script). So the main
 * module exports only the handler; src/index.ts keeps its named exports for the tests.
 */
export { default } from "./index";
