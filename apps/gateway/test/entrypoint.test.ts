/**
 * The Workers runtime treats every named export of the main module as an entrypoint and refuses to
 * start when one is not a handler ("Incorrect type for map entry 'CLIENT_IP_HEADER'", workerd via
 * `wrangler dev`, 2026-10-25). `wrangler deploy --dry-run` does not execute the script, so this test
 * guards the entry point instead: the module that wrangler.jsonc names as `main` (top level and every
 * environment) must export only the default handler.
 */
import { describe, expect, it } from "vitest";
import worker from "../src/index";
import * as entry from "../src/worker";
import wranglerText from "../wrangler.jsonc?raw";
import { parseJsonc } from "./jsonc";

interface WranglerConfig { main?: string; env?: Record<string, { main?: string }> }

describe("Worker entry point", () => {
  it("exports only the default handler (fetch, and scheduled for the daily refresh)", () => {
    expect(Object.keys(entry)).toEqual(["default"]);
    expect(entry.default).toBe(worker);
    expect(typeof entry.default.fetch).toBe("function");
    expect(typeof entry.default.scheduled).toBe("function");
  });

  it("is the module wrangler.jsonc deploys, in every environment", async () => {
    const config = parseJsonc(wranglerText) as WranglerConfig;
    expect(config.main).toBe("src/worker.ts");
    for (const [name, env] of Object.entries(config.env ?? {})) {
      expect(env.main ?? config.main, `env.${name}.main`).toBe("src/worker.ts");
    }
    // Load the configured file itself, so a different `main` with extra exports fails here too.
    const configured = (await import(/* @vite-ignore */ `../${config.main}`)) as Record<string, unknown>;
    expect(Object.keys(configured)).toEqual(["default"]);
    expect(configured.default).toBe(worker);
  });

  it("parses JSONC without touching // inside strings", () => {
    expect(parseJsonc('{ "url": "https://a.example//x", // note\n "n": 1, /* block */ "l": [1,], }'))
      .toEqual({ url: "https://a.example//x", n: 1, l: [1] });
  });
});
