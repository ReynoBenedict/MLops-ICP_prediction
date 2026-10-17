// Fails when packages/contracts is out of date with the backend's OpenAPI schema.
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { ROOT, npmCmd, requirePython, run } from "./lib.mjs";

const tmp = mkdtempSync(path.join(os.tmpdir(), "icp-contracts-"));
try {
  const spec = path.join(tmp, "openapi.json");
  const types = path.join(tmp, "schema.d.ts");
  run(requirePython(), ["-m", "icp_api.cli", "export-openapi", "--out", spec]);
  run(npmCmd(), ["exec", "-w", "@icp/contracts", "--", "openapi-typescript", spec, "-o", types],
      { shell: process.platform === "win32" });
  const pairs = [
    [spec, path.join(ROOT, "packages", "contracts", "openapi.json")],
    [types, path.join(ROOT, "packages", "contracts", "src", "schema.d.ts")],
  ];
  const norm = (f) => readFileSync(f, "utf8").replace(/\r\n/g, "\n");
  const stale = pairs.filter(([fresh, committed]) => {
    try {
      return norm(fresh) !== norm(committed);
    } catch {
      return true;
    }
  });
  if (stale.length) {
    console.error(`API contract is out of date: ${stale.map(([, c]) => path.relative(ROOT, c)).join(", ")}.`);
    console.error("Run `npm run contracts` and commit the result.");
    process.exit(1);
  }
  console.log("API contract is up to date.");
} finally {
  rmSync(tmp, { recursive: true, force: true });
}
