// Run the project's Python (from .venv) with the given arguments: `node scripts/py.mjs -m pytest`.
import { requirePython, run } from "./lib.mjs";

run(requirePython(), process.argv.slice(2));
