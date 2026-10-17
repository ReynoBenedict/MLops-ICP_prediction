// Open the MLflow UI on the local tracking store (http://127.0.0.1:5000).
import path from "node:path";
import { ROOT, requirePython, run } from "./lib.mjs";

const store = path.join(ROOT, "var", "mlflow");
run(requirePython(), [
  "-m", "mlflow", "ui",
  "--backend-store-uri", `sqlite:///${path.join(store, "mlflow.db").replaceAll("\\", "/")}`,
  "--host", "127.0.0.1", "--port", "5000", "--workers", "1",
]);
