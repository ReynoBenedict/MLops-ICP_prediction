# legacy/

The previous implementation, kept for reference and course evidence. **Nothing here is part of the
active application.** It is not installed, built, linted, type-checked or tested (it is excluded in
`pyproject.toml`, `.dockerignore` and the scripts).

| Folder | Contents |
|---|---|
| `streamlit/` | The Streamlit dashboard (pages, components, services, settings, theme, Dockerfile) |
| `serving/` | The LK-10/LK-11 FastAPI `/predict` service and the MLflow server Dockerfile |
| `pipeline-v1/` | The LK-06…LK-12 training scripts, old tests, configs, CT drift simulator, old compose file |
| `ci/` | The previous GitHub Actions workflows |
| `evidence/` | Course artifacts (metrics.json, CT trigger event, scaling proof, registry YAML, plots, traffic scripts) |
| `mlflow-history/` | Read-only export of the previous MLflow tracking database and artifacts (see its README) |

Several of these files had uncommitted edits when they were moved, so they were moved rather than
deleted. Once you have committed or reviewed them, the folder can be deleted. Everything else is also
in Git history (last commit before the refactor: `69f4308`). See [docs/MIGRATION.md](../docs/MIGRATION.md)
for where each piece of logic went and which defects were fixed.
