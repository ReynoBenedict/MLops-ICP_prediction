# Previous MLflow history (exported 2026-10-16)

Read-only export of the tracking server used by the Docker Compose setup (LK-09…LK-12). The source
Docker volumes (`mlops-icp_prediction_old_pgdata`, `mlops-icp_prediction_old_mlflow-artifacts`) were
not modified or deleted.

- `mlflow_postgres_dump.sql`: `pg_dump` of the `mlflow` database (schema + data).
- `mlflow_artifacts.tar.gz`: the artifact volume (`1/<run_id>/artifacts/{model/, pred_*.png}`).

Contents: experiment `icp-price-prediction`, 4 runs from 2026-05-28.

| Run | RMSE | MAE |
|---|---|---|
| LinearRegression | 3.616 | 1.916 |
| RandomForest_n10 | 5.554 | 3.316 |
| RandomForest_n50 | 4.953 | 3.046 |
| RandomForest_n100 | 4.890 | 2.958 |

`ICP_Price_Model` v1 (the LinearRegression run) was in the `Production` stage. These models were
trained on gap-bridging lag features (see docs/MIGRATION.md, defect 1). Do not promote them into the
new registry.

To inspect it:
```bash
docker run --rm -d --name mlflow-legacy -e POSTGRES_PASSWORD=x -e POSTGRES_USER=mlflow -e POSTGRES_DB=mlflow postgres:15-alpine
docker exec -i mlflow-legacy psql -U mlflow -d mlflow < mlflow_postgres_dump.sql
```
