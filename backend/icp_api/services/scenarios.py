"""Saved scenarios (per organisation) and side-by-side comparison against the no-scenario baseline.

A scenario is a named set of assumed monthly benchmark/FX values for months inside the forecast
window. Running it produces an ordinary forecast record (workflow ``scenario``) so the issue date,
model version and dataset versions are preserved exactly like any other forecast.
"""

from __future__ import annotations

from typing import Any

from icp_api.entitlements import plan
from icp_api.errors import ApiError
from icp_api.services.forecasts import ForecastService
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import Page, new_id

MAX_COMPARE = 4


class ScenarioService:
    def __init__(self, repo: ProductRepository, forecasts: ForecastService):
        self.repo = repo
        self.forecasts = forecasts

    def _shape(self, row: dict[str, Any]) -> dict[str, Any]:
        return {**row, "archived": bool(row.get("archived_at"))}

    def create(
        self,
        org_id: str,
        actor: dict[str, Any],
        name: str,
        description: str | None,
        horizon: int,
        inputs: dict[str, dict[str, float]],
        request_id: str | None,
    ) -> dict[str, Any]:
        org = self.repo.get_organization(org_id)
        p = plan(org["plan"] if org else None)
        if not p.scenarios:
            raise ApiError(
                403,
                "FEATURE_NOT_IN_PLAN",
                f"Skenario tidak termasuk dalam paket {p.label_id}.",
                {"feature": "scenarios", "plan": p.code},
            )
        if self.repo.count_scenarios(org_id) >= p.saved_scenarios_max:
            raise ApiError(
                403,
                "PLAN_LIMIT",
                f"Paket {p.label_id} mencakup maksimal {p.saved_scenarios_max} skenario tersimpan.",
                {"limit": p.saved_scenarios_max, "plan": p.code},
            )
        if horizon > p.forecast_horizon_max:
            raise ApiError(
                403,
                "PLAN_LIMIT",
                f"Paket {p.label_id} mendukung horizon maksimal {p.forecast_horizon_max} bulan.",
                {"limit": p.forecast_horizon_max, "plan": p.code},
            )
        sid = new_id("scn")
        self.repo.insert_scenario(
            {
                "id": sid,
                "org_id": org_id,
                "created_by": actor["id"],
                "name": name.strip()[:120],
                "description": (description or "").strip()[:1000] or None,
                "horizon": horizon,
                "inputs": {k: v for k, v in inputs.items() if v},
            }
        )
        self.repo.audit(
            f"user:{actor['id']}", "scenario.create", "success", target=sid, request_id=request_id
        )
        return self.get(org_id, sid)

    def get(self, org_id: str, scenario_id: str) -> dict[str, Any]:
        row = self.repo.get_scenario(org_id, scenario_id)
        if row is None:
            raise ApiError(404, "NOT_FOUND", "Skenario tidak ditemukan.")
        return self._shape(row)

    def list_scenarios(
        self, org_id: str, limit: int, cursor: str | None, include_archived: bool = False
    ) -> Page:
        page = self.repo.list_scenarios(org_id, limit, cursor, include_archived)
        return Page([self._shape(r) for r in page.items], page.next_cursor)

    def update(
        self, org_id: str, actor: dict[str, Any], scenario_id: str, request_id: str | None, **fields: Any
    ) -> dict[str, Any]:
        self.get(org_id, scenario_id)
        clean = {k: v for k, v in fields.items() if v is not None}
        if "name" in clean:
            clean["name"] = clean["name"].strip()[:120]
        if "inputs" in clean:
            clean["inputs"] = {k: v for k, v in clean["inputs"].items() if v}
        if clean:
            self.repo.update_scenario(org_id, scenario_id, **clean)
        self.repo.audit(
            f"user:{actor['id']}",
            "scenario.update",
            "success",
            target=scenario_id,
            request_id=request_id,
            details={"fields": sorted(clean)},
        )
        return self.get(org_id, scenario_id)

    def archive(self, org_id: str, actor: dict[str, Any], scenario_id: str, request_id: str | None) -> None:
        from icp_api.storage.repository import utcnow

        self.get(org_id, scenario_id)
        self.repo.update_scenario(org_id, scenario_id, archived_at=utcnow())
        self.repo.audit(
            f"user:{actor['id']}", "scenario.archive", "success", target=scenario_id, request_id=request_id
        )

    def run(self, org_id: str, actor_name: str, scenario_id: str, request_id: str | None) -> dict[str, Any]:
        sc = self.get(org_id, scenario_id)
        fc = self.forecasts.run(
            sc["horizon"],
            sc["inputs"],
            actor_name,
            request_id,
            workflow="scenario",
            org_id=org_id,
            scenario_id=scenario_id,
        )
        self.repo.update_scenario(org_id, scenario_id, last_forecast_id=fc["id"])
        self.repo.record_usage(org_id, None, None, "scenario.run", request_id=request_id)
        return fc

    def compare(
        self,
        org_id: str,
        actor_name: str,
        scenario_ids: list[str],
        request_id: str | None,
        horizon: int | None = None,
    ) -> dict[str, Any]:
        """Run the baseline (no assumptions) and up to MAX_COMPARE scenarios on the same horizon and present them side by side."""
        ids = list(dict.fromkeys(scenario_ids))[:MAX_COMPARE]
        scenarios = [self.get(org_id, s) for s in ids]
        h = horizon or max([s["horizon"] for s in scenarios] or [3])
        baseline = self.forecasts.run(h, {}, actor_name, request_id, workflow="scenario", org_id=org_id)
        runs = []
        for s in scenarios:
            fc = self.forecasts.run(
                h,
                s["inputs"],
                actor_name,
                request_id,
                workflow="scenario",
                org_id=org_id,
                scenario_id=s["id"],
            )
            self.repo.update_scenario(org_id, s["id"], last_forecast_id=fc["id"])
            runs.append({"scenario": s, "forecast": fc})
        periods = [p["period"] for p in baseline["result"]["points"]]
        table = []
        for i, period in enumerate(periods):
            base_pt = baseline["result"]["points"][i]
            row: dict[str, Any] = {
                "period": period,
                "kind": base_pt["kind"],
                "baseline": base_pt["prediction"],
                "baseline_idr": base_pt.get("prediction_idr"),
                "scenarios": {},
            }
            for r in runs:
                pt = next((x for x in r["forecast"]["result"]["points"] if x["period"] == period), None)
                if pt:
                    row["scenarios"][r["scenario"]["id"]] = {
                        "prediction": pt["prediction"],
                        "prediction_idr": pt.get("prediction_idr"),
                        "delta_vs_baseline": round(pt["prediction"] - base_pt["prediction"], 4),
                        "conditional_on_scenario": pt["conditional_on_scenario"],
                        "inputs": {b: pt["benchmarks"][b] for b in ("brent", "dubai", "wti")},
                        "fx_source": pt.get("fx_source"),
                    }
            table.append(row)
        self.repo.record_usage(
            org_id, None, None, "scenario.compare", units=len(runs) + 1, request_id=request_id
        )
        return {
            "horizon": h,
            "forecast_origin": baseline["forecast_origin"],
            "issue_date": baseline["issue_date"],
            "method": baseline["method"],
            "baseline_forecast_id": baseline["id"],
            "scenarios": [
                {
                    "id": r["scenario"]["id"],
                    "name": r["scenario"]["name"],
                    "forecast_id": r["forecast"]["id"],
                    "inputs": r["scenario"]["inputs"],
                }
                for r in runs
            ],
            "rows": table,
            "note": "Baseline menggunakan data acuan yang teramati untuk bulan yang telah berakhir dan nilai terakhir yang diketahui untuk bulan mendatang; "
            "skenario menggantikan asumsi tersebut hanya pada bulan yang Anda isi.",
        }
