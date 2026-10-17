"""Configurable alerts: rules per organisation, evaluated against real data after every data or
model change (and on demand). Events are in-app notifications (no e-mail/webhook delivery is
configured; see docs/COMMERCIAL_READINESS.md), de-duplicated per (rule, fact) so a user is told
once per ICP month, not once per evaluation.
"""

from __future__ import annotations

import logging
from typing import Any

from icp_api.entitlements import plan
from icp_api.errors import ApiError
from icp_api.services.datasets import DatasetService
from icp_api.services.insights import fmt_pct, fmt_period, fmt_usd
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id, utcnow
from icp_ml.schema import DATASET_SPECS

logger = logging.getLogger(__name__)

KINDS: dict[str, dict[str, Any]] = {
    "icp_published": {"label_id": "ICP resmi bulan baru terbit", "params": {}},
    "icp_change_pct": {"label_id": "Perubahan ICP bulanan melebihi ambang", "params": {"threshold_pct": 5.0}},
    "benchmark_change_pct": {
        "label_id": "Perubahan harga acuan bulanan melebihi ambang",
        "params": {"benchmark": "brent", "threshold_pct": 8.0},
    },
    "forecast_change": {
        "label_id": "Proyeksi bulan yang sama bergeser melebihi ambang",
        "params": {"threshold_usd": 3.0},
    },
    "data_stale": {"label_id": "Data belum diperbarui sesuai jadwal", "params": {"dataset": "icp"}},
    "ingestion_failed": {"label_id": "Pembaruan data gagal", "params": {}},
}


def _label(dataset: str) -> str:
    sp = DATASET_SPECS.get(dataset)
    return sp.label_id if sp else dataset


def validate_rule(kind: str, params: dict[str, Any]) -> dict[str, Any]:
    if kind not in KINDS:
        raise ApiError(422, "VALIDATION_ERROR", f"Jenis peringatan tidak dikenal: {kind}")
    merged = {**KINDS[kind]["params"], **(params or {})}
    if "threshold_pct" in merged and not 0.1 <= float(merged["threshold_pct"]) <= 100:
        raise ApiError(422, "VALIDATION_ERROR", "threshold_pct harus antara 0,1 dan 100")
    if "threshold_usd" in merged and not 0.1 <= float(merged["threshold_usd"]) <= 100:
        raise ApiError(422, "VALIDATION_ERROR", "threshold_usd harus antara 0,1 dan 100")
    if "benchmark" in merged and merged["benchmark"] not in ("brent", "dubai", "wti"):
        raise ApiError(422, "VALIDATION_ERROR", "benchmark harus brent, dubai, atau wti")
    if "dataset" in merged and merged["dataset"] not in DATASET_SPECS:
        raise ApiError(422, "VALIDATION_ERROR", "dataset tidak dikenal")
    return merged


class AlertService:
    def __init__(self, repo: ProductRepository, datasets: DatasetService):
        self.repo = repo
        self.datasets = datasets

    # -- rules -------------------------------------------------------------------------------
    def kinds(self) -> list[dict[str, Any]]:
        return [{"kind": k, **v} for k, v in KINDS.items()]

    def create_rule(
        self, org_id: str, actor: dict[str, Any], kind: str, params: dict[str, Any], request_id: str | None
    ) -> dict[str, Any]:
        org = self.repo.get_organization(org_id)
        p = plan(org["plan"] if org else None)
        if self.repo.count_alert_rules(org_id) >= p.alert_rules_max:
            raise ApiError(
                403,
                "PLAN_LIMIT",
                f"Paket {p.label_id} mencakup maksimal {p.alert_rules_max} aturan peringatan.",
                {"limit": p.alert_rules_max, "plan": p.code},
            )
        merged = validate_rule(kind, params)
        rid = new_id("alr")
        self.repo.insert_alert_rule(
            {
                "id": rid,
                "org_id": org_id,
                "created_by": actor["id"] or actor.get("name", "operator"),
                "kind": kind,
                "params": merged,
            }
        )
        self.repo.audit(
            f"user:{actor['id']}" if actor.get("id") else str(actor.get("name")),
            "alert.rule.create",
            "success",
            target=rid,
            request_id=request_id,
            details={"kind": kind},
        )
        rule = self.repo.get_alert_rule(org_id, rid)
        self.evaluate(org_id=org_id)  # tell the user immediately whether the condition already holds
        return rule or {}

    def update_rule(
        self,
        org_id: str,
        actor: dict[str, Any],
        rule_id: str,
        params: dict[str, Any] | None,
        enabled: bool | None,
        request_id: str | None,
    ) -> dict[str, Any]:
        rule = self.repo.get_alert_rule(org_id, rule_id)
        if rule is None:
            raise ApiError(404, "NOT_FOUND", "Aturan peringatan tidak ditemukan.")
        fields: dict[str, Any] = {}
        if params is not None:
            fields["params"] = validate_rule(rule["kind"], params)
        if enabled is not None:
            fields["enabled"] = enabled
        self.repo.update_alert_rule(org_id, rule_id, **fields)
        self.repo.audit(
            f"user:{actor['id']}", "alert.rule.update", "success", target=rule_id, request_id=request_id
        )
        return self.repo.get_alert_rule(org_id, rule_id) or {}

    def delete_rule(self, org_id: str, actor: dict[str, Any], rule_id: str, request_id: str | None) -> None:
        if not self.repo.delete_alert_rule(org_id, rule_id):
            raise ApiError(404, "NOT_FOUND", "Aturan peringatan tidak ditemukan.")
        self.repo.audit(
            f"user:{actor['id']}", "alert.rule.delete", "success", target=rule_id, request_id=request_id
        )

    def list_rules(self, org_id: str) -> list[dict[str, Any]]:
        return [
            {**r, "enabled": bool(r["enabled"]), "label_id": KINDS[r["kind"]]["label_id"]}
            for r in self.repo.list_alert_rules(org_id)
        ]

    # -- events ------------------------------------------------------------------------------
    def events(self, org_id: str, limit: int, cursor: str | None, unread_only: bool) -> dict[str, Any]:
        page = self.repo.list_alert_events(org_id, limit, cursor, unread_only)
        return {
            "items": page.items,
            "next_cursor": page.next_cursor,
            "unread": self.repo.count_unread_alerts(org_id),
        }

    def mark_read(self, org_id: str, event_id: str | None) -> int:
        return self.repo.mark_alert_read(org_id, event_id)

    # -- evaluation --------------------------------------------------------------------------
    def evaluate(self, org_id: str | None = None) -> dict[str, int]:
        """Evaluate every enabled rule (optionally one organisation). Returns counts per kind."""
        rules = self.repo.list_alert_rules(org_id, enabled_only=True)
        if not rules:
            return {}
        facts = self._facts()
        created: dict[str, int] = {}
        for rule in rules:
            try:
                events = self._evaluate_rule(rule, facts)
            except Exception:
                logger.exception("alert rule %s failed", rule["id"])
                continue
            n = 0
            for ev in events:
                if self.repo.insert_alert_event(
                    {**ev, "id": new_id("ale"), "rule_id": rule["id"], "org_id": rule["org_id"]}
                ):
                    n += 1
            self.repo.update_alert_rule(
                rule["org_id"],
                rule["id"],
                last_evaluated_at=utcnow(),
                **({"last_triggered_at": utcnow()} if n else {}),
            )
            if n:
                created[rule["kind"]] = created.get(rule["kind"], 0) + n
        return created

    def _facts(self) -> dict[str, Any]:
        facts: dict[str, Any] = {"series": {}, "freshness": {}, "failed_runs": [], "forecasts": []}
        for ds in DATASET_SPECS:
            a = self.datasets.optional(ds)
            if a is not None:
                facts["series"][ds] = a.series
                facts["freshness"][ds] = self.datasets.provenance(ds)["freshness"]
        facts["failed_runs"] = [
            r for r in self.repo.list_ingestion_runs(None, 30, None).items if r.get("status") == "failed"
        ]
        page = self.repo.list_forecasts(10, None, "succeeded")
        facts["forecasts"] = [r for r in page.items if r.get("workflow") == "system" and r.get("result")]
        return facts

    def _evaluate_rule(self, rule: dict[str, Any], facts: dict[str, Any]) -> list[dict[str, Any]]:
        kind, p = rule["kind"], rule["params"] or {}
        icp = facts["series"].get("icp")
        events: list[dict[str, Any]] = []
        if kind == "icp_published" and icp is not None and len(icp):
            last = icp.index.max()
            events.append(
                {
                    "dedupe_key": str(last),
                    "severity": "info",
                    "title": f"ICP resmi {fmt_period(str(last))} tersedia: {fmt_usd(float(icp[last]))} per barel",
                    "body": "Nilai resmi baru telah masuk ke riwayat dan menjadi titik awal proyeksi berikutnya.",
                    "data": {"period": str(last), "value": float(icp[last])},
                }
            )
        elif kind == "icp_change_pct" and icp is not None and len(icp) > 1:
            last, prev = icp.index.max(), icp.index.max() - 1
            if prev in icp.index:
                pct = (float(icp[last]) - float(icp[prev])) / float(icp[prev]) * 100
                if abs(pct) >= float(p.get("threshold_pct", 5)):
                    events.append(
                        {
                            "dedupe_key": str(last),
                            "severity": "warning" if abs(pct) >= 10 else "info",
                            "title": f"ICP {fmt_period(str(last))} berubah {fmt_pct(pct)} (ambang {p.get('threshold_pct', 5)}%)",
                            "body": f"{fmt_usd(float(icp[prev]))} → {fmt_usd(float(icp[last]))} per barel.",
                            "data": {"period": str(last), "pct": pct},
                        }
                    )
        elif kind == "benchmark_change_pct":
            b = p.get("benchmark", "brent")
            s = facts["series"].get(b)
            if s is not None and len(s) > 1:
                last, prev = s.index.max(), s.index.max() - 1
                if prev in s.index:
                    pct = (float(s[last]) - float(s[prev])) / float(s[prev]) * 100
                    if abs(pct) >= float(p.get("threshold_pct", 8)):
                        events.append(
                            {
                                "dedupe_key": f"{b}:{last}",
                                "severity": "info",
                                "title": f"{DATASET_SPECS[b].label_id} {fmt_period(str(last))} berubah {fmt_pct(pct)}",
                                "body": f"{fmt_usd(float(s[prev]))} → {fmt_usd(float(s[last]))} per barel (rata-rata bulanan).",
                                "data": {"period": str(last), "pct": pct, "benchmark": b},
                            }
                        )
        elif kind == "forecast_change" and len(facts["forecasts"]) >= 2:
            cur, old = facts["forecasts"][0], facts["forecasts"][1]
            cur_pts = {x["period"]: x for x in cur["result"]["points"]}
            old_pts = {x["period"]: x for x in old["result"]["points"]}
            for period in sorted(set(cur_pts) & set(old_pts)):
                d = cur_pts[period]["prediction"] - old_pts[period]["prediction"]
                if abs(d) >= float(p.get("threshold_usd", 3)):
                    events.append(
                        {
                            "dedupe_key": f"{period}:{cur['id']}",
                            "severity": "info",
                            "title": f"Proyeksi {fmt_period(period)} bergeser {fmt_usd(abs(d))} per barel",
                            "body": f"Terbitan {old['issue_date']}: {fmt_usd(old_pts[period]['prediction'])} → terbitan {cur['issue_date']}: {fmt_usd(cur_pts[period]['prediction'])}.",
                            "data": {
                                "period": period,
                                "delta": d,
                                "forecast_id": cur["id"],
                                "previous_forecast_id": old["id"],
                            },
                        }
                    )
        elif kind == "data_stale":
            ds = p.get("dataset", "icp")
            fr = facts["freshness"].get(ds)
            if fr and fr.get("status") in ("lagging", "stale"):
                events.append(
                    {
                        "dedupe_key": f"{ds}:{fr.get('latest_period')}:{fr['status']}",
                        "severity": "warning" if fr["status"] == "stale" else "info",
                        "title": f"{DATASET_SPECS[ds].label_id}: data terakhir {fmt_period(fr['latest_period']) if fr.get('latest_period') else '-'} ({fr['status']})",
                        "body": "Berdasarkan jadwal publikasi sumber, data yang lebih baru seharusnya sudah tersedia.",
                        "data": fr,
                    }
                )
        elif kind == "ingestion_failed":
            for r in facts["failed_runs"][:5]:
                events.append(
                    {
                        "dedupe_key": r["id"],
                        "severity": "warning",
                        "title": f"Pembaruan {_label(r['dataset'])} gagal",
                        "body": str(r.get("error_message") or "")[:300],
                        "data": {"ingestion_run_id": r["id"], "dataset": r["dataset"]},
                    }
                )
        return events
