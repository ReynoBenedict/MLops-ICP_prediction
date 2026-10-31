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
from icp_api.insights.evidence import Value
from icp_api.insights.narrative import fmt_value
from icp_api.services.datasets import DatasetService
from icp_api.services.insights import fmt_pct, fmt_period, fmt_usd
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id, utcnow
from icp_ml.schema import DATASET_SPECS

logger = logging.getLogger(__name__)

KINDS: dict[str, dict[str, Any]] = {
    "icp_published": {
        "label_id": "ICP resmi bulan baru terdeteksi",
        "label_en": "New official ICP month detected",
        "params": {},
    },
    "icp_change_pct": {
        "label_id": "Perubahan ICP bulanan melebihi ambang",
        "label_en": "Monthly ICP change above threshold",
        "params": {"threshold_pct": 5.0},
    },
    "benchmark_change_pct": {
        "label_id": "Perubahan harga acuan bulanan melebihi ambang",
        "label_en": "Monthly benchmark change above threshold",
        "params": {"benchmark": "brent", "threshold_pct": 8.0},
    },
    "forecast_change": {
        "label_id": "Proyeksi bulan yang sama bergeser melebihi ambang",
        "label_en": "Forecast for the same month moved above threshold",
        "params": {"threshold_usd": 3.0},
    },
    "data_stale": {
        "label_id": "Data belum diperbarui sesuai jadwal",
        "label_en": "Data not updated on schedule",
        "params": {"dataset": "icp"},
    },
    "ingestion_failed": {
        "label_id": "Pembaruan data gagal",
        "label_en": "Data update failed",
        "params": {},
    },
}


def _p_en(period: str) -> str:
    return fmt_value(Value("p", period, "period"), "en")


def _usd_en(v: float) -> str:
    return fmt_value(Value("x", v, "usd"), "en")


def _pct_en(v: float) -> str:
    return fmt_value(Value("x", v, "pct_signed"), "en")


def with_labels(rule: dict[str, Any]) -> dict[str, Any]:
    k = KINDS.get(rule["kind"]) or {}
    return {**rule, "label_id": k.get("label_id"), "label_en": k.get("label_en")}


def shape_event(e: dict[str, Any]) -> dict[str, Any]:
    """API view of a stored event: English text, subject period and deep link come from the
    event's data (events stored before 2026-10-19 have none: null)."""
    data = dict(e.get("data") or {})
    meta = data.pop("_meta", None) or {}
    return {
        **e,
        "data": data,
        "title_en": meta.get("title_en"),
        "body_en": meta.get("body_en"),
        "period": meta.get("period", data.get("period")),
        "link": meta.get("link"),
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
        return with_labels(rule) if rule else {}

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
        updated = self.repo.get_alert_rule(org_id, rule_id)
        return with_labels(updated) if updated else {}

    def delete_rule(self, org_id: str, actor: dict[str, Any], rule_id: str, request_id: str | None) -> None:
        if not self.repo.delete_alert_rule(org_id, rule_id):
            raise ApiError(404, "NOT_FOUND", "Aturan peringatan tidak ditemukan.")
        self.repo.audit(
            f"user:{actor['id']}", "alert.rule.delete", "success", target=rule_id, request_id=request_id
        )

    def list_rules(self, org_id: str) -> list[dict[str, Any]]:
        return [
            {
                **r,
                "enabled": bool(r["enabled"]),
                "label_id": KINDS[r["kind"]]["label_id"],
                "label_en": KINDS[r["kind"]]["label_en"],
            }
            for r in self.repo.list_alert_rules(org_id)
        ]

    # -- events ------------------------------------------------------------------------------
    def events(self, org_id: str, limit: int, cursor: str | None, unread_only: bool) -> dict[str, Any]:
        page = self.repo.list_alert_events(org_id, limit, cursor, unread_only)
        return {
            "items": [shape_event(e) for e in page.items],
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
                extra = {k: ev.pop(k) for k in ("title_en", "body_en", "period", "link") if k in ev}
                ev["data"] = {**(ev.get("data") or {}), "_meta": extra}
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
        """Events with Indonesian ``title``/``body`` plus ``title_en``/``body_en``, the ``period`` the
        event is about and a deep-link target (stored in the event's data)."""
        kind, p = rule["kind"], rule["params"] or {}
        icp = facts["series"].get("icp")
        events: list[dict[str, Any]] = []
        if kind == "icp_published" and icp is not None and len(icp):
            last = icp.index.max()
            v = float(icp[last])
            events.append(
                {
                    "dedupe_key": str(last),
                    "severity": "info",
                    # created_at is the detection time; ESDM's own decree date is not claimed here
                    "title": f"ICP resmi {fmt_period(str(last))} terdeteksi: {fmt_usd(v)} per barel",
                    "body": f"Sistem mendeteksi nilai ICP resmi {fmt_period(str(last))} saat memperbarui data. "
                    "Waktu pemberitahuan adalah waktu deteksi, bukan tanggal penetapan oleh ESDM. "
                    "Nilai ini menjadi titik awal proyeksi berikutnya.",
                    "title_en": f"Official ICP for {_p_en(str(last))} detected: {_usd_en(v)} per barrel",
                    "body_en": f"The system detected the official ICP for {_p_en(str(last))} while updating data. "
                    "The notification time is the detection time, not ESDM's determination date. "
                    "This value becomes the starting point of the next forecast.",
                    "period": str(last),
                    "link": {"kind": "period", "period": str(last), "dataset": "icp"},
                    "data": {"period": str(last), "value": v},
                }
            )
        elif kind == "icp_change_pct" and icp is not None and len(icp) > 1:
            last, prev = icp.index.max(), icp.index.max() - 1
            if prev in icp.index:
                a, b = float(icp[prev]), float(icp[last])
                pct = (b - a) / a * 100
                thr = p.get("threshold_pct", 5)
                if abs(pct) >= float(thr):
                    events.append(
                        {
                            "dedupe_key": str(last),
                            "severity": "warning" if abs(pct) >= 10 else "info",
                            "title": f"ICP {fmt_period(str(last))} berubah {fmt_pct(pct)} (ambang {thr}%)",
                            "body": f"{fmt_usd(a)} → {fmt_usd(b)} per barel.",
                            "title_en": f"ICP {_p_en(str(last))} changed {_pct_en(pct)} (threshold {thr}%)",
                            "body_en": f"{_usd_en(a)} → {_usd_en(b)} per barrel.",
                            "period": str(last),
                            "link": {"kind": "period", "period": str(last), "dataset": "icp"},
                            "data": {"period": str(last), "pct": pct},
                        }
                    )
        elif kind == "benchmark_change_pct":
            bm = p.get("benchmark", "brent")
            s = facts["series"].get(bm)
            if s is not None and len(s) > 1:
                last, prev = s.index.max(), s.index.max() - 1
                if prev in s.index:
                    a, b = float(s[prev]), float(s[last])
                    pct = (b - a) / a * 100
                    if abs(pct) >= float(p.get("threshold_pct", 8)):
                        sp = DATASET_SPECS[bm]
                        events.append(
                            {
                                "dedupe_key": f"{bm}:{last}",
                                "severity": "info",
                                "title": f"{sp.label_id} {fmt_period(str(last))} berubah {fmt_pct(pct)}",
                                "body": f"{fmt_usd(a)} → {fmt_usd(b)} per barel (rata-rata bulanan).",
                                "title_en": f"{sp.label_en} {_p_en(str(last))} changed {_pct_en(pct)}",
                                "body_en": f"{_usd_en(a)} → {_usd_en(b)} per barrel (monthly average).",
                                "period": str(last),
                                "link": {"kind": "period", "period": str(last), "dataset": bm},
                                "data": {"period": str(last), "pct": pct, "benchmark": bm},
                            }
                        )
        elif kind == "forecast_change" and len(facts["forecasts"]) >= 2:
            cur, old = facts["forecasts"][0], facts["forecasts"][1]
            cur_pts = {x["period"]: x for x in cur["result"]["points"]}
            old_pts = {x["period"]: x for x in old["result"]["points"]}
            for period in sorted(set(cur_pts) & set(old_pts)):
                a, b = float(old_pts[period]["prediction"]), float(cur_pts[period]["prediction"])
                d = b - a
                if abs(d) >= float(p.get("threshold_usd", 3)):
                    events.append(
                        {
                            "dedupe_key": f"{period}:{cur['id']}",
                            "severity": "info",
                            "title": f"Proyeksi {fmt_period(period)} bergeser {fmt_usd(abs(d))} per barel",
                            "body": f"Terbitan {old['issue_date']}: {fmt_usd(a)} → terbitan {cur['issue_date']}: {fmt_usd(b)}.",
                            "title_en": f"Forecast for {_p_en(period)} moved {_usd_en(abs(d))} per barrel",
                            "body_en": f"Issue {old['issue_date']}: {_usd_en(a)} → issue {cur['issue_date']}: {_usd_en(b)}.",
                            "period": period,
                            "link": {"kind": "forecast", "forecast_id": cur["id"], "period": period},
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
                sp = DATASET_SPECS[ds]
                lp = fr.get("latest_period")
                late_id = "terlambat" if fr["status"] == "lagging" else "basi"
                events.append(
                    {
                        "dedupe_key": f"{ds}:{lp}:{fr['status']}",
                        "severity": "warning" if fr["status"] == "stale" else "info",
                        "title": f"{sp.label_id}: data terakhir {fmt_period(lp) if lp else '-'} ({late_id})",
                        "body": "Menurut jadwal publikasi sumber yang biasa, bulan yang lebih baru mungkin sudah "
                        "tersedia tetapi belum ada di sistem.",
                        "title_en": f"{sp.label_en}: latest data {_p_en(lp) if lp else '-'} ({'late' if fr['status'] == 'lagging' else 'stale'})",
                        "body_en": "By the source's usual publication schedule, a newer month may already be "
                        "available but is not in the system yet.",
                        "period": lp,
                        "link": {"kind": "dataset", "dataset": ds},
                        "data": fr,
                    }
                )
        elif kind == "ingestion_failed":
            for r in facts["failed_runs"][:5]:
                spec_r = DATASET_SPECS.get(r["dataset"])
                events.append(
                    {
                        "dedupe_key": r["id"],
                        "severity": "warning",
                        "title": f"Pembaruan {_label(r['dataset'])} gagal",
                        # Raw ingestion errors (upstream responses, paths) are operator detail; the
                        # tenant-facing event names the run so an operator can look it up.
                        "body": f"Percobaan pembaruan otomatis gagal; data yang ditampilkan adalah versi terakhir yang berhasil (ID proses {r['id']}).",
                        "title_en": f"{spec_r.label_en if spec_r else r['dataset']} update failed",
                        "body_en": f"An automatic update failed; the data shown is the last successful version (run ID {r['id']}).",
                        "period": None,
                        "link": {"kind": "dataset", "dataset": r["dataset"]},
                        "data": {"ingestion_run_id": r["id"], "dataset": r["dataset"]},
                    }
                )
        return events
