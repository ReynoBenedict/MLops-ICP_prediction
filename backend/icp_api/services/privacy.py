"""Privacy operations: organisation data export and account/organisation deletion requests.

* Export: the organisation owner downloads every tenant record of their own organisation as a ZIP of
  JSON files (plus CSV for the tabular ones). Generated on demand and streamed; nothing is stored
  server-side. Secrets (password, session-token and API-key hashes) are never included. Official
  datasets are public and not part of a tenant export.
* Deletion: a member asks for their own account to be deleted, the owner for the whole organisation.
  A platform operator executes (anonymises personal data, revokes every credential) or declines.
  Nothing is hard-deleted: audit rows, usage counters and forecast/report records stay (they carry
  ids, not personal data), and official datasets are never touched. No e-mail is sent (none exists).
"""

from __future__ import annotations

import csv
import io
import json
import secrets
import zipfile
from datetime import datetime, timezone
from typing import Any

from icp_api.errors import ApiError
from icp_api.services.accounts import customer_actor, customer_audit_row
from icp_api.services.reports import cell_safe
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id, utcnow

EXPORT_FORMAT_VERSION = 1
_JSON_TABLES = {
    "scenarios.json": "saved_scenarios",
    "alert_rules.json": "alert_rules",
    "alert_events.json": "alert_events",
    "reports.json": "report_runs",
    "forecasts.json": "forecast_runs",
    "usage_events.json": "usage_events",
    "plan_requests.json": "plan_change_requests",
    "deletion_requests.json": "deletion_requests",
}


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str).encode("utf-8")


def _csv_bytes(rows: list[dict[str, Any]], columns: list[str]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    for r in rows:
        w.writerow(
            [
                cell_safe(
                    json.dumps(r.get(c), default=str) if isinstance(r.get(c), dict | list) else r.get(c)
                )
                for c in columns
            ]
        )
    return buf.getvalue().encode("utf-8-sig")


class PrivacyService:
    def __init__(self, repo: ProductRepository):
        self.repo = repo

    # -- export ------------------------------------------------------------------------------
    def export(self, org_id: str, user_id: str, request_id: str | None) -> tuple[bytes, str]:
        org = self.repo.get_organization(org_id)
        if org is None:
            raise ApiError(404, "NOT_FOUND", "Organisasi tidak ditemukan.")
        members = self.repo.export_rows("users", org_id)
        files: dict[str, bytes] = {
            "organization.json": _json_bytes(org),
            "members.json": _json_bytes(members),
            "members.csv": _csv_bytes(
                members, ["id", "email", "name", "role", "status", "created_at", "last_login_at"]
            ),
            # Only the requesting owner's own sessions: other members' devices (user agents, sign-in
            # times) are their personal data, not the organisation's.
            "sessions.json": _json_bytes(self.repo.export_sessions(user_id)),
            "api_keys.json": _json_bytes(self.repo.export_rows("api_keys", org_id)),
        }
        tables = {name: self.repo.export_rows(table, org_id) for name, table in _JSON_TABLES.items()}
        for name in ("plan_requests.json", "deletion_requests.json"):
            tables[name] = [{**r, "resolved_by": customer_actor(r.get("resolved_by"))} for r in tables[name]]
        files.update({name: _json_bytes(rows) for name, rows in tables.items()})
        files["scenarios.csv"] = _csv_bytes(
            tables["scenarios.json"],
            ["id", "name", "description", "horizon", "inputs", "created_at", "updated_at", "archived_at"],
        )
        files["usage_events.csv"] = _csv_bytes(
            tables["usage_events.json"], ["id", "created_at", "action", "units", "user_id", "api_key_id"]
        )
        # Operator names and operator-only notes are internal; members' own records are kept whole.
        files["audit_events.json"] = _json_bytes(
            [customer_audit_row(r) for r in self.repo.export_audit(org_id)]
        )
        generated = utcnow()
        manifest = {
            "format_version": EXPORT_FORMAT_VERSION,
            "org_id": org_id,
            "generated_at": generated,
            "generated_by": user_id,
            "files": sorted([*files, "manifest.json"]),
            "note": "Data organisasi Anda di ICP Intelligence. Dataset resmi bersifat publik dan tidak disertakan. "
            "Hash kata sandi, token sesi dan kunci API tidak pernah diekspor.",
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("manifest.json", _json_bytes(manifest))
            for name, data in files.items():
                z.writestr(name, data)
        self.repo.audit(
            f"user:{user_id}",
            "org.export",
            "success",
            target=org_id,
            request_id=request_id,
            details={"files": len(files) + 1},
        )
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        return buf.getvalue(), f"icp-intelligence_org-export_{org_id}_{stamp}.zip"

    # -- deletion requests -------------------------------------------------------------------
    def _shape(self, row: dict[str, Any], customer: bool = False) -> dict[str, Any]:
        org = self.repo.get_organization(row["org_id"])
        names: dict[str, str | None] = {}
        for key in ("user_id", "requested_by"):
            uid = row.get(key)
            u = self.repo.get_user(uid) if uid else None
            names[key] = u["name"] if u else None
        out = {
            **row,
            "org_name": org["name"] if org else None,
            "user_name": names["user_id"],
            "requested_by_name": names["requested_by"],
        }
        if customer:
            out["resolved_by"] = customer_actor(row.get("resolved_by"))
        return out

    def _get(self, rid: str, org_id: str | None = None) -> dict[str, Any]:
        row = self.repo.get_deletion_request(rid)
        if row is None or (org_id is not None and row["org_id"] != org_id):
            raise ApiError(404, "NOT_FOUND", "Permintaan tidak ditemukan.")
        return row

    def create(
        self, org_id: str, user_id: str, role: str, scope: str, reason: str | None, request_id: str | None
    ) -> dict[str, Any]:
        from icp_api.storage.sql import IntegrityError

        if scope == "account" and role == "owner":
            raise ApiError(
                409,
                "OWNER_ACCOUNT",
                "Pemilik tidak dapat menghapus akunnya sendiri saja. Ajukan penghapusan organisasi, "
                "atau minta operator mengalihkan kepemilikan terlebih dahulu.",
            )
        if scope == "organization" and role != "owner":
            raise ApiError(403, "FORBIDDEN", "Hanya pemilik yang dapat meminta penghapusan organisasi.")
        target = user_id if scope == "account" else None
        if self.repo.pending_deletion_request(org_id, scope, target) is not None:
            raise ApiError(409, "CONFLICT", "Permintaan penghapusan yang sama masih menunggu keputusan.")
        rid = new_id("del")
        try:
            self.repo.insert_deletion_request(
                {
                    "id": rid,
                    "org_id": org_id,
                    "scope": scope,
                    "user_id": target,
                    "requested_by": user_id,
                    "reason": (reason or "").strip() or None,
                }
            )
        except IntegrityError as exc:  # concurrent duplicate: the unique pending index decides
            raise ApiError(
                409, "CONFLICT", "Permintaan penghapusan yang sama masih menunggu keputusan."
            ) from exc
        self.repo.audit(
            f"user:{user_id}",
            f"{'org' if scope == 'organization' else 'account'}.deletion.request",
            "success",
            target=org_id if scope == "organization" else user_id,
            request_id=request_id,
            details={"request_id": rid},
        )
        return self._shape(self._get(rid), customer=True)

    def list(
        self,
        org_id: str | None,
        status: str | None,
        limit: int,
        cursor: str | None,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        page = self.repo.list_deletion_requests(org_id, status, limit, cursor, requested_by)
        customer = org_id is not None
        return {"items": [self._shape(r, customer) for r in page.items], "next_cursor": page.next_cursor}

    def withdraw(self, rid: str, org_id: str, user_id: str, request_id: str | None) -> dict[str, Any]:
        """Only the person who asked may withdraw a request. An owner can see a member's request
        for their own account but cannot cancel it (the member's right to erasure); operators
        decline through ``resolve``."""
        row = self._get(rid, org_id)
        if row["requested_by"] != user_id:
            raise ApiError(404, "NOT_FOUND", "Permintaan tidak ditemukan.")
        if not self.repo.resolve_deletion_request(rid, "withdrawn", f"user:{user_id}", None):
            raise ApiError(409, "CONFLICT", "Permintaan ini sudah diputuskan atau ditarik.")
        self.repo.audit(
            f"user:{user_id}",
            "deletion_request.withdraw",
            "success",
            target=org_id,
            request_id=request_id,
            details={"request_id": rid},
        )
        return self._shape(self._get(rid), customer=True)

    def resolve(
        self, rid: str, decision: str, operator: str, note: str | None, request_id: str | None
    ) -> dict[str, Any]:
        row = self._get(rid)
        if row["status"] != "pending":
            raise ApiError(409, "CONFLICT", "Permintaan ini sudah diputuskan atau ditarik.")
        status = "executed" if decision == "execute" else "declined"
        if not self.repo.resolve_deletion_request(rid, status, operator, (note or "").strip() or None):
            raise ApiError(409, "CONFLICT", "Permintaan ini sudah diputuskan atau ditarik.")
        if status == "executed":
            self._execute(row, operator, request_id)
        else:
            self.repo.audit(
                operator,
                "deletion_request.decline",
                "success",
                target=row["org_id"],
                request_id=request_id,
                details={"request_id": rid},
            )
        return self._shape(self._get(rid))

    def _execute(self, row: dict[str, Any], operator: str, request_id: str | None) -> None:
        def unusable() -> str:  # not an scrypt hash: verify_password always fails
            return "deleted$" + secrets.token_urlsafe(24)

        if row["scope"] == "account":
            self.repo.anonymise_user(row["user_id"], unusable())
            self.repo.audit(
                operator,
                "account.deletion.execute",
                "success",
                target=row["user_id"],
                request_id=request_id,
                details={"request_id": row["id"], "org_id": row["org_id"]},
            )
            return
        users = self.repo.list_users(row["org_id"])
        for u in users:
            self.repo.anonymise_user(u["id"], unusable())
        self.repo.anonymise_organization(row["org_id"])
        self.repo.audit(
            operator,
            "org.deletion.execute",
            "success",
            target=row["org_id"],
            request_id=request_id,
            details={"request_id": row["id"], "members_anonymised": len(users)},
        )
