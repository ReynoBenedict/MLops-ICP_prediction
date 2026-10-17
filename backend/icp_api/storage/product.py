"""Tenant-scoped product tables: organisations, users, sessions, API keys, scenarios, alerts,
reports and usage accounting (migration 0004). Extends the metadata repository so one object
serves every service; every query that returns tenant data takes the ``org_id`` explicitly.
"""

from __future__ import annotations

from typing import Any

from icp_api.storage.repository import MetadataRepository, Page, _decode, _json, utcnow


class ProductRepository(MetadataRepository):
    # -- organisations & users ---------------------------------------------------------------
    def insert_organization(self, row: dict[str, Any]) -> None:
        now = utcnow()
        self.db.execute(
            "INSERT INTO organizations (id, name, slug, plan, status, created_at, updated_at, settings_json) "
            "VALUES (?, ?, ?, ?, 'active', ?, ?, ?)",
            (
                row["id"],
                row["name"],
                row["slug"],
                row.get("plan", "trial"),
                now,
                now,
                _json(row.get("settings")),
            ),
        )

    def get_organization(self, org_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM organizations WHERE id = ?", (org_id,))

    def get_organization_by_slug(self, slug: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM organizations WHERE slug = ?", (slug,))

    def update_organization(self, org_id: str, **fields: Any) -> None:
        allowed = {"name", "plan", "status", "settings"}
        sets, params = [], []
        for k, v in fields.items():
            if k not in allowed:
                raise ValueError(k)
            col = "settings_json" if k == "settings" else k
            sets.append(f"{col} = ?")
            params.append(_json(v) if k == "settings" else v)
        sets.append("updated_at = ?")
        params.append(utcnow())
        self.db.execute(f"UPDATE organizations SET {', '.join(sets)} WHERE id = ?", (*params, org_id))  # noqa: S608 - columns whitelisted above

    def list_organizations(self, limit: int, cursor: str | None) -> Page:
        return self._page("organizations", [], [], limit, cursor)

    def count_users(self, org_id: str, active_only: bool = True) -> int:
        sql = "SELECT COUNT(*) AS n FROM users WHERE org_id = ?" + (  # noqa: S608 - constant fragment
            " AND status = 'active'" if active_only else ""
        )
        return int(self.db.query(sql, (org_id,))[0]["n"])

    def insert_user(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO users (id, org_id, email, name, password_hash, role, status, must_change_password, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)",
            (
                row["id"],
                row["org_id"],
                row["email"],
                row["name"],
                row["password_hash"],
                row["role"],
                1 if row.get("must_change_password") else 0,
                utcnow(),
            ),
        )

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM users WHERE id = ?", (user_id,))

    def get_user_by_email(self, email: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM users WHERE email = ?", (email.lower(),))

    def list_users(self, org_id: str) -> list[dict[str, Any]]:
        rows = self.db.query("SELECT * FROM users WHERE org_id = ? ORDER BY created_at, id", (org_id,))
        return [r for r in (_decode(x) for x in rows) if r is not None]

    def update_user(self, user_id: str, **fields: Any) -> None:
        allowed = {"name", "role", "status", "password_hash", "must_change_password", "last_login_at"}
        sets, params = [], []
        for k, v in fields.items():
            if k not in allowed:
                raise ValueError(k)
            sets.append(f"{k} = ?")
            params.append(int(v) if k == "must_change_password" else v)
        if sets:
            self.db.execute(f"UPDATE users SET {', '.join(sets)} WHERE id = ?", (*params, user_id))  # noqa: S608 - columns whitelisted above

    # -- sessions ----------------------------------------------------------------------------
    def insert_session(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO sessions (id, user_id, token_hash, created_at, expires_at, last_seen_at, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["user_id"],
                row["token_hash"],
                utcnow(),
                row["expires_at"],
                utcnow(),
                row.get("user_agent"),
            ),
        )

    def get_session_by_hash(self, token_hash: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT s.*, u.org_id, u.email, u.name AS user_name, u.role, u.status AS user_status, u.must_change_password, "
            "o.plan, o.status AS org_status, o.name AS org_name, o.slug AS org_slug "
            "FROM sessions s JOIN users u ON u.id = s.user_id JOIN organizations o ON o.id = u.org_id WHERE s.token_hash = ?",
            (token_hash,),
        )

    def touch_session(self, session_id: str) -> None:
        self.db.execute("UPDATE sessions SET last_seen_at = ? WHERE id = ?", (utcnow(), session_id))

    def revoke_session(self, session_id: str) -> None:
        self.db.execute(
            "UPDATE sessions SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL", (utcnow(), session_id)
        )

    def revoke_user_sessions(self, user_id: str) -> int:
        return self.db.execute(
            "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL", (utcnow(), user_id)
        )

    def list_sessions(self, user_id: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT id, created_at, expires_at, last_seen_at, revoked_at, user_agent FROM sessions WHERE user_id = ? "
            "ORDER BY created_at DESC LIMIT 50",
            (user_id,),
        )
        return [dict(r) for r in rows]

    # -- API keys ----------------------------------------------------------------------------
    def insert_api_key(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO api_keys (id, org_id, name, prefix, key_hash, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["org_id"],
                row["name"],
                row["prefix"],
                row["key_hash"],
                row["created_by"],
                utcnow(),
            ),
        )

    def get_api_key_by_hash(self, key_hash: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT k.*, o.plan, o.status AS org_status, o.name AS org_name, o.slug AS org_slug FROM api_keys k "
            "JOIN organizations o ON o.id = k.org_id WHERE k.key_hash = ?",
            (key_hash,),
        )

    def touch_api_key(self, key_id: str) -> None:
        self.db.execute("UPDATE api_keys SET last_used_at = ? WHERE id = ?", (utcnow(), key_id))

    def list_api_keys(self, org_id: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT id, org_id, name, prefix, created_by, created_at, last_used_at, revoked_at FROM api_keys WHERE org_id = ? "
            "ORDER BY created_at DESC",
            (org_id,),
        )
        return [dict(r) for r in rows]

    def revoke_api_key(self, org_id: str, key_id: str) -> int:
        return self.db.execute(
            "UPDATE api_keys SET revoked_at = ? WHERE id = ? AND org_id = ? AND revoked_at IS NULL",
            (utcnow(), key_id, org_id),
        )

    # -- scenarios ---------------------------------------------------------------------------
    def insert_scenario(self, row: dict[str, Any]) -> None:
        now = utcnow()
        self.db.execute(
            "INSERT INTO saved_scenarios (id, org_id, created_by, name, description, horizon, inputs_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["org_id"],
                row["created_by"],
                row["name"],
                row.get("description"),
                row["horizon"],
                _json(row["inputs"]),
                now,
                now,
            ),
        )

    def get_scenario(self, org_id: str, scenario_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM saved_scenarios WHERE id = ? AND org_id = ?", (scenario_id, org_id))

    def list_scenarios(
        self, org_id: str, limit: int, cursor: str | None, include_archived: bool = False
    ) -> Page:
        where = ["org_id = ?"] + ([] if include_archived else ["archived_at IS NULL"])
        return self._page("saved_scenarios", where, [org_id], limit, cursor)

    def count_scenarios(self, org_id: str) -> int:
        return int(
            self.db.query(
                "SELECT COUNT(*) AS n FROM saved_scenarios WHERE org_id = ? AND archived_at IS NULL",
                (org_id,),
            )[0]["n"]
        )

    def update_scenario(self, org_id: str, scenario_id: str, **fields: Any) -> None:
        allowed = {"name", "description", "horizon", "inputs", "last_forecast_id", "archived_at"}
        sets, params = [], []
        for k, v in fields.items():
            if k not in allowed:
                raise ValueError(k)
            col = "inputs_json" if k == "inputs" else k
            sets.append(f"{col} = ?")
            params.append(_json(v) if k == "inputs" else v)
        sets.append("updated_at = ?")
        params.append(utcnow())
        self.db.execute(
            f"UPDATE saved_scenarios SET {', '.join(sets)} WHERE id = ? AND org_id = ?",  # noqa: S608 - columns/tables are whitelisted constants
            (*params, scenario_id, org_id),
        )

    # -- alerts ------------------------------------------------------------------------------
    def insert_alert_rule(self, row: dict[str, Any]) -> None:
        now = utcnow()
        self.db.execute(
            "INSERT INTO alert_rules (id, org_id, created_by, kind, params_json, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["org_id"],
                row["created_by"],
                row["kind"],
                _json(row.get("params") or {}),
                1 if row.get("enabled", True) else 0,
                now,
                now,
            ),
        )

    def get_alert_rule(self, org_id: str, rule_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM alert_rules WHERE id = ? AND org_id = ?", (rule_id, org_id))

    def list_alert_rules(self, org_id: str | None = None, enabled_only: bool = False) -> list[dict[str, Any]]:
        where, params = [], []
        if org_id is not None:
            where.append("org_id = ?")
            params.append(org_id)
        if enabled_only:
            where.append("enabled = 1")
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        rows = self.db.query(
            f"SELECT * FROM alert_rules {clause} ORDER BY created_at DESC, id DESC",  # noqa: S608 - clause built from constants
            tuple(params),
        )
        return [r for r in (_decode(x) for x in rows) if r is not None]

    def count_alert_rules(self, org_id: str) -> int:
        return int(self.db.query("SELECT COUNT(*) AS n FROM alert_rules WHERE org_id = ?", (org_id,))[0]["n"])

    def update_alert_rule(self, org_id: str, rule_id: str, **fields: Any) -> None:
        allowed = {"params", "enabled", "last_evaluated_at", "last_triggered_at"}
        sets, params = [], []
        for k, v in fields.items():
            if k not in allowed:
                raise ValueError(k)
            col = "params_json" if k == "params" else k
            sets.append(f"{col} = ?")
            params.append(_json(v) if k == "params" else (int(v) if k == "enabled" else v))
        sets.append("updated_at = ?")
        params.append(utcnow())
        self.db.execute(
            f"UPDATE alert_rules SET {', '.join(sets)} WHERE id = ? AND org_id = ?",  # noqa: S608 - columns/tables are whitelisted constants
            (*params, rule_id, org_id),
        )

    def delete_alert_rule(self, org_id: str, rule_id: str) -> int:
        self.db.execute("DELETE FROM alert_events WHERE rule_id = ? AND org_id = ?", (rule_id, org_id))
        return self.db.execute("DELETE FROM alert_rules WHERE id = ? AND org_id = ?", (rule_id, org_id))

    def insert_alert_event(self, row: dict[str, Any]) -> bool:
        """Insert unless an event with the same (rule, dedupe_key) exists. Returns True when inserted."""
        exists = self.db.query(
            "SELECT 1 FROM alert_events WHERE rule_id = ? AND dedupe_key = ?",
            (row["rule_id"], row["dedupe_key"]),
        )
        if exists:
            return False
        self.db.execute(
            "INSERT INTO alert_events (id, rule_id, org_id, created_at, dedupe_key, severity, title, body, data_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["rule_id"],
                row["org_id"],
                utcnow(),
                row["dedupe_key"],
                row["severity"],
                row["title"],
                row["body"],
                _json(row.get("data")),
            ),
        )
        return True

    def list_alert_events(
        self, org_id: str, limit: int, cursor: str | None, unread_only: bool = False
    ) -> Page:
        where = ["org_id = ?"] + (["read_at IS NULL"] if unread_only else [])
        return self._page("alert_events", where, [org_id], limit, cursor)

    def count_unread_alerts(self, org_id: str) -> int:
        return int(
            self.db.query(
                "SELECT COUNT(*) AS n FROM alert_events WHERE org_id = ? AND read_at IS NULL", (org_id,)
            )[0]["n"]
        )

    def mark_alert_read(self, org_id: str, event_id: str | None) -> int:
        if event_id is None:
            return self.db.execute(
                "UPDATE alert_events SET read_at = ? WHERE org_id = ? AND read_at IS NULL", (utcnow(), org_id)
            )
        return self.db.execute(
            "UPDATE alert_events SET read_at = ? WHERE id = ? AND org_id = ? AND read_at IS NULL",
            (utcnow(), event_id, org_id),
        )

    # -- reports -----------------------------------------------------------------------------
    def insert_report(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO report_runs (id, org_id, created_by, kind, params_json, status, created_at, finished_at, object_key_pdf, object_key_xlsx, "
            "object_key_csv, error_message, summary_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row.get("org_id"),
                row["created_by"],
                row["kind"],
                _json(row["params"]),
                row["status"],
                row["created_at"],
                row.get("finished_at"),
                row.get("object_key_pdf"),
                row.get("object_key_xlsx"),
                row.get("object_key_csv"),
                row.get("error_message"),
                _json(row.get("summary")),
            ),
        )

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM report_runs WHERE id = ?", (report_id,))

    def list_reports(
        self, org_id: str | None, limit: int, cursor: str | None, include_platform: bool = True
    ) -> Page:
        if org_id is None:
            return self._page("report_runs", [], [], limit, cursor)
        if include_platform:
            return self._page("report_runs", ["(org_id = ? OR org_id IS NULL)"], [org_id], limit, cursor)
        return self._page("report_runs", ["org_id = ?"], [org_id], limit, cursor)

    # -- usage -------------------------------------------------------------------------------
    def record_usage(
        self,
        org_id: str | None,
        user_id: str | None,
        api_key_id: str | None,
        action: str,
        units: int = 1,
        request_id: str | None = None,
    ) -> None:
        from icp_api.storage.repository import new_id

        self.db.execute(
            "INSERT INTO usage_events (id, org_id, user_id, api_key_id, created_at, action, units, request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (new_id("use"), org_id, user_id, api_key_id, utcnow(), action, units, request_id),
        )

    def usage_summary(self, org_id: str, since: str) -> dict[str, int]:
        rows = self.db.query(
            "SELECT action, SUM(units) AS units FROM usage_events WHERE org_id = ? AND created_at >= ? GROUP BY action",
            (org_id, since),
        )
        return {str(r["action"]): int(r["units"] or 0) for r in rows}

    # -- forecasts (tenant views) ------------------------------------------------------------
    def list_org_forecasts(
        self, org_id: str, limit: int, cursor: str | None, status: str | None = None
    ) -> Page:
        where, params = ["org_id = ?"], [org_id]
        if status:
            where.append("status = ?")
            params.append(status)
        return self._page("forecast_runs", where, params, limit, cursor)

    def list_system_forecasts(self, limit: int, cursor: str | None) -> Page:
        return self._page("forecast_runs", ["workflow = 'system'", "status = 'succeeded'"], [], limit, cursor)

    def latest_system_forecast(self) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM forecast_runs WHERE status = 'succeeded' AND workflow = 'system' ORDER BY created_at DESC, id DESC LIMIT 1",
            (),
        )

    def forecasts_for_target(self, period: str, limit: int = 50) -> list[dict[str, Any]]:
        """Succeeded runs (any workflow) whose window includes ``period`` - for forecast-history / evaluation."""
        rows = self.db.query(
            "SELECT * FROM forecast_runs WHERE status = 'succeeded' AND forecast_origin < ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (period, limit),
        )
        return [r for r in (_decode(x) for x in rows) if r is not None]
