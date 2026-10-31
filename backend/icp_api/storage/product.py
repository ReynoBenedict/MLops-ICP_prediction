"""Tenant-scoped product tables: organisations, users, sessions, API keys, scenarios, alerts,
reports and usage accounting (migration 0004). Extends the metadata repository so one object
serves every service; every query that returns tenant data takes the ``org_id`` explicitly.
"""

from __future__ import annotations

from typing import Any

from icp_api.storage.repository import MetadataRepository, Page, _decode, _json, new_id, utcnow

# Partial updates use one constant statement per table: every updatable column is written as
# ``col = CASE WHEN ? THEN ? ELSE col END`` and receives a (flag, value) pair, so the SQL text never
# depends on the caller and every value is a bound parameter (no string-built SQL).
_ORG_UPDATE = (
    "UPDATE organizations SET "
    "name = CASE WHEN ? THEN ? ELSE name END, "
    "plan = CASE WHEN ? THEN ? ELSE plan END, "
    "status = CASE WHEN ? THEN ? ELSE status END, "
    "settings_json = CASE WHEN ? THEN ? ELSE settings_json END, "
    "updated_at = ? WHERE id = ?"
)
_ORG_COLUMNS = ("name", "plan", "status", "settings")
_USER_UPDATE = (
    "UPDATE users SET "
    "name = CASE WHEN ? THEN ? ELSE name END, "
    "role = CASE WHEN ? THEN ? ELSE role END, "
    "status = CASE WHEN ? THEN ? ELSE status END, "
    "password_hash = CASE WHEN ? THEN ? ELSE password_hash END, "
    "must_change_password = CASE WHEN ? THEN ? ELSE must_change_password END, "
    "last_login_at = CASE WHEN ? THEN ? ELSE last_login_at END, "
    "temp_password_expires_at = CASE WHEN ? THEN ? ELSE temp_password_expires_at END, "
    "email_verified_at = CASE WHEN ? THEN ? ELSE email_verified_at END "
    "WHERE id = ?"
)
_USER_COLUMNS = (
    "name",
    "role",
    "status",
    "password_hash",
    "must_change_password",
    "last_login_at",
    "temp_password_expires_at",
    "email_verified_at",
)
_SCENARIO_UPDATE = (
    "UPDATE saved_scenarios SET "
    "name = CASE WHEN ? THEN ? ELSE name END, "
    "description = CASE WHEN ? THEN ? ELSE description END, "
    "horizon = CASE WHEN ? THEN ? ELSE horizon END, "
    "inputs_json = CASE WHEN ? THEN ? ELSE inputs_json END, "
    "last_forecast_id = CASE WHEN ? THEN ? ELSE last_forecast_id END, "
    "archived_at = CASE WHEN ? THEN ? ELSE archived_at END, "
    "updated_at = ? WHERE id = ? AND org_id = ?"
)
_SCENARIO_COLUMNS = ("name", "description", "horizon", "inputs", "last_forecast_id", "archived_at")
_ALERT_RULE_UPDATE = (
    "UPDATE alert_rules SET "
    "params_json = CASE WHEN ? THEN ? ELSE params_json END, "
    "enabled = CASE WHEN ? THEN ? ELSE enabled END, "
    "last_evaluated_at = CASE WHEN ? THEN ? ELSE last_evaluated_at END, "
    "last_triggered_at = CASE WHEN ? THEN ? ELSE last_triggered_at END, "
    "updated_at = ? WHERE id = ? AND org_id = ?"
)
_ALERT_RULE_COLUMNS = ("params", "enabled", "last_evaluated_at", "last_triggered_at")

# Tenant data export: one constant statement per table (secret columns are never selected).
_EXPORT_SQL = {
    "users": "SELECT id, org_id, email, name, role, status, must_change_password, created_at, last_login_at, "
    "email_verified_at "
    "FROM users WHERE org_id = ? ORDER BY created_at, id",
    "api_keys": "SELECT id, org_id, name, prefix, created_by, created_at, last_used_at, revoked_at "
    "FROM api_keys WHERE org_id = ? ORDER BY created_at, id",
    "saved_scenarios": "SELECT * FROM saved_scenarios WHERE org_id = ? ORDER BY created_at, id",
    "alert_rules": "SELECT * FROM alert_rules WHERE org_id = ? ORDER BY created_at, id",
    "alert_events": "SELECT * FROM alert_events WHERE org_id = ? ORDER BY created_at, id",
    "report_runs": "SELECT * FROM report_runs WHERE org_id = ? ORDER BY created_at, id",
    "forecast_runs": "SELECT * FROM forecast_runs WHERE org_id = ? ORDER BY created_at, id",
    "usage_events": "SELECT * FROM usage_events WHERE org_id = ? ORDER BY created_at, id",
    "plan_change_requests": "SELECT * FROM plan_change_requests WHERE org_id = ? ORDER BY created_at, id",
    "deletion_requests": "SELECT * FROM deletion_requests WHERE org_id = ? ORDER BY created_at, id",
}


def _case_params(columns: tuple[str, ...], fields: dict[str, Any], encode: Any = None) -> list[Any]:
    """(flag, value) pairs for a ``CASE WHEN ? THEN ? ELSE col END`` update, in column order."""
    unknown = set(fields) - set(columns)
    if unknown:
        raise ValueError(sorted(unknown)[0])
    out: list[Any] = []
    for col in columns:
        if col in fields:
            value = fields[col]
            out += [1, encode(col, value) if encode else value]
        else:
            out += [0, None]
    return out


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
        params = _case_params(_ORG_COLUMNS, fields, lambda k, v: _json(v) if k == "settings" else v)
        self.db.execute(_ORG_UPDATE, (*params, utcnow(), org_id))

    def delete_empty_organization(self, org_id: str) -> int:
        """Remove an organisation that has no users (compensation for a lost registration race)."""
        return self.db.execute(
            "DELETE FROM organizations WHERE id = ? AND NOT EXISTS (SELECT 1 FROM users WHERE org_id = ?)",
            (org_id, org_id),
        )

    def list_organizations(self, limit: int, cursor: str | None) -> Page:
        return self._page("organizations", [], [], limit, cursor)

    def count_users(self, org_id: str, active_only: bool = True) -> int:
        rows = self.db.query(
            "SELECT COUNT(*) AS n FROM users WHERE org_id = ? AND (? = 0 OR status = 'active')",
            (org_id, 1 if active_only else 0),
        )
        return int(rows[0]["n"])

    def insert_user(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO users (id, org_id, email, name, password_hash, role, status, must_change_password, created_at, "
            "temp_password_expires_at) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)",
            (
                row["id"],
                row["org_id"],
                row["email"],
                row["name"],
                row["password_hash"],
                row["role"],
                1 if row.get("must_change_password") else 0,
                utcnow(),
                row.get("temp_password_expires_at"),
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
        params = _case_params(
            _USER_COLUMNS, fields, lambda k, v: int(v) if k == "must_change_password" else v
        )
        if fields:
            self.db.execute(_USER_UPDATE, (*params, user_id))

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
            "u.temp_password_expires_at, "
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

    def list_active_sessions(self, user_id: str, now: str) -> list[dict[str, Any]]:
        rows = self.db.query(
            "SELECT id, created_at, expires_at, last_seen_at, user_agent FROM sessions "
            "WHERE user_id = ? AND revoked_at IS NULL AND expires_at > ? ORDER BY created_at DESC LIMIT 50",
            (user_id, now),
        )
        return [dict(r) for r in rows]

    def revoke_user_session(self, user_id: str, session_id: str) -> int:
        """Revoke one active session of ``user_id`` (0 when it is not theirs or already ended)."""
        return self.db.execute(
            "UPDATE sessions SET revoked_at = ? WHERE id = ? AND user_id = ? AND revoked_at IS NULL",
            (utcnow(), session_id, user_id),
        )

    def revoke_other_sessions(self, user_id: str, keep_session_id: str) -> int:
        return self.db.execute(
            "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND id <> ? AND revoked_at IS NULL",
            (utcnow(), user_id, keep_session_id),
        )

    # -- e-mail tokens (migration 0007) ------------------------------------------------------
    def insert_email_token(self, row: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO email_tokens (id, user_id, purpose, token_hash, created_at, expires_at, request_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["user_id"],
                row["purpose"],
                row["token_hash"],
                utcnow(),
                row["expires_at"],
                row.get("request_id"),
            ),
        )

    def supersede_email_tokens(self, user_id: str, purpose: str) -> int:
        """Mark every unused token of this purpose as used (a newer one replaces it)."""
        return self.db.execute(
            "UPDATE email_tokens SET used_at = ? WHERE user_id = ? AND purpose = ? AND used_at IS NULL",
            (utcnow(), user_id, purpose),
        )

    def consume_email_token(self, token_hash: str, purpose: str) -> dict[str, Any] | None:
        """Single use: one conditional UPDATE marks the token used only if it is unused and unexpired,
        so two concurrent attempts cannot both succeed. Returns the token row or None."""
        now = utcnow()
        changed = self.db.execute(
            "UPDATE email_tokens SET used_at = ? WHERE token_hash = ? AND purpose = ? AND used_at IS NULL "
            "AND expires_at > ?",
            (now, token_hash, purpose, now),
        )
        if not changed:
            return None
        return self._one(
            "SELECT id, user_id, purpose, created_at, expires_at, used_at FROM email_tokens WHERE token_hash = ?",
            (token_hash,),
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
        params = _case_params(_SCENARIO_COLUMNS, fields, lambda k, v: _json(v) if k == "inputs" else v)
        self.db.execute(_SCENARIO_UPDATE, (*params, utcnow(), scenario_id, org_id))

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
        rows = self.db.query(
            "SELECT * FROM alert_rules WHERE (? IS NULL OR org_id = ?) AND (? = 0 OR enabled = 1) "
            "ORDER BY created_at DESC, id DESC",
            (org_id, org_id, 1 if enabled_only else 0),
        )
        return [r for r in (_decode(x) for x in rows) if r is not None]

    def count_alert_rules(self, org_id: str) -> int:
        return int(self.db.query("SELECT COUNT(*) AS n FROM alert_rules WHERE org_id = ?", (org_id,))[0]["n"])

    def update_alert_rule(self, org_id: str, rule_id: str, **fields: Any) -> None:
        params = _case_params(
            _ALERT_RULE_COLUMNS,
            fields,
            lambda k, v: _json(v) if k == "params" else (int(v) if k == "enabled" else v),
        )
        self.db.execute(_ALERT_RULE_UPDATE, (*params, utcnow(), rule_id, org_id))

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
        self.db.execute(
            "INSERT INTO usage_events (id, org_id, user_id, api_key_id, created_at, action, units, request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (new_id("use"), org_id, user_id, api_key_id, utcnow(), action, units, request_id),
        )

    def reserve_usage(
        self,
        org_id: str,
        api_key_id: str | None,
        action: str,
        since: str,
        limit: int,
        request_id: str | None = None,
    ) -> str | None:
        """Atomically record one unit unless ``limit`` units of ``action`` since ``since`` already
        exist (one conditional INSERT, so concurrent requests cannot overshoot). Returns the new
        usage id, or None when the limit is reached."""
        uid = new_id("use")
        inserted = self.db.execute(
            "INSERT INTO usage_events (id, org_id, user_id, api_key_id, created_at, action, units, request_id) "
            "SELECT ?, ?, NULL, ?, ?, ?, 1, ? WHERE (SELECT COALESCE(SUM(units), 0) FROM usage_events "
            "WHERE org_id = ? AND action = ? AND created_at >= ?) < ?",
            (uid, org_id, api_key_id, utcnow(), action, request_id, org_id, action, since, limit),
        )
        return uid if inserted else None

    def delete_usage(self, usage_id: str) -> int:
        """Hand back a reserved unit (a refused API-key request does not consume quota)."""
        return self.db.execute("DELETE FROM usage_events WHERE id = ?", (usage_id,))

    def count_usage(self, org_id: str, action: str, since: str) -> int:
        rows = self.db.query(
            "SELECT COALESCE(SUM(units), 0) AS n FROM usage_events WHERE org_id = ? AND action = ? AND created_at >= ?",
            (org_id, action, since),
        )
        return int(rows[0]["n"] or 0) if rows else 0

    def usage_summary(self, org_id: str, since: str) -> dict[str, int]:
        rows = self.db.query(
            "SELECT action, SUM(units) AS units FROM usage_events WHERE org_id = ? AND created_at >= ? GROUP BY action",
            (org_id, since),
        )
        return {str(r["action"]): int(r["units"] or 0) for r in rows}

    # -- forecasts (tenant views) ------------------------------------------------------------
    def list_org_forecasts(
        self,
        org_id: str,
        limit: int,
        cursor: str | None,
        status: str | None = None,
        include_system: bool = False,
    ) -> Page:
        """An organisation's runs; ``include_system`` adds the published (succeeded) system forecasts."""
        own = "org_id = ?"
        where = [f"({own} OR (workflow = 'system' AND status = 'succeeded'))" if include_system else own]
        params: list[Any] = [org_id]
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

    def system_forecasts(self, limit: int = 2) -> list[dict[str, Any]]:
        """Most recent succeeded system forecasts, newest first."""
        return self.list_system_forecasts(limit, None).items

    def forecasts_for_target(self, period: str, limit: int = 50) -> list[dict[str, Any]]:
        """Succeeded runs (any workflow) whose window includes ``period`` - for forecast-history / evaluation."""
        rows = self.db.query(
            "SELECT * FROM forecast_runs WHERE status = 'succeeded' AND forecast_origin < ? ORDER BY created_at DESC, id DESC LIMIT ?",
            (period, limit),
        )
        return [r for r in (_decode(x) for x in rows) if r is not None]

    # -- plan change requests (migration 0005) -----------------------------------------------
    def insert_plan_request(self, row: dict[str, Any]) -> None:
        """Raises IntegrityError when the organisation already has a pending request."""
        self.db.execute(
            "INSERT INTO plan_change_requests (id, org_id, requested_by, previous_plan, requested_plan, note, "
            "status, created_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
            (
                row["id"],
                row["org_id"],
                row["requested_by"],
                row["previous_plan"],
                row["requested_plan"],
                row.get("note"),
                row.get("created_at") or utcnow(),
            ),
        )

    def get_plan_request(self, request_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM plan_change_requests WHERE id = ?", (request_id,))

    def pending_plan_request(self, org_id: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM plan_change_requests WHERE org_id = ? AND status = 'pending'", (org_id,)
        )

    def list_plan_requests(
        self, org_id: str | None, status: str | None, limit: int, cursor: str | None
    ) -> Page:
        where: list[str] = []
        params: list[Any] = []
        if org_id is not None:
            where.append("org_id = ?")
            params.append(org_id)
        if status is not None:
            where.append("status = ?")
            params.append(status)
        return self._page("plan_change_requests", where, params, limit, cursor)

    def resolve_plan_request(self, request_id: str, status: str, resolved_by: str, note: str | None) -> bool:
        """Pending -> final status; returns False when the request was no longer pending."""
        before = self.get_plan_request(request_id)
        if before is None or before["status"] != "pending":
            return False
        self.db.execute(
            "UPDATE plan_change_requests SET status = ?, resolved_at = ?, resolved_by = ?, resolution_note = ? "
            "WHERE id = ? AND status = 'pending'",
            (status, utcnow(), resolved_by, note, request_id),
        )
        after = self.get_plan_request(request_id)
        return bool(after and after["status"] == status and after["resolved_by"] == resolved_by)

    # -- deletion requests (migration 0006) --------------------------------------------------
    def insert_deletion_request(self, row: dict[str, Any]) -> None:
        """Raises IntegrityError when an identical request is already pending."""
        self.db.execute(
            "INSERT INTO deletion_requests (id, org_id, scope, user_id, requested_by, reason, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)",
            (
                row["id"],
                row["org_id"],
                row["scope"],
                row.get("user_id"),
                row["requested_by"],
                row.get("reason"),
                utcnow(),
            ),
        )

    def get_deletion_request(self, request_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM deletion_requests WHERE id = ?", (request_id,))

    def pending_deletion_request(self, org_id: str, scope: str, user_id: str | None) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM deletion_requests WHERE org_id = ? AND scope = ? AND COALESCE(user_id, '') = ? "
            "AND status = 'pending'",
            (org_id, scope, user_id or ""),
        )

    def list_deletion_requests(
        self,
        org_id: str | None,
        status: str | None,
        limit: int,
        cursor: str | None,
        requested_by: str | None = None,
    ) -> Page:
        where: list[str] = []
        params: list[Any] = []
        for col, value in (("org_id", org_id), ("status", status), ("requested_by", requested_by)):
            if value is not None:
                where.append(f"{col} = ?")
                params.append(value)
        return self._page("deletion_requests", where, params, limit, cursor)

    def resolve_deletion_request(
        self, request_id: str, status: str, resolved_by: str, note: str | None
    ) -> bool:
        """Pending -> final status; returns False when the request was no longer pending."""
        before = self.get_deletion_request(request_id)
        if before is None or before["status"] != "pending":
            return False
        stamp = utcnow()
        self.db.execute(
            "UPDATE deletion_requests SET status = ?, resolved_at = ?, resolved_by = ?, resolution_note = ? "
            "WHERE id = ? AND status = 'pending'",
            (status, stamp, resolved_by, note, request_id),
        )
        after = self.get_deletion_request(request_id)
        return bool(after and after["status"] == status and after["resolved_at"] == stamp)

    def anonymise_user(self, user_id: str, unusable_hash: str) -> None:
        """Replace personal data of one account; the row stays for referential and audit integrity."""
        now = utcnow()
        self.db.execute(
            "UPDATE users SET email = ?, name = 'Pengguna dihapus', password_hash = ?, status = 'disabled', "
            "must_change_password = 0, deleted_at = ? WHERE id = ?",
            (f"deleted+{user_id}@invalid.local", unusable_hash, now, user_id),
        )
        self.db.execute(
            "UPDATE sessions SET revoked_at = COALESCE(revoked_at, ?), user_agent = NULL WHERE user_id = ?",
            (now, user_id),
        )
        self.db.execute(
            "UPDATE email_tokens SET used_at = COALESCE(used_at, ?) WHERE user_id = ?",
            (now, user_id),
        )

    def anonymise_organization(self, org_id: str) -> None:
        """Organisation part of an executed deletion (members are anonymised one by one)."""
        now = utcnow()
        self.db.execute(
            "UPDATE organizations SET name = 'Organisasi dihapus', status = 'suspended', settings_json = NULL, "
            "deleted_at = ?, updated_at = ? WHERE id = ?",
            (now, now, org_id),
        )
        self.db.execute(
            "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, ?), name = 'dihapus' WHERE org_id = ?",
            (now, org_id),
        )
        self.db.execute(
            "UPDATE saved_scenarios SET name = 'Skenario dihapus', description = NULL, "
            "archived_at = COALESCE(archived_at, ?), updated_at = ? WHERE org_id = ?",
            (now, now, org_id),
        )
        self.db.execute("UPDATE alert_rules SET enabled = 0, updated_at = ? WHERE org_id = ?", (now, org_id))
        self.db.execute(
            "UPDATE plan_change_requests SET note = NULL WHERE org_id = ?",
            (org_id,),
        )
        self.db.execute(
            "UPDATE deletion_requests SET reason = NULL WHERE org_id = ?",
            (org_id,),
        )

    # -- organisation data export ------------------------------------------------------------
    def export_rows(self, table: str, org_id: str) -> list[dict[str, Any]]:
        """Every row of one tenant table for the owner's data export (secret columns excluded)."""
        sql = _EXPORT_SQL[table]  # KeyError for anything else: table names are internal constants
        rows = self.db.query(sql, (org_id,))
        return [r for r in (_decode(x) for x in rows) if r is not None]

    def export_sessions(self, user_id: str) -> list[dict[str, Any]]:
        """The requesting member's own sessions (other members' devices are not exported)."""
        rows = self.db.query(
            "SELECT id, user_id, created_at, expires_at, last_seen_at, revoked_at, user_agent FROM sessions "
            "WHERE user_id = ? ORDER BY created_at, id",
            (user_id,),
        )
        return [dict(r) for r in rows]

    def export_audit(self, org_id: str) -> list[dict[str, Any]]:
        """Audit events whose actor is a member of the organisation or whose target is the organisation."""
        rows = self.db.query(
            "SELECT * FROM audit_events WHERE target = ? "
            "OR actor IN (SELECT 'user:' || id FROM users WHERE org_id = ?) "
            "ORDER BY created_at, id",
            (org_id, org_id),
        )
        return [r for r in (_decode(x) for x in rows) if r is not None]

    # -- audit retention ---------------------------------------------------------------------
    def count_audit_before(self, cutoff: str) -> int:
        return int(
            self.db.query("SELECT COUNT(*) AS n FROM audit_events WHERE created_at < ?", (cutoff,))[0]["n"]
        )

    def prune_audit_before(self, cutoff: str) -> int:
        return self.db.execute("DELETE FROM audit_events WHERE created_at < ?", (cutoff,))
