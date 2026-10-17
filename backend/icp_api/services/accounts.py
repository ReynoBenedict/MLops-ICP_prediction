"""Accounts: organisations, users, sessions, API keys, plans and usage.

Security notes
* Passwords are hashed with scrypt (stdlib, n=2^15, r=8, p=1, 16-byte salt); hashes are never
  returned by the API or written to logs.
* Session and API-key tokens are opaque random strings; only their SHA-256 is stored. The clear
  token is shown exactly once (login response / key creation response).
* Self-service registration creates a new organisation on the ``trial`` plan with the registering
  user as ``owner``. Plan changes are an operator action (no payment provider is integrated).
* Every state change is audited with the acting principal; emails are the only personal data kept.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from icp_api.entitlements import PLANS, ROLES, Plan, has_role, plan
from icp_api.errors import ApiError
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id, utcnow

SESSION_TTL = timedelta(days=30)
API_KEY_PREFIX = "icp_live_"
# n=2^15, r=8 needs 32 MiB; OpenSSL's default cap is exactly that, so raise it explicitly.
_SCRYPT = {"n": 2**15, "r": 8, "p": 1, "dklen": 32, "maxmem": 64 * 1024 * 1024}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, **_SCRYPT)
    return "scrypt$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, salt_b64, digest_b64 = stored.split("$")
        if scheme != "scrypt":
            return False
        salt, digest = base64.b64decode(salt_b64), base64.b64decode(digest_b64)
    except (ValueError, TypeError):
        return False
    candidate = hashlib.scrypt(password.encode("utf-8"), salt=salt, **_SCRYPT)
    return hmac.compare_digest(candidate, digest)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:40]
    return slug or "org"


def check_password_strength(password: str) -> None:
    if len(password) < 10:
        raise ApiError(422, "WEAK_PASSWORD", "Kata sandi minimal 10 karakter.")
    if password.lower() in {"password123", "icp-intelligence", "1234567890"}:
        raise ApiError(422, "WEAK_PASSWORD", "Kata sandi terlalu umum.")


def public_user(u: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": u["id"],
        "org_id": u["org_id"],
        "email": u["email"],
        "name": u["name"],
        "role": u["role"],
        "status": u["status"],
        "must_change_password": bool(u.get("must_change_password")),
        "created_at": u["created_at"],
        "last_login_at": u.get("last_login_at"),
    }


def public_org(o: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": o["id"],
        "name": o["name"],
        "slug": o["slug"],
        "plan": o["plan"],
        "status": o["status"],
        "created_at": o["created_at"],
        "entitlements": plan(o["plan"]).to_dict(),
    }


class AccountService:
    def __init__(self, repo: ProductRepository):
        self.repo = repo

    # -- registration & login ----------------------------------------------------------------
    def register(
        self, org_name: str, email: str, name: str, password: str, request_id: str | None
    ) -> dict[str, Any]:
        email = email.strip().lower()
        if not EMAIL_RE.match(email):
            raise ApiError(422, "VALIDATION_ERROR", "Alamat email tidak valid.")
        check_password_strength(password)
        if self.repo.get_user_by_email(email):
            raise ApiError(409, "EMAIL_TAKEN", "Email ini sudah terdaftar. Silakan masuk.")
        base = slugify(org_name)
        slug = base
        n = 2
        while self.repo.get_organization_by_slug(slug):
            slug = f"{base}-{n}"
            n += 1
        org_id, user_id = new_id("org"), new_id("usr")
        self.repo.insert_organization({"id": org_id, "name": org_name.strip(), "slug": slug, "plan": "trial"})
        self.repo.insert_user(
            {
                "id": user_id,
                "org_id": org_id,
                "email": email,
                "name": name.strip(),
                "password_hash": hash_password(password),
                "role": "owner",
            }
        )
        self.repo.audit(
            f"user:{user_id}",
            "account.register",
            "success",
            target=org_id,
            request_id=request_id,
            details={"plan": "trial"},
        )
        return self.login(email, password, request_id, user_agent=None)

    def login(
        self, email: str, password: str, request_id: str | None, user_agent: str | None
    ) -> dict[str, Any]:
        email = email.strip().lower()
        user = self.repo.get_user_by_email(email)
        # Constant-time-ish: verify against a dummy hash when the user is unknown.
        ok = (
            verify_password(password, user["password_hash"])
            if user
            else verify_password(password, hash_password("dummy-password-x"))
        )
        if not user or not ok:
            self.repo.audit(
                "unknown", "auth.login", "denied", target=email[:3] + "***", request_id=request_id
            )
            raise ApiError(401, "AUTH_INVALID", "Email atau kata sandi salah.")
        if user["status"] != "active":
            raise ApiError(
                403, "ACCOUNT_DISABLED", "Akun ini dinonaktifkan. Hubungi administrator organisasi Anda."
            )
        org = self.repo.get_organization(user["org_id"])
        if org is None or org["status"] != "active":
            raise ApiError(403, "ORG_SUSPENDED", "Organisasi ini ditangguhkan.")
        token = "icp_sess_" + secrets.token_urlsafe(32)
        expires = (datetime.now(timezone.utc) + SESSION_TTL).strftime("%Y-%m-%dT%H:%M:%SZ")
        sid = new_id("ses")
        self.repo.insert_session(
            {
                "id": sid,
                "user_id": user["id"],
                "token_hash": token_hash(token),
                "expires_at": expires,
                "user_agent": (user_agent or "")[:200] or None,
            }
        )
        self.repo.update_user(user["id"], last_login_at=utcnow())
        self.repo.audit(
            f"user:{user['id']}", "auth.login", "success", target=org["id"], request_id=request_id
        )
        return {
            "token": token,
            "expires_at": expires,
            "user": public_user({**user, "last_login_at": utcnow()}),
            "organization": public_org(org),
        }

    def logout(self, session_id: str, user_id: str, request_id: str | None) -> None:
        self.repo.revoke_session(session_id)
        self.repo.audit(f"user:{user_id}", "auth.logout", "success", request_id=request_id)

    def resolve_session(self, token: str) -> dict[str, Any] | None:
        row = self.repo.get_session_by_hash(token_hash(token))
        if row is None or row.get("revoked_at") or row["expires_at"] <= utcnow():
            return None
        if row["user_status"] != "active" or row["org_status"] != "active":
            return None
        self.repo.touch_session(row["id"])
        return row

    def resolve_api_key(self, key: str) -> dict[str, Any] | None:
        row = self.repo.get_api_key_by_hash(token_hash(key))
        if row is None or row.get("revoked_at") or row["org_status"] != "active":
            return None
        if not plan(row["plan"]).api_access:
            return None
        self.repo.touch_api_key(row["id"])
        return row

    # -- profile -----------------------------------------------------------------------------
    def me(self, user_id: str) -> dict[str, Any]:
        user = self.repo.get_user(user_id)
        org = self.repo.get_organization(user["org_id"]) if user else None
        if user is None or org is None:
            raise ApiError(404, "NOT_FOUND", "Pengguna tidak ditemukan.")
        return {
            "user": public_user(user),
            "organization": public_org(org),
            "sessions": self.repo.list_sessions(user_id),
        }

    def change_password(self, user_id: str, current: str, new: str, request_id: str | None) -> None:
        user = self.repo.get_user(user_id)
        if user is None or not verify_password(current, user["password_hash"]):
            raise ApiError(401, "AUTH_INVALID", "Kata sandi saat ini salah.")
        check_password_strength(new)
        self.repo.update_user(user_id, password_hash=hash_password(new), must_change_password=False)
        self.repo.revoke_user_sessions(user_id)
        self.repo.audit(f"user:{user_id}", "account.password_change", "success", request_id=request_id)

    def update_profile(self, user_id: str, name: str) -> dict[str, Any]:
        self.repo.update_user(user_id, name=name.strip())
        return self.me(user_id)

    # -- members -----------------------------------------------------------------------------
    def members(self, org_id: str) -> list[dict[str, Any]]:
        return [public_user(u) for u in self.repo.list_users(org_id)]

    def invite_member(
        self, org_id: str, actor: dict[str, Any], email: str, name: str, role: str, request_id: str | None
    ) -> dict[str, Any]:
        email = email.strip().lower()
        if not EMAIL_RE.match(email):
            raise ApiError(422, "VALIDATION_ERROR", "Alamat email tidak valid.")
        if role not in ROLES or role == "owner":
            raise ApiError(422, "VALIDATION_ERROR", "Peran harus viewer, analyst, atau admin.")
        if role == "admin" and not has_role(actor["role"], "owner"):
            raise ApiError(403, "FORBIDDEN", "Hanya pemilik organisasi yang dapat menambahkan administrator.")
        if self.repo.get_user_by_email(email):
            raise ApiError(409, "EMAIL_TAKEN", "Email ini sudah terdaftar.")
        org = self.repo.get_organization(org_id)
        p = plan(org["plan"] if org else None)
        if self.repo.count_users(org_id) >= p.seats:
            raise ApiError(
                403,
                "SEAT_LIMIT",
                f"Paket {p.label_id} mencakup {p.seats} kursi. Tingkatkan paket untuk menambah anggota.",
                {"limit": p.seats, "plan": p.code},
            )
        temp_password = "Icp-" + secrets.token_urlsafe(9)
        uid = new_id("usr")
        self.repo.insert_user(
            {
                "id": uid,
                "org_id": org_id,
                "email": email,
                "name": name.strip(),
                "password_hash": hash_password(temp_password),
                "role": role,
                "must_change_password": True,
            }
        )
        self.repo.audit(
            f"user:{actor['id']}",
            "org.member.invite",
            "success",
            target=uid,
            request_id=request_id,
            details={"role": role},
        )
        # No email delivery is configured: the inviter hands over the one-time password out of band.
        return {
            "member": public_user(self.repo.get_user(uid) or {}),
            "temporary_password": temp_password,
            "note": "Sampaikan kata sandi sementara ini secara langsung; pengguna wajib menggantinya saat pertama masuk.",
        }

    def update_member(
        self,
        org_id: str,
        actor: dict[str, Any],
        user_id: str,
        role: str | None,
        status: str | None,
        request_id: str | None,
    ) -> dict[str, Any]:
        target = self.repo.get_user(user_id)
        if target is None or target["org_id"] != org_id:
            raise ApiError(404, "NOT_FOUND", "Anggota tidak ditemukan.")
        if target["id"] == actor["id"]:
            raise ApiError(409, "SELF_EDIT", "Anda tidak dapat mengubah peran atau status akun sendiri.")
        if target["role"] == "owner":
            raise ApiError(403, "FORBIDDEN", "Peran pemilik tidak dapat diubah melalui API ini.")
        fields: dict[str, Any] = {}
        if role is not None:
            if role not in ROLES or role == "owner":
                raise ApiError(422, "VALIDATION_ERROR", "Peran tidak valid.")
            if role == "admin" and not has_role(actor["role"], "owner"):
                raise ApiError(403, "FORBIDDEN", "Hanya pemilik yang dapat memberikan peran admin.")
            fields["role"] = role
        if status is not None:
            if status not in ("active", "disabled"):
                raise ApiError(422, "VALIDATION_ERROR", "Status tidak valid.")
            fields["status"] = status
            if status == "disabled":
                self.repo.revoke_user_sessions(user_id)
        if fields:
            self.repo.update_user(user_id, **fields)
        self.repo.audit(
            f"user:{actor['id']}",
            "org.member.update",
            "success",
            target=user_id,
            request_id=request_id,
            details=fields,
        )
        return public_user(self.repo.get_user(user_id) or {})

    def reset_member_password(
        self, org_id: str, actor: dict[str, Any], user_id: str, request_id: str | None
    ) -> dict[str, Any]:
        target = self.repo.get_user(user_id)
        if target is None or target["org_id"] != org_id:
            raise ApiError(404, "NOT_FOUND", "Anggota tidak ditemukan.")
        if target["role"] == "owner" and target["id"] != actor["id"]:
            raise ApiError(
                403, "FORBIDDEN", "Kata sandi pemilik hanya dapat diatur ulang oleh pemilik sendiri."
            )
        temp_password = "Icp-" + secrets.token_urlsafe(9)
        self.repo.update_user(user_id, password_hash=hash_password(temp_password), must_change_password=True)
        self.repo.revoke_user_sessions(user_id)
        self.repo.audit(
            f"user:{actor['id']}",
            "org.member.password_reset",
            "success",
            target=user_id,
            request_id=request_id,
        )
        return {"temporary_password": temp_password}

    # -- organisation & plan -----------------------------------------------------------------
    def organization(self, org_id: str) -> dict[str, Any]:
        org = self.repo.get_organization(org_id)
        if org is None:
            raise ApiError(404, "NOT_FOUND", "Organisasi tidak ditemukan.")
        since = (
            datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            **public_org(org),
            "members": self.repo.count_users(org_id, active_only=False),
            "active_members": self.repo.count_users(org_id),
            "saved_scenarios": self.repo.count_scenarios(org_id),
            "alert_rules": self.repo.count_alert_rules(org_id),
            "usage_this_month": self.repo.usage_summary(org_id, since),
            "plans": [p.to_dict() for p in PLANS.values()],
        }

    def rename_organization(
        self, org_id: str, actor: dict[str, Any], name: str, request_id: str | None
    ) -> dict[str, Any]:
        self.repo.update_organization(org_id, name=name.strip())
        self.repo.audit(f"user:{actor['id']}", "org.rename", "success", target=org_id, request_id=request_id)
        return self.organization(org_id)

    def set_plan(
        self, org_id: str, plan_code: str, actor: str, request_id: str | None, reason: str
    ) -> dict[str, Any]:
        """Operator-only until a billing provider is integrated (see docs/COMMERCIAL_READINESS.md)."""
        if plan_code not in PLANS:
            raise ApiError(422, "VALIDATION_ERROR", f"Unknown plan {plan_code!r}.")
        if self.repo.get_organization(org_id) is None:
            raise ApiError(404, "NOT_FOUND", "Organisasi tidak ditemukan.")
        self.repo.update_organization(org_id, plan=plan_code)
        self.repo.audit(
            actor,
            "org.plan.set",
            "success",
            target=org_id,
            request_id=request_id,
            details={"plan": plan_code, "reason": reason},
        )
        return self.organization(org_id)

    def set_status(
        self, org_id: str, status: str, actor: str, request_id: str | None, reason: str
    ) -> dict[str, Any]:
        if status not in ("active", "suspended"):
            raise ApiError(422, "VALIDATION_ERROR", "status must be active or suspended")
        self.repo.update_organization(org_id, status=status)
        self.repo.audit(
            actor,
            "org.status.set",
            "success",
            target=org_id,
            request_id=request_id,
            details={"status": status, "reason": reason},
        )
        return self.organization(org_id)

    # -- API keys ----------------------------------------------------------------------------
    def create_api_key(
        self, org_id: str, actor: dict[str, Any], name: str, request_id: str | None
    ) -> dict[str, Any]:
        org = self.repo.get_organization(org_id)
        p: Plan = plan(org["plan"] if org else None)
        if not p.api_access:
            raise ApiError(
                403,
                "FEATURE_NOT_IN_PLAN",
                f"Akses API tidak termasuk dalam paket {p.label_id}.",
                {"feature": "api_access", "plan": p.code},
            )
        key = API_KEY_PREFIX + secrets.token_urlsafe(32)
        kid = new_id("key")
        self.repo.insert_api_key(
            {
                "id": kid,
                "org_id": org_id,
                "name": name.strip()[:80],
                "prefix": key[:12],
                "key_hash": token_hash(key),
                "created_by": actor["id"],
            }
        )
        self.repo.audit(
            f"user:{actor['id']}", "org.api_key.create", "success", target=kid, request_id=request_id
        )
        return {
            "id": kid,
            "name": name.strip()[:80],
            "prefix": key[:12],
            "key": key,
            "note": "Simpan kunci ini sekarang; tidak akan ditampilkan lagi.",
        }

    def list_api_keys(self, org_id: str) -> list[dict[str, Any]]:
        return self.repo.list_api_keys(org_id)

    def revoke_api_key(self, org_id: str, actor: dict[str, Any], key_id: str, request_id: str | None) -> None:
        if not self.repo.revoke_api_key(org_id, key_id):
            raise ApiError(404, "NOT_FOUND", "Kunci API tidak ditemukan.")
        self.repo.audit(
            f"user:{actor['id']}", "org.api_key.revoke", "success", target=key_id, request_id=request_id
        )
