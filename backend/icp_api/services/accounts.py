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
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any

from icp_api.entitlements import PLANS, ROLES, Plan, has_role, plan
from icp_api.errors import ApiError
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id, utcnow
from icp_api.storage.sql import IntegrityError

SESSION_TTL = timedelta(days=30)
API_KEY_PREFIX = "icp_live_"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Password hashes are versioned: ``scrypt$ln=15,r=8,p=1$<salt b64>$<digest b64>`` records the cost
# parameters, so they can be raised later without breaking stored hashes. The legacy format
# ``scrypt$<salt>$<digest>`` (before 2026-10-20) used ln=15, r=8, p=1; it keeps verifying and is
# rewritten in the current format on the next successful sign-in (``needs_rehash``).
SCRYPT_LN, SCRYPT_R, SCRYPT_P = 15, 8, 1  # n = 2^15 -> 32 MiB per hash
_LEGACY_PARAMS = (15, 8, 1)
_MAX_LN = 20  # refuse absurd stored parameters (memory) instead of trying them
_DKLEN = 32


def _scrypt(password: str, salt: bytes, ln: int, r: int, p: int) -> bytes:
    n = 2**ln
    # OpenSSL's default memory cap (32 MiB) equals n=2^15, r=8; allow the exact need plus headroom.
    maxmem = 128 * r * n * p + 32 * 1024 * 1024
    return hashlib.scrypt(password.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=_DKLEN, maxmem=maxmem)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = _scrypt(password, salt, SCRYPT_LN, SCRYPT_R, SCRYPT_P)
    params = f"ln={SCRYPT_LN},r={SCRYPT_R},p={SCRYPT_P}"
    return f"scrypt${params}$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()


def _parse_hash(stored: str) -> tuple[tuple[int, int, int], bytes, bytes] | None:
    """(ln, r, p), salt, digest of a stored hash, or None when it is not a usable scrypt hash."""
    try:
        parts = stored.split("$")
        if parts[0] != "scrypt":
            return None
        if len(parts) == 3:  # legacy, parameters implied
            params = _LEGACY_PARAMS
            salt_b64, digest_b64 = parts[1], parts[2]
        elif len(parts) == 4:
            fields = dict(item.split("=", 1) for item in parts[1].split(","))
            params = (int(fields["ln"]), int(fields["r"]), int(fields["p"]))
            salt_b64, digest_b64 = parts[2], parts[3]
        else:
            return None
        if not (1 <= params[0] <= _MAX_LN and 1 <= params[1] <= 32 and 1 <= params[2] <= 4):
            return None
        return params, base64.b64decode(salt_b64, validate=True), base64.b64decode(digest_b64, validate=True)
    except (ValueError, TypeError, KeyError):
        return None


def verify_password(password: str, stored: str) -> bool:
    parsed = _parse_hash(stored)
    if parsed is None:
        return False
    (ln, r, p), salt, digest = parsed
    candidate = _scrypt(password, salt, ln, r, p)
    return hmac.compare_digest(candidate, digest)


def needs_rehash(stored: str) -> bool:
    """True for legacy-format hashes and hashes with parameters other than the current ones."""
    parsed = _parse_hash(stored)
    return parsed is None or stored.count("$") != 3 or parsed[0] != (SCRYPT_LN, SCRYPT_R, SCRYPT_P)


# Verified against when the email is unknown, so both paths cost one scrypt (computed once).
_DUMMY_HASH = hash_password("dummy-password-x")


def normalize_email(email: str) -> str:
    """Canonical form used for storage, look-up and uniqueness: NFKC (full-width and compatibility
    forms fold to their ASCII equivalents, e.g. U+FF54 -> "t"), case-folded, trimmed. For ASCII
    addresses this equals the previous ``lower()``, so existing rows keep matching."""
    return unicodedata.normalize("NFKC", email).strip().casefold()


def _equalise_registration_cost(password: str) -> None:
    """A successful registration hashes the password and signs in (two scrypt runs); a refused one
    for a taken address costs the same, so timing does not reveal registered addresses."""
    verify_password(password, _DUMMY_HASH)
    verify_password(password, _DUMMY_HASH)


# Touch session/API-key "last seen" at most this often: each touch is a metadata write (D1 free
# tier: 100k writes/day), and per-request precision is not needed.
TOUCH_INTERVAL = timedelta(minutes=5)


def _stale(stamp: str | None) -> bool:
    if not stamp:
        return True
    cutoff = (datetime.now(timezone.utc) - TOUCH_INTERVAL).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return stamp < cutoff


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
        "temporary_login_expires_at": u.get("temp_password_expires_at")
        if u.get("must_change_password")
        else None,
        "email_verified": bool(u.get("email_verified_at")),
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


OPERATOR_ACTOR = "platform-operator"
# Operator-only detail keys in audit records (internal reasons/notes for plan or status changes).
_OPERATOR_DETAIL_KEYS = frozenset({"reason", "note"})


def customer_actor(actor: str | None) -> str | None:
    """Actor label as shown to a customer: members and API keys keep their id; platform operators
    are shown as one role (their names are internal)."""
    if actor is None or actor.startswith(("user:", "apikey:")) or actor == "unknown":
        return actor
    return OPERATOR_ACTOR


def customer_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    """An audit record as included in a customer's data export: operator names and operator-only
    details (internal reasons/notes) removed; members' own records unchanged."""
    if customer_actor(row.get("actor")) != OPERATOR_ACTOR:
        return row
    details = row.get("details") if isinstance(row.get("details"), dict) else None
    out = {**row, "actor": OPERATOR_ACTOR}
    if details is not None:
        out["details"] = {k: v for k, v in details.items() if k not in _OPERATOR_DETAIL_KEYS}
    return out


def temp_password_expired_error() -> ApiError:
    return ApiError(
        403,
        "TEMP_PASSWORD_EXPIRED",
        "Kata sandi sementara sudah kedaluwarsa. Minta administrator organisasi Anda mengatur ulang kata sandi.",
    )


def _require_owner_for_admin_target(actor: dict[str, Any], target: dict[str, Any]) -> None:
    """Admins manage analysts and viewers; only the owner may reset, demote or disable an admin
    (otherwise one admin could take over or remove another, although only the owner grants admin)."""
    if target["role"] == "admin" and not has_role(actor["role"], "owner"):
        raise ApiError(
            403, "FORBIDDEN", "Hanya pemilik organisasi yang dapat mengelola akun administrator lain."
        )


def _stamp(moment: datetime) -> str:
    """The storage timestamp format (``utcnow``), so stored values compare as strings."""
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


_BROWSERS = (
    ("Edg/", "Edge"),
    ("OPR/", "Opera"),
    ("Firefox/", "Firefox"),
    ("Chrome/", "Chrome"),
    ("Safari/", "Safari"),
)
_SYSTEMS = (
    ("Windows", "Windows"),
    ("Android", "Android"),
    ("iPhone", "iOS"),
    ("iPad", "iPadOS"),
    ("Mac OS X", "macOS"),
    ("Macintosh", "macOS"),
    ("CrOS", "ChromeOS"),
    ("Linux", "Linux"),
)


def device_label(user_agent: str | None) -> str:
    """Short, non-identifying device description for the session list (never the raw user agent)."""
    ua = user_agent or ""
    if not ua:
        return "Perangkat tidak dikenal"
    lowered = ua.lower()
    if any(
        k in lowered
        for k in ("python", "httpx", "curl", "wget", "okhttp", "node-fetch", "postman", "testclient")
    ):
        return "Skrip atau klien API"
    browser = next((name for key, name in _BROWSERS if key in ua), "Peramban")
    system = next((name for key, name in _SYSTEMS if key in ua), None)
    return f"{browser} di {system}" if system else browser


class AccountService:
    """Accounts and sessions. ``idle_timeout`` and ``temp_password_ttl`` are set from the settings by
    ``create_app`` (SESSION_IDLE_TIMEOUT_HOURS, TEMP_PASSWORD_TTL_HOURS); the defaults equal theirs."""

    def __init__(self, repo: ProductRepository):
        self.repo = repo
        self.idle_timeout: timedelta | None = timedelta(hours=168)
        self.temp_password_ttl = timedelta(hours=72)

    def configure(self, idle_timeout_hours: int, temp_password_ttl_hours: int) -> None:
        self.idle_timeout = timedelta(hours=idle_timeout_hours) if idle_timeout_hours > 0 else None
        self.temp_password_ttl = timedelta(hours=temp_password_ttl_hours)

    def _temp_expiry(self) -> str:
        return _stamp(datetime.now(timezone.utc) + self.temp_password_ttl)

    @staticmethod
    def temp_password_expired(row: dict[str, Any]) -> bool:
        """A temporary password (must change, with an expiry) whose expiry has passed."""
        expires = row.get("temp_password_expires_at")
        return bool(row.get("must_change_password")) and bool(expires) and str(expires) <= utcnow()

    def _idle_expired(self, row: dict[str, Any]) -> bool:
        if self.idle_timeout is None:
            return False
        last = row.get("last_seen_at") or row.get("created_at")
        return bool(last) and str(last) < _stamp(datetime.now(timezone.utc) - self.idle_timeout)

    # -- registration & login ----------------------------------------------------------------
    def register(
        self, org_name: str, email: str, name: str, password: str, request_id: str | None
    ) -> dict[str, Any]:
        email = normalize_email(email)
        if not EMAIL_RE.match(email):
            raise ApiError(422, "VALIDATION_ERROR", "Alamat email tidak valid.")
        check_password_strength(password)
        taken = ApiError(409, "EMAIL_TAKEN", "Email ini sudah terdaftar. Silakan masuk.")
        if self.repo.get_user_by_email(email):
            _equalise_registration_cost(password)
            raise taken
        password_hash = hash_password(password)
        org_id, user_id = new_id("org"), new_id("usr")
        base = slugify(org_name)
        slug, n = base, 2
        while self.repo.get_organization_by_slug(slug):
            slug = f"{base}-{n}"
            n += 1
        for attempt in range(3):
            try:
                self.repo.insert_organization(
                    {"id": org_id, "name": org_name.strip(), "slug": slug, "plan": "trial"}
                )
                break
            except IntegrityError:  # concurrent registration took the slug: pick a random suffix
                if attempt == 2:
                    raise ApiError(409, "CONFLICT", "Pendaftaran bersamaan; silakan coba lagi.") from None
                slug = f"{base}-{secrets.token_hex(3)}"
        try:
            self.repo.insert_user(
                {
                    "id": user_id,
                    "org_id": org_id,
                    "email": email,
                    "name": name.strip(),
                    "password_hash": password_hash,
                    "role": "owner",
                }
            )
        except IntegrityError as exc:
            # A concurrent registration with the same address won the unique index: remove the
            # organisation created above so no owner-less organisation remains, then answer 409.
            self.repo.delete_empty_organization(org_id)
            raise taken from exc
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
        email = normalize_email(email)
        user = self.repo.get_user_by_email(email)
        # Constant-time-ish: verify against a dummy hash when the user is unknown.
        ok = (
            verify_password(password, user["password_hash"])
            if user
            else verify_password(password, _DUMMY_HASH)
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
        if self.temp_password_expired(user):
            self.repo.audit(
                f"user:{user['id']}",
                "auth.login",
                "denied",
                target=org["id"],
                request_id=request_id,
                details={"reason": "temporary password expired"},
            )
            raise temp_password_expired_error()
        if needs_rehash(user["password_hash"]):
            # legacy or outdated parameters: store the current format now that the password is known
            self.repo.update_user(user["id"], password_hash=hash_password(password))
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
        """Active session for ``token``: not revoked, before its absolute expiry (30 days), used within
        the idle timeout, account and organisation active, temporary password not expired."""
        row = self.repo.get_session_by_hash(token_hash(token))
        if row is None or row.get("revoked_at") or row["expires_at"] <= utcnow():
            return None
        if row["user_status"] != "active" or row["org_status"] != "active":
            return None
        if self._idle_expired(row):
            self.repo.revoke_session(row["id"])  # idle: ended server-side, not only refused
            return None
        if self.temp_password_expired(row):
            return None
        if _stale(row.get("last_seen_at")):
            self.repo.touch_session(row["id"])
        return row

    def resolve_api_key(self, key: str) -> dict[str, Any] | None:
        row = self.repo.get_api_key_by_hash(token_hash(key))
        if row is None or row.get("revoked_at") or row["org_status"] != "active":
            return None
        if not plan(row["plan"]).api_access:
            return None
        if _stale(row.get("last_used_at")):
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
        self.set_password(user_id, new)
        self.repo.audit(f"user:{user_id}", "account.password_change", "success", request_id=request_id)

    def set_password(self, user_id: str, new: str) -> None:
        """Store a new chosen password: clears the temporary-password state and ends every session."""
        self.repo.update_user(
            user_id,
            password_hash=hash_password(new),
            must_change_password=False,
            temp_password_expires_at=None,
        )
        self.repo.revoke_user_sessions(user_id)

    # -- sessions of the signed-in user ------------------------------------------------------
    def list_my_sessions(self, user_id: str, current_session_id: str | None) -> list[dict[str, Any]]:
        """Active sessions (not revoked, not expired, not idle) with a device label; no token data."""
        now = utcnow()
        out = []
        for s in self.repo.list_active_sessions(user_id, now):
            if self._idle_expired(s):
                continue
            last = s.get("last_seen_at") or s["created_at"]
            idle_until = (
                _stamp(
                    datetime.strptime(last[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                    + self.idle_timeout
                )
                if self.idle_timeout is not None
                else None
            )
            out.append(
                {
                    "id": s["id"],
                    "device": device_label(s.get("user_agent")),
                    "created_at": s["created_at"],
                    "last_seen_at": s.get("last_seen_at"),
                    "expires_at": s["expires_at"],
                    "idle_expires_at": idle_until,
                    "current": s["id"] == current_session_id,
                }
            )
        return out

    def revoke_my_session(self, user_id: str, session_id: str, request_id: str | None) -> None:
        if not self.repo.revoke_user_session(user_id, session_id):
            raise ApiError(404, "NOT_FOUND", "Sesi tidak ditemukan atau sudah berakhir.")
        self.repo.audit(
            f"user:{user_id}", "auth.session.revoke", "success", target=session_id, request_id=request_id
        )

    def revoke_my_other_sessions(self, user_id: str, current_session_id: str, request_id: str | None) -> int:
        n = self.repo.revoke_other_sessions(user_id, current_session_id)
        self.repo.audit(
            f"user:{user_id}",
            "auth.session.revoke_others",
            "success",
            request_id=request_id,
            details={"revoked": n},
        )
        return n

    def update_profile(self, user_id: str, name: str) -> dict[str, Any]:
        self.repo.update_user(user_id, name=name.strip())
        return self.me(user_id)

    # -- members -----------------------------------------------------------------------------
    def members(self, org_id: str) -> list[dict[str, Any]]:
        return [public_user(u) for u in self.repo.list_users(org_id)]

    def invite_member(
        self, org_id: str, actor: dict[str, Any], email: str, name: str, role: str, request_id: str | None
    ) -> dict[str, Any]:
        email = normalize_email(email)
        if not EMAIL_RE.match(email):
            raise ApiError(422, "VALIDATION_ERROR", "Alamat email tidak valid.")
        if role not in ROLES or role == "owner":
            raise ApiError(422, "VALIDATION_ERROR", "Peran harus viewer, analyst, atau admin.")
        if role == "admin" and not has_role(actor["role"], "owner"):
            raise ApiError(403, "FORBIDDEN", "Hanya pemilik organisasi yang dapat menambahkan administrator.")
        # One generic answer whether the address belongs to this organisation or to another one, so
        # an inviting admin learns nothing about other tenants' members (no e-mail delivery exists to
        # make the outcome fully indistinguishable from a successful invitation; see SECURITY.md).
        unavailable = ApiError(
            409,
            "EMAIL_UNAVAILABLE",
            "Alamat email ini tidak dapat diundang. Gunakan alamat lain atau minta anggota masuk dengan akunnya.",
        )
        if self.repo.get_user_by_email(email):
            raise unavailable
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
        try:
            self.repo.insert_user(
                {
                    "id": uid,
                    "org_id": org_id,
                    "email": email,
                    "name": name.strip(),
                    "password_hash": hash_password(temp_password),
                    "role": role,
                    "must_change_password": True,
                    "temp_password_expires_at": self._temp_expiry(),
                }
            )
        except IntegrityError as exc:  # concurrent registration/invitation of the same address
            raise unavailable from exc
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
        _require_owner_for_admin_target(actor, target)
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
        if target["id"] != actor["id"]:
            _require_owner_for_admin_target(actor, target)
        temp_password = "Icp-" + secrets.token_urlsafe(9)
        expires = self._temp_expiry()
        self.repo.update_user(
            user_id,
            password_hash=hash_password(temp_password),
            must_change_password=True,
            temp_password_expires_at=expires,
        )
        self.repo.revoke_user_sessions(user_id)
        self.repo.audit(
            f"user:{actor['id']}",
            "org.member.password_reset",
            "success",
            target=user_id,
            request_id=request_id,
        )
        return {"temporary_password": temp_password, "expires_at": expires}

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
        if self.repo.get_organization(org_id) is None:
            raise ApiError(404, "NOT_FOUND", "Organisasi tidak ditemukan.")
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


class PlanRequestService:
    """Plan change requests without payment: customers ask, platform operators decide.

    Approval reuses ``AccountService.set_plan`` (the audited operator path), so the plan change,
    its audit record and any downstream entitlement are identical to a direct operator change."""

    def __init__(self, repo: ProductRepository, accounts: AccountService):
        self.repo = repo
        self.accounts = accounts

    def _shape(self, row: dict[str, Any], customer: bool = False) -> dict[str, Any]:
        org = self.repo.get_organization(row["org_id"])
        user = self.repo.get_user(row["requested_by"])
        out = {
            **row,
            "org_name": org["name"] if org else None,
            "requested_by_name": user["name"] if user else None,
        }
        if customer:
            out["resolved_by"] = customer_actor(row.get("resolved_by"))
        return out

    def create(
        self, org_id: str, user_id: str, requested_plan: str, note: str | None, request_id: str | None
    ) -> dict[str, Any]:
        org = self.repo.get_organization(org_id)
        if org is None:
            raise ApiError(404, "NOT_FOUND", "Organisasi tidak ditemukan.")
        if requested_plan not in PLANS:
            raise ApiError(422, "VALIDATION_ERROR", f"Unknown plan {requested_plan!r}.")
        if requested_plan == org["plan"]:
            raise ApiError(422, "VALIDATION_ERROR", "Organisasi sudah memakai paket ini.")
        if self.repo.pending_plan_request(org_id) is not None:
            raise ApiError(409, "CONFLICT", "Masih ada permintaan perubahan paket yang menunggu keputusan.")
        rid = new_id("pcr")
        try:
            self.repo.insert_plan_request(
                {
                    "id": rid,
                    "org_id": org_id,
                    "requested_by": user_id,
                    "previous_plan": org["plan"],
                    "requested_plan": requested_plan,
                    "note": (note or "").strip() or None,
                }
            )
        except IntegrityError as exc:  # concurrent request: the unique pending index decides
            raise ApiError(
                409, "CONFLICT", "Masih ada permintaan perubahan paket yang menunggu keputusan."
            ) from exc
        self.repo.audit(
            f"user:{user_id}",
            "org.plan_request.create",
            "success",
            target=org_id,
            request_id=request_id,
            details={"request_id": rid, "requested_plan": requested_plan},
        )
        return self.get(rid)

    def get(self, rid: str, org_id: str | None = None) -> dict[str, Any]:
        row = self.repo.get_plan_request(rid)
        if row is None or (org_id is not None and row["org_id"] != org_id):
            raise ApiError(404, "NOT_FOUND", "Permintaan tidak ditemukan.")
        return self._shape(row, customer=org_id is not None)

    def list(self, org_id: str | None, status: str | None, limit: int, cursor: str | None) -> dict[str, Any]:
        page = self.repo.list_plan_requests(org_id, status, limit, cursor)
        customer = org_id is not None
        return {"items": [self._shape(r, customer) for r in page.items], "next_cursor": page.next_cursor}

    def withdraw(self, rid: str, org_id: str, user_id: str, request_id: str | None) -> dict[str, Any]:
        self.get(rid, org_id)
        if not self.repo.resolve_plan_request(rid, "withdrawn", f"user:{user_id}", None):
            raise ApiError(409, "CONFLICT", "Permintaan ini sudah diputuskan atau ditarik.")
        self.repo.audit(
            f"user:{user_id}", "org.plan_request.withdraw", "success", target=org_id, request_id=request_id
        )
        return self.get(rid, org_id)

    def resolve(
        self, rid: str, decision: str, operator: str, note: str | None, request_id: str | None
    ) -> dict[str, Any]:
        row = self.get(rid)
        if row["status"] != "pending":
            raise ApiError(409, "CONFLICT", "Permintaan ini sudah diputuskan atau ditarik.")
        status = "approved" if decision == "approve" else "declined"
        if not self.repo.resolve_plan_request(rid, status, operator, (note or "").strip() or None):
            raise ApiError(409, "CONFLICT", "Permintaan ini sudah diputuskan atau ditarik.")
        if status == "approved":
            self.accounts.set_plan(
                row["org_id"],
                row["requested_plan"],
                operator,
                request_id,
                f"plan request {rid}" + (f": {note.strip()}" if note and note.strip() else ""),
            )
        self.repo.audit(
            operator,
            f"org.plan_request.{status}",
            "success",
            target=row["org_id"],
            request_id=request_id,
            details={"request_id": rid, "requested_plan": row["requested_plan"]},
        )
        return self.get(rid)
