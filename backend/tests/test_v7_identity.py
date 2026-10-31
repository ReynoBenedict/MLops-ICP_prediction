"""Identity lifecycle (audit v7 Wave B): session idle timeout and session management, password-hash
versioning with rehash on sign-in, expiring temporary passwords, the disabled-by-default e-mail token
service, and migration 0007 on a populated database."""

from __future__ import annotations

import base64
import hashlib
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from icp_api.services.accounts import hash_password, needs_rehash, verify_password
from icp_api.services.email_tokens import EmailMessage, EmailTokenService
from icp_api.storage.migrations import current_version, discover
from icp_api.storage.sql import IntegrityError, SqliteExecutor

API = "/api/v1"
PW = "Identitas-v7-Uji-2026!"
NEW_PW = "Sandi-Baru-v7-2026!!"
CHROME_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36"
FIREFOX_LINUX = "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0"


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _register(client: Any, slug: str) -> dict[str, Any]:
    email = f"owner@{slug}.identitas.test"
    r = client.post(
        f"{API}/auth/register",
        json={
            "organization_name": f"PT {slug}",
            "email": email,
            "name": "O",
            "password": PW,
            "accept_terms": True,
        },
    )
    assert r.status_code == 201, r.text
    b = r.json()
    return {
        "h": {"Authorization": f"Bearer {b['token']}"},
        "org": b["organization"]["id"],
        "user": b["user"]["id"],
        "email": email,
    }


def _login(client: Any, email: str, password: str = PW, ua: str | None = None) -> dict[str, str]:
    headers = {"User-Agent": ua} if ua else {}
    r = client.post(f"{API}/auth/login", json={"email": email, "password": password}, headers=headers)
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _db(client: Any) -> Any:
    return client.app.state.container.repo.db


# -- 1. sessions -----------------------------------------------------------------------------------
def test_idle_session_is_ended_server_side_and_absolute_expiry_still_applies(client):
    o = _register(client, "idle")
    accounts = client.app.state.container.accounts
    assert accounts.idle_timeout == timedelta(hours=168)  # SESSION_IDLE_TIMEOUT_HOURS default
    h = _login(client, o["email"])
    sid = next(s for s in client.get(f"{API}/auth/sessions", headers=h).json() if s["current"])["id"]
    old = _stamp(datetime.now(timezone.utc) - timedelta(hours=169))
    _db(client).execute("UPDATE sessions SET last_seen_at = ? WHERE id = ?", (old, sid))
    assert client.get(f"{API}/auth/whoami", headers=h).status_code == 401
    row = _db(client).query("SELECT revoked_at FROM sessions WHERE id = ?", (sid,))[0]
    assert row["revoked_at"] is not None  # ended, not merely refused
    # a session used recently but past its absolute expiry is refused too
    h2 = _login(client, o["email"])
    sid2 = next(s for s in client.get(f"{API}/auth/sessions", headers=h2).json() if s["current"])["id"]
    _db(client).execute("UPDATE sessions SET expires_at = ? WHERE id = ?", ("2020-01-01T00:00:00Z", sid2))
    assert client.get(f"{API}/auth/whoami", headers=h2).status_code == 401


def test_idle_timeout_can_be_disabled(client, monkeypatch):
    o = _register(client, "idle-off")
    accounts = client.app.state.container.accounts
    monkeypatch.setattr(accounts, "idle_timeout", None)
    h = _login(client, o["email"])
    _db(client).execute(
        "UPDATE sessions SET last_seen_at = ? WHERE user_id = ?", ("2025-01-01T00:00:00.000Z", o["user"])
    )
    assert client.get(f"{API}/auth/whoami", headers=h).status_code == 200


def test_session_list_revoke_one_and_sign_out_everywhere_else(client, customer):
    o = _register(client, "sesi")
    a = _login(client, o["email"], ua=CHROME_WIN)
    b = _login(client, o["email"], ua=FIREFOX_LINUX)
    c = _login(client, o["email"])
    sessions = client.get(f"{API}/auth/sessions", headers=a).json()
    assert len(sessions) == 4  # registration + 3 sign-ins
    assert sum(s["current"] for s in sessions) == 1
    current = next(s for s in sessions if s["current"])
    assert current["device"] == "Chrome di Windows"
    assert {"Firefox di Linux", "Skrip atau klien API"} <= {s["device"] for s in sessions}
    for s in sessions:
        assert set(s) == {
            "id",
            "device",
            "created_at",
            "last_seen_at",
            "expires_at",
            "idle_expires_at",
            "current",
        }
    blob = str(sessions)
    assert "icp_sess_" not in blob and "Mozilla" not in blob and "token" not in blob
    # revoke one (the Firefox one) from the Chrome session
    ff = next(s for s in sessions if s["device"] == "Firefox di Linux")
    assert client.post(f"{API}/auth/sessions/{ff['id']}/revoke", headers=a).status_code == 204
    assert client.get(f"{API}/auth/whoami", headers=b).status_code == 401
    assert client.post(f"{API}/auth/sessions/{ff['id']}/revoke", headers=a).status_code == 404
    # someone else's session id is not found
    other_sid = client.get(f"{API}/auth/sessions", headers=customer).json()[0]["id"]
    assert client.post(f"{API}/auth/sessions/{other_sid}/revoke", headers=a).status_code == 404
    assert client.get(f"{API}/auth/whoami", headers=customer).status_code == 200
    # sign out everywhere else
    r = client.post(f"{API}/auth/sessions/revoke-others", headers=a)
    assert r.status_code == 200 and r.json() == {"revoked": 2}
    assert client.get(f"{API}/auth/whoami", headers=c).status_code == 401
    assert client.get(f"{API}/auth/whoami", headers=o["h"]).status_code == 401
    assert client.get(f"{API}/auth/whoami", headers=a).status_code == 200
    assert [s["current"] for s in client.get(f"{API}/auth/sessions", headers=a).json()] == [True]


def test_session_endpoints_need_a_signed_in_session(client):
    assert client.get(f"{API}/auth/sessions").status_code == 401
    admin = {"Authorization": f"Bearer {client.app.state.container.settings.admin_tokens['tester']}"}
    assert client.get(f"{API}/auth/sessions", headers=admin).status_code == 403
    o = _register(client, "sesi-key")
    client.post(
        f"{API}/admin/organizations/{o['org']}/plan",
        json={"plan": "professional", "reason": "uji sesi kunci"},
        headers=admin,
    )
    key = client.post(f"{API}/org/api-keys", json={"name": "k"}, headers=o["h"]).json()["key"]
    assert client.get(f"{API}/auth/sessions", headers={"Authorization": f"Bearer {key}"}).status_code == 403


# -- 2. password hash versioning -------------------------------------------------------------------
def _legacy_hash(password: str, n: int = 2**15) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=n, r=8, p=1, dklen=32, maxmem=64 * 1024 * 1024)
    return "scrypt$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()


def test_hash_format_records_parameters_and_legacy_hashes_still_verify():
    h = hash_password("Contoh-Sandi-123")
    assert h.startswith("scrypt$ln=15,r=8,p=1$") and h.count("$") == 3
    assert verify_password("Contoh-Sandi-123", h) and not verify_password("salah", h)
    assert not needs_rehash(h)
    legacy = _legacy_hash("Contoh-Sandi-123")
    assert verify_password("Contoh-Sandi-123", legacy) and not verify_password("salah", legacy)
    assert needs_rehash(legacy)
    weaker = hash_password("x" * 12).replace("ln=15", "ln=14")  # a different parameter set
    assert needs_rehash(weaker)
    for broken in (
        "",
        "scrypt$",
        "bcrypt$x$y",
        "deleted$abc",
        "scrypt$ln=99,r=8,p=1$AAAA$AAAA",
        "scrypt$ln=x$a$b",
    ):
        assert verify_password("x", broken) is False


def test_sign_in_with_a_legacy_hash_works_and_upgrades_it(client):
    o = _register(client, "legacy-hash")
    repo = client.app.state.container.repo
    repo.update_user(o["user"], password_hash=_legacy_hash(PW))
    _login(client, o["email"])
    stored = repo.get_user(o["user"])["password_hash"]
    assert stored.startswith("scrypt$ln=15,r=8,p=1$") and verify_password(PW, stored)
    _login(client, o["email"])  # and keeps working afterwards
    # an older parameter set (ln=14) is upgraded the same way
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(PW.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    repo.update_user(
        o["user"],
        password_hash="scrypt$ln=14,r=8,p=1$"
        + base64.b64encode(salt).decode()
        + "$"
        + base64.b64encode(digest).decode(),
    )
    _login(client, o["email"])
    assert repo.get_user(o["user"])["password_hash"].startswith("scrypt$ln=15,")


# -- 3. temporary passwords ------------------------------------------------------------------------
def test_temporary_passwords_expire_and_force_a_change(client):
    o = _register(client, "sementara")
    repo = client.app.state.container.repo
    inv = client.post(
        f"{API}/org/members",
        json={"email": "m@sementara.identitas.test", "name": "M", "role": "viewer"},
        headers=o["h"],
    ).json()
    member = inv["member"]
    assert member["must_change_password"] and member["temporary_login_expires_at"]
    expires = datetime.strptime(member["temporary_login_expires_at"][:19], "%Y-%m-%dT%H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    assert (
        timedelta(hours=71) < expires - datetime.now(timezone.utc) <= timedelta(hours=72)
    )  # TTL default 72 h
    # signed in with the temporary password: only the password change is allowed (existing rule)
    tmp_h = _login(client, member["email"], inv["temporary_password"])
    assert (
        client.get(f"{API}/forecasts/latest", headers=tmp_h).json()["error"]["code"]
        == "PASSWORD_CHANGE_REQUIRED"
    )
    # once expired: the session made with it ends and the password no longer signs in
    repo.update_user(member["id"], temp_password_expires_at="2020-01-01T00:00:00.000Z")
    assert client.get(f"{API}/auth/whoami", headers=tmp_h).status_code == 401
    r = client.post(
        f"{API}/auth/login", json={"email": member["email"], "password": inv["temporary_password"]}
    )
    assert r.status_code == 403 and r.json()["error"]["code"] == "TEMP_PASSWORD_EXPIRED"
    wrong = client.post(f"{API}/auth/login", json={"email": member["email"], "password": "salah-salah-salah"})
    assert wrong.status_code == 401  # the expiry is only revealed with the right password
    # a fresh reset issues a new expiring temporary password; choosing a password clears the state
    reset = client.post(f"{API}/org/members/{member['id']}/reset-password", headers=o["h"]).json()
    assert reset["expires_at"]
    h = _login(client, member["email"], reset["temporary_password"])
    assert (
        client.post(
            f"{API}/auth/password",
            json={"current_password": reset["temporary_password"], "new_password": NEW_PW},
            headers=h,
        ).status_code
        == 204
    )
    row = repo.get_user(member["id"])
    assert row["temp_password_expires_at"] is None and not row["must_change_password"]
    _login(client, member["email"], NEW_PW)


def test_temporary_password_ttl_is_configurable(client, monkeypatch):
    accounts = client.app.state.container.accounts
    monkeypatch.setattr(accounts, "temp_password_ttl", timedelta(hours=2))
    o = _register(client, "ttl")
    inv = client.post(
        f"{API}/org/members",
        json={"email": "t@ttl.identitas.test", "name": "T", "role": "viewer"},
        headers=o["h"],
    ).json()
    expires = datetime.strptime(
        inv["member"]["temporary_login_expires_at"][:19], "%Y-%m-%dT%H:%M:%S"
    ).replace(tzinfo=timezone.utc)
    assert expires - datetime.now(timezone.utc) <= timedelta(hours=2)


# -- 4. e-mail token service -----------------------------------------------------------------------
def _tokens(client: Any) -> int:
    return int(_db(client).query("SELECT COUNT(*) AS n FROM email_tokens")[0]["n"])


def test_email_flows_are_disabled_by_default_and_send_nothing(client):
    assert client.app.state.email_tokens.enabled is False
    o = _register(client, "mail-off")
    inv = client.post(
        f"{API}/org/members",
        json={"email": "i@mail-off.identitas.test", "name": "I", "role": "viewer"},
        headers=o["h"],
    ).json()
    before = _tokens(client)
    calls = [
        ("/auth/password-reset/request", {"email": o["email"]}, {}),
        ("/auth/password-reset/confirm", {"token": "icp_mail_" + "x" * 30, "new_password": NEW_PW}, {}),
        ("/auth/email/verify/request", None, o["h"]),
        ("/auth/email/verify/confirm", {"token": "icp_mail_" + "x" * 30}, {}),
        ("/auth/invite/accept", {"token": "icp_mail_" + "x" * 30, "new_password": NEW_PW}, {}),
        (f"/org/members/{inv['member']['id']}/invitation-email", None, o["h"]),
    ]
    for path, body, headers in calls:
        r = client.post(f"{API}{path}", json=body, headers=headers)
        assert r.status_code == 503, (path, r.text)
        err = r.json()["error"]
        assert err["code"] == "EMAIL_DISABLED" and "belum diaktifkan" in err["message"]
    assert _tokens(client) == before


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[EmailMessage] = []

    def send(self, message: EmailMessage) -> None:
        self.sent.append(message)

    def token(self, index: int = -1) -> str:
        m = re.search(r"token=(icp_mail_[A-Za-z0-9_-]+)", self.sent[index].text)
        assert m
        return m.group(1)


@pytest.fixture
def mail(client: Any, monkeypatch: pytest.MonkeyPatch) -> FakeSender:
    sender = FakeSender()
    service = EmailTokenService(
        client.app.state.container.repo, sender, enabled=True, link_base_url="https://app.example/"
    )
    monkeypatch.setattr(client.app.state, "email_tokens", service)
    return sender


def test_password_reset_tokens_are_single_use_hashed_and_expiring(client, mail):
    o = _register(client, "reset-mail")
    old = _login(client, o["email"])
    r = client.post(f"{API}/auth/password-reset/request", json={"email": o["email"].upper()})
    assert r.status_code == 202
    unknown = client.post(
        f"{API}/auth/password-reset/request", json={"email": "nobody@reset-mail.identitas.test"}
    )
    assert unknown.status_code == 202 and unknown.json() == r.json()  # same answer, nothing sent
    assert len(mail.sent) == 1 and mail.sent[0].to == o["email"]
    assert mail.sent[0].text.count("https://app.example/atur-ulang-sandi?token=icp_mail_") == 1
    token = mail.token()
    stored = _db(client).query("SELECT token_hash FROM email_tokens WHERE user_id = ?", (o["user"],))
    assert stored and all(token not in str(row) for row in stored)  # only the hash is kept
    # a weak password does not spend the token
    weak = client.post(
        f"{API}/auth/password-reset/confirm", json={"token": token, "new_password": "1234567890"}
    )
    assert weak.status_code == 422
    ok = client.post(f"{API}/auth/password-reset/confirm", json={"token": token, "new_password": NEW_PW})
    assert ok.status_code == 204
    assert client.get(f"{API}/auth/whoami", headers=old).status_code == 401  # every session ended
    _login(client, o["email"], NEW_PW)
    again = client.post(f"{API}/auth/password-reset/confirm", json={"token": token, "new_password": PW + "x"})
    assert again.status_code == 400 and again.json()["error"]["code"] == "TOKEN_INVALID"
    # a newer token supersedes the older one; an expired token is refused
    client.post(f"{API}/auth/password-reset/request", json={"email": o["email"]})
    first = mail.token()
    client.post(f"{API}/auth/password-reset/request", json={"email": o["email"]})
    second = mail.token()
    assert (
        client.post(
            f"{API}/auth/password-reset/confirm", json={"token": first, "new_password": PW}
        ).status_code
        == 400
    )
    _db(client).execute(
        "UPDATE email_tokens SET expires_at = '2020-01-01T00:00:00.000Z' WHERE user_id = ?", (o["user"],)
    )
    assert (
        client.post(
            f"{API}/auth/password-reset/confirm", json={"token": second, "new_password": PW}
        ).status_code
        == 400
    )
    # a token of another purpose is not accepted
    client.post(f"{API}/auth/password-reset/request", json={"email": o["email"]})
    assert client.post(f"{API}/auth/email/verify/confirm", json={"token": mail.token()}).status_code == 400


def test_email_verification_and_invitation_delivery(client, mail):
    o = _register(client, "verify-mail")
    h = _login(client, o["email"])
    assert client.post(f"{API}/auth/email/verify/request", headers=h).status_code == 202
    assert client.post(f"{API}/auth/email/verify/confirm", json={"token": mail.token()}).status_code == 204
    me = client.get(f"{API}/auth/me", headers=h).json()["user"]
    assert me["email_verified"] is True
    again = client.post(f"{API}/auth/email/verify/request", headers=h)
    assert again.status_code == 409 and again.json()["error"]["code"] == "EMAIL_ALREADY_VERIFIED"
    # invitation by e-mail instead of handing over the temporary password
    inv = client.post(
        f"{API}/org/members",
        json={"email": "baru@verify-mail.identitas.test", "name": "B", "role": "analyst"},
        headers=h,
    ).json()
    mid = inv["member"]["id"]
    assert client.post(f"{API}/org/members/{mid}/invitation-email", headers=h).status_code == 202
    assert (
        mail.sent[-1].to == "baru@verify-mail.identitas.test"
        and "/terima-undangan?token=" in mail.sent[-1].text
    )
    accept = client.post(f"{API}/auth/invite/accept", json={"token": mail.token(), "new_password": NEW_PW})
    assert accept.status_code == 204
    member = client.app.state.container.repo.get_user(mid)
    assert (
        not member["must_change_password"]
        and member["email_verified_at"]
        and member["temp_password_expires_at"] is None
    )
    _login(client, "baru@verify-mail.identitas.test", NEW_PW)
    done = client.post(f"{API}/org/members/{mid}/invitation-email", headers=h)
    assert done.status_code == 409 and done.json()["error"]["code"] == "MEMBER_ALREADY_ACTIVE"


def test_settings_refuse_email_without_a_provider(monkeypatch):
    from icp_api.settings import ConfigError, load_settings

    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("EMAIL_ENABLED", "true")
    with pytest.raises(ConfigError, match="EMAIL_ENABLED"):
        load_settings()
    monkeypatch.setenv("EMAIL_ENABLED", "false")
    monkeypatch.setenv("SESSION_IDLE_TIMEOUT_HOURS", "-1")
    with pytest.raises(ConfigError, match="SESSION_IDLE_TIMEOUT_HOURS"):
        load_settings()


# -- 5. migration 0007 -----------------------------------------------------------------------------
def test_migration_0007_is_additive_and_keeps_every_account_row(tmp_path):
    db = SqliteExecutor(tmp_path / "v6.db")
    migs = discover()
    assert migs[6].name == "0007_identity_lifecycle"
    for mig in migs[:6]:
        db.executescript(mig.path.read_text(encoding="utf-8"))
    assert current_version(db) == 6
    legacy = _legacy_hash(PW)
    db.execute(
        "INSERT INTO organizations (id, name, slug, plan, status, created_at, updated_at) "
        "VALUES ('org_a', 'A', 'a', 'trial', 'active', '2026-01-01', '2026-01-01')"
    )
    db.execute(
        "INSERT INTO users (id, org_id, email, name, password_hash, role, status, must_change_password, created_at) "
        "VALUES ('usr_a', 'org_a', 'a@x.test', 'A', ?, 'owner', 'active', 1, '2026-01-01')",
        (legacy,),
    )
    db.execute(
        "INSERT INTO sessions (id, user_id, token_hash, created_at, expires_at, last_seen_at) "
        "VALUES ('ses_a', 'usr_a', 'h', '2026-01-01', '2026-02-01', '2026-01-02')"
    )
    users_before = db.query("SELECT * FROM users")
    sessions_before = db.query("SELECT * FROM sessions")
    db.executescript(migs[6].path.read_text(encoding="utf-8"))
    assert current_version(db) == 7
    users_after = db.query("SELECT * FROM users")
    assert [{k: r[k] for k in users_before[0]} for r in users_after] == users_before
    assert users_after[0]["temp_password_expires_at"] is None and users_after[0]["email_verified_at"] is None
    assert db.query("SELECT * FROM sessions") == sessions_before
    assert verify_password(PW, users_after[0]["password_hash"])  # legacy hash still verifies
    assert db.query("SELECT COUNT(*) AS n FROM email_tokens")[0]["n"] == 0
    with pytest.raises(IntegrityError):  # purpose is constrained
        db.execute(
            "INSERT INTO email_tokens (id, user_id, purpose, token_hash, created_at, expires_at) "
            "VALUES ('e', 'usr_a', 'other', 'x', '2026', '2027')"
        )
