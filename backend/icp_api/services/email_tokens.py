"""E-mail tokens for verification, password reset and invitation delivery - disabled by default.

No e-mail provider is integrated (EMAIL_ENABLED=false, the default and the only value the settings
accept today). While disabled, every e-mail flow answers ``503 EMAIL_DISABLED``; nothing is created
or sent and success is never pretended. The token mechanics are complete and tested with an
in-memory sender so a provider can be added by implementing ``EmailSender`` only:

* tokens are random (``secrets.token_urlsafe(32)``, prefix ``icp_mail_``), single use and expiring;
  only their SHA-256 is stored (``email_tokens``, migration 0007);
* issuing a token supersedes earlier unused tokens of the same purpose for that account;
* consuming is one conditional UPDATE (unused and unexpired), so a token works exactly once;
* the clear token exists only inside the outgoing message (a link to the web app).
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from icp_api.errors import ApiError
from icp_api.storage.product import ProductRepository
from icp_api.storage.repository import new_id

MAIL_CODE_PREFIX = "icp_mail_"
TOKEN_TTL = {
    "verify": timedelta(hours=48),
    "reset": timedelta(hours=1),
    "invite": timedelta(days=7),
}
# Web-app routes the links point to (the SPA reads ?token= and calls the confirm endpoint).
LINK_PATHS = {"verify": "/verifikasi-email", "reset": "/atur-ulang-sandi", "invite": "/terima-undangan"}
SUBJECTS = {
    "verify": "Verifikasi alamat email Anda - ICP Intelligence",
    "reset": "Atur ulang kata sandi - ICP Intelligence",
    "invite": "Undangan bergabung - ICP Intelligence",
}


@dataclass(frozen=True)
class EmailMessage:
    to: str
    subject: str
    text: str


class EmailSender(Protocol):
    """Delivers one message. Implementations must not log the message text (it carries a token)."""

    def send(self, message: EmailMessage) -> None: ...


class NullEmailSender:
    """The only sender shipped: refuses to send (no provider is configured)."""

    def send(self, message: EmailMessage) -> None:
        raise email_disabled_error()


def email_disabled_error() -> ApiError:
    return ApiError(
        503,
        "EMAIL_DISABLED",
        "Pengiriman email belum diaktifkan di layanan ini. Hubungi administrator organisasi Anda.",
    )


def token_invalid_error() -> ApiError:
    return ApiError(
        400,
        "TOKEN_INVALID",
        "Tautan tidak valid, sudah dipakai, atau sudah kedaluwarsa. Minta tautan baru.",
    )


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class EmailTokenService:
    def __init__(self, repo: ProductRepository, sender: EmailSender, enabled: bool, link_base_url: str = ""):
        self.repo = repo
        self.sender = sender
        self.enabled = enabled
        self.link_base_url = link_base_url.rstrip("/")

    def require_enabled(self) -> None:
        if not self.enabled:
            raise email_disabled_error()

    def issue_and_send(self, user: dict[str, Any], purpose: str, request_id: str | None) -> None:
        """Create a token for ``user`` and send the link. Earlier unused tokens of the purpose end."""
        self.require_enabled()
        if purpose not in TOKEN_TTL:
            raise ValueError(purpose)
        token = MAIL_CODE_PREFIX + secrets.token_urlsafe(32)
        self.repo.supersede_email_tokens(user["id"], purpose)
        self.repo.insert_email_token(
            {
                "id": new_id("emt"),
                "user_id": user["id"],
                "purpose": purpose,
                "token_hash": _hash(token),
                "expires_at": _stamp(datetime.now(timezone.utc) + TOKEN_TTL[purpose]),
                "request_id": request_id,
            }
        )
        link = f"{self.link_base_url}{LINK_PATHS[purpose]}?token={token}"
        hours = int(TOKEN_TTL[purpose].total_seconds() // 3600)
        text = (
            f"Halo {user.get('name') or ''},\n\n"
            f"Buka tautan berikut (berlaku {hours} jam, sekali pakai):\n{link}\n\n"
            "Abaikan email ini jika Anda tidak memintanya."
        )
        self.sender.send(EmailMessage(to=user["email"], subject=SUBJECTS[purpose], text=text))
        self.repo.audit(
            f"user:{user['id']}", f"email.{purpose}.sent", "success", target=user["id"], request_id=request_id
        )

    def consume(self, token: str, purpose: str) -> dict[str, Any]:
        """The token row for a valid, unused, unexpired token of ``purpose`` (now marked used)."""
        self.require_enabled()
        if not token.startswith(MAIL_CODE_PREFIX) or len(token) > 200:
            raise token_invalid_error()
        row = self.repo.consume_email_token(_hash(token), purpose)
        if row is None:
            raise token_invalid_error()
        return row
