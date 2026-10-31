"""Billing configuration from the environment (read here, not in icp_api.settings).

Variables (names only; values are secrets and never logged):

* ``BILLING_ENABLED``        - ``true`` enables the billing endpoints; default ``false`` (every endpoint
  answers 503 BILLING_DISABLED and the webhook rejects everything).
* ``MIDTRANS_ENVIRONMENT``   - ``sandbox`` (default). ``production`` is refused in this build.
* ``MIDTRANS_SERVER_KEY``    - Snap server key (Basic-auth user, notification signature secret).
* ``MIDTRANS_CLIENT_KEY``    - optional, only for a browser Snap.js integration (not used by the API).
* ``MIDTRANS_TIMEOUT_SECONDS`` - HTTP timeout for provider calls (default 15).

Environment/key consistency: Midtrans dashboard keys of the sandbox environment carry the
``SB-Mid-server-`` prefix. This is the observed dashboard convention; the access-keys page
(https://docs.midtrans.com/docs/access-keys, accessed 2026-10-20) does not state it, so the check is
deliberately strict (a sandbox configuration with a non-sandbox-looking key is refused) and the
owner should confirm it against the dashboard before any production work.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

SANDBOX_KEY_PREFIX = "SB-Mid-server-"
ENVIRONMENTS = ("sandbox", "production")


class BillingConfigError(ValueError):
    """The billing configuration is inconsistent (reported as 503 BILLING_MISCONFIGURED)."""


@dataclass(frozen=True)
class BillingConfig:
    enabled: bool = False
    environment: str = "sandbox"
    server_key: str = field(default="", repr=False)
    client_key: str = field(default="", repr=False)
    timeout_seconds: float = 15.0
    currency: str = "IDR"
    provider: str = "midtrans"

    @classmethod
    def from_env(cls) -> BillingConfig:
        return cls(
            enabled=os.getenv("BILLING_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on"),
            environment=os.getenv("MIDTRANS_ENVIRONMENT", "sandbox").strip().lower() or "sandbox",
            server_key=os.getenv("MIDTRANS_SERVER_KEY", "").strip(),
            client_key=os.getenv("MIDTRANS_CLIENT_KEY", "").strip(),
            timeout_seconds=float(os.getenv("MIDTRANS_TIMEOUT_SECONDS", "15") or 15),
        )

    def problems(self) -> list[str]:
        """Configuration errors (empty when usable). Messages never contain key values."""
        out = []
        if self.environment not in ENVIRONMENTS:
            out.append("MIDTRANS_ENVIRONMENT must be 'sandbox'")
        elif self.environment == "production":
            out.append("production billing is not enabled in this build (sandbox only)")
        if not self.server_key:
            out.append("MIDTRANS_SERVER_KEY is not set")
        elif self.environment == "sandbox" and not self.server_key.startswith(SANDBOX_KEY_PREFIX):
            out.append("MIDTRANS_SERVER_KEY does not look like a sandbox key (environment/key mismatch)")
        elif self.environment != "sandbox" and self.server_key.startswith(SANDBOX_KEY_PREFIX):
            out.append("a sandbox key is configured for a non-sandbox environment")
        return out
