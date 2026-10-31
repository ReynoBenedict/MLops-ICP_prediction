"""Provider-neutral billing (audit v7 Wave E): SANDBOX ONLY and disabled by default (``BILLING_ENABLED``).

* ``config``   - environment configuration (BILLING_ENABLED, MIDTRANS_*); production is refused in this build.
* ``domain``   - order / payment-intent finite state machine and provider-status mapping.
* ``midtrans`` - Midtrans Snap adapter: request building, notification signature check, status API
  (the HTTP client is injectable; tests never touch the network).
* ``store``    - billing_orders / billing_events (migration 0008) over the shared SQL executor.
* ``service``  - order creation, checkout, idempotent notification processing, dry-run reconciliation.
* ``router``   - ``APIRouter`` under ``/api/v1/billing`` (wired in ``icp_api.main`` by the coordinator).

No prices or plans are defined anywhere in this package: amounts are set per order by a platform
operator after an owner decision (docs: OWNER_DECISIONS_REQUIRED.md).
"""
