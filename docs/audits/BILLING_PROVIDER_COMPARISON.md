# Billing and e-mail provider comparison (checked 2026-10-20)

Status: CURRENT as of 2026-10-20. Every statement comes from the provider's official page fetched on
2026-10-20, without logging in and without sending any project code or data. Fees change: re-check them
before signing anything. Nothing here is a recommendation to sign up; the owner decides (see
[OWNER_DECISIONS_REQUIRED.md](OWNER_DECISIONS_REQUIRED.md) §3–6 and §8).

## Payment providers

| | Midtrans (Snap) | Xendit | Stripe (Indonesia) |
|---|---|---|---|
| **Merchant availability / KYC** | Production onboarding requirements are not stated on the docs pages read. The Technical FAQ points to the dashboard and support@midtrans.com. Sandbox host `app.sandbox.midtrans.com`; for production, drop `.sandbox`. Server and client keys must belong to the same merchant and environment. | Not verified: the official pricing and help pages answered 403 to the fetcher. | **Invite-only** in Indonesia. Requirements: a legal name matching the NPWP, an IDR bank account, owners holding more than 25%, and NPWP/NIB/AKTA/SIUP/SK Kemenkumham/KTP documents. Accepts a PT, sole proprietor or individual. No non-profits. **No cross-border transactions.** |
| **Methods relevant to B2B** | VA/bank transfer, QRIS, GoPay, ShopeePay, DANA, OVO, cards, over-the-counter, PayLater (pricing page) | VA, QRIS, e-wallets, cards. This comes from a search-engine summary of the help centre, **not verified** on an official page. | The page names only ID Bank Transfer VA. |
| **Recurring billing** | Subscription API `POST /v1/subscriptions`: **only `credit_card` and `gopay`**, IDR only. By default it retries 3 times, once an hour, then marks the subscription `inactive`. The idempotency key is valid for 3 minutes. Use the separate subscription notification to track outcomes. | Subscriptions model: Plan → Cycles → Attempts. The scheduler retries and tries tokens in priority order. Integration through a Checkout `SUBSCRIPTION` session or an existing token. The page does not list Indonesian methods. | Stripe Billing exists globally; availability for Indonesian accounts was not confirmed. |
| **Webhook authenticity** | `signature_key = SHA512(order_id + status_code + gross_amount + ServerKey)`. Also confirm through the GET Status API, and don't trust frontend callbacks. | Static `x-callback-token` header (token from the dashboard). **No HMAC** is described. | `Stripe-Signature: t=…,v1=…`. HMAC-SHA256 over `"{t}.{raw_body}"` with the endpoint's `whsec_` secret. Ignore non-`v1` schemes. Default tolerance is 5 minutes (never 0). Compare in constant time. Up to 24 h of overlap when rolling secrets. |
| **Retries / duplicates / order** | At most 5 retries (about 2, 10, 30, 90 and 210 minutes, with random shift). 15 s timeout (answer in under 5 s). Codes: 2xx no retry; 500 once; 503 four times; 400/404 twice. Duplicates are possible: dedupe on `order_id`. Ignore out-of-order statuses (for example pending after settlement). | Up to 6 retries with exponential backoff; a webhook can also be re-triggered from the dashboard. Dedupe on `payment_id` / `capture_id`. Order is not guaranteed; queue and process asynchronously. | Live mode retries for up to 3 days (sandbox: 3 times over a few hours). Dedupe on event id. Order is not guaranteed. Events stay listable for 30 days for recovery. |
| **Refunds** | `POST /v2/{order_id\|transaction_id}/refund` (Basic auth). Settled transactions only (use Cancel for pending ones). Partial refunds are allowed except for ShopeePay and OVO. `refund_key` is the idempotency key: retries within 7 days, and a duplicate returns 406. Refund windows run from 7 to 365 days depending on the method. Refund capability must be activated through Midtrans support. | Each succeeded cycle has an `action_id` usable for refunds; the details were not fetched. | Standard Refunds API (not re-fetched). |
| **Sandbox** | Yes (`app.sandbox.midtrans.com`, `api.sandbox.midtrans.com`). GoPay tokenisation in sandbox needs the documented test phone numbers. | Not verified (the test-mode docs URL tried returned 404). | Yes (sandbox, Stripe CLI `listen` / `trigger`). |
| **Fees (official)** | VA IDR 4,000; QRIS 0.7%; GoPay 2%; ShopeePay 2% (English table) or 1.5% (Indonesian section, a discrepancy); DANA/OVO 1.5%; cards 2.9% + IDR 2,000; over-the-counter IDR 5,000. No setup or monthly fee. Excludes VAT except QRIS, GoPay and ShopeePay. | **Not verified** (official page 403). A search snippet of the help centre gives VA IDR 4,000, QRIS 0.63% and no registration fee. Treat these as unconfirmed. | Not published on the page read. |

### Sources (fetched 2026-10-20)
- Midtrans:
  - [Snap overview](https://docs.midtrans.com/docs/snap)
  - [Getting started with Snap](https://docs.midtrans.com/reference/getting-started-with-snap)
  - [Create subscription](https://docs.midtrans.com/reference/create-subscription)
  - [HTTP(S) notification / webhooks](https://docs.midtrans.com/docs/https-notification-webhooks)
  - [Best practices to handle notification](https://docs.midtrans.com/reference/best-practices-to-handle-notification)
  - [Refund transaction](https://docs.midtrans.com/reference/refund-transaction)
  - [Technical FAQ](https://docs.midtrans.com/docs/technical-faq)
  - [Pricing](https://midtrans.com/pricing)
- Xendit:
  - [Handling webhooks](https://docs.xendit.co/docs/handling-webhooks)
  - [Subscriptions overview](https://docs.xendit.co/docs/subscriptions-overview)
  - [Pricing](https://www.xendit.co/en-id/pricing/) (403, not read)
- Stripe:
  - [Requirements to open a Stripe account in Indonesia](https://support.stripe.com/questions/requirements-to-open-a-stripe-account-in-indonesia)
  - [Webhooks (signature verification)](https://docs.stripe.com/webhooks)

### Assessment for this product (Indonesian B2B SaaS, IDR)
1. **Stripe is not a practical first choice:** it is invite-only and has no cross-border transactions.
2. **Midtrans** has the best-documented webhook integrity of the two local options: a signature that includes the
   amount, plus a status API. It has sandbox and refund idempotency.
   - Its recurring API covers only cards and GoPay, which suits few B2B buyers.
   - For B2B, **period invoices paid once by VA/QRIS** are simpler: one Snap transaction per billing period. The
     plan is then activated through the audited `set_plan` path after signature check, status re-check and amount
     match.
3. **Xendit** authenticates webhooks with a static token. If it is chosen, always re-check the payment through its
   API before granting a plan, and restrict the webhook source if Xendit publishes IP ranges.

### Integration requirements independent of the provider (BILL-02)
- A raw-body webhook route that is **not** behind the gateway-secret requirement. It is authenticated by the
  provider signature or token, and the order id, amount, currency and environment are checked against the stored
  order.
- A `payment_events` table with a unique provider event or order id, so processing is idempotent. A finite state
  machine (created → pending → paid → refunded/expired/failed) ignores out-of-order transitions.
- A status-API re-check before any entitlement change. A reconciliation job compares local orders with the
  provider's status.
- An always-on HTTPS endpoint: providers retry for hours or days, and the Quick Tunnel hostname changes on every
  restart.
- Owner decisions on PPN/e-Faktur, invoices, refunds, downgrade and cancellation (OWNER_DECISIONS §5–6).

## Transactional e-mail providers (official pricing pages, 2026-10-20)

| Provider | Free | Cheapest paid | Notes | Source |
|---|---|---|---|---|
| Resend | 3,000/month, 100/day, 3 domains | Pro $20/mo for 50,000/month | | [resend.com/pricing](https://resend.com/pricing) |
| Postmark | 100/month (developer plan, no expiry) | Basic $15/mo for 10,000/month, plus $1.80 per extra 1,000 | | [postmarkapp.com/pricing](https://postmarkapp.com/pricing) |
| Amazon SES | Free-tier credits: up to $200, to be used within 12 months of account creation | $0.10 per 1,000 outbound, plus $0.12/GB of attachments | Plan pricing is per region | [aws.amazon.com/ses/pricing](https://aws.amazon.com/ses/pricing/) |
| Brevo | not retrieved (the page returned only its title) | — | | [brevo.com/pricing](https://www.brevo.com/pricing/) |

All of them need a domain the owner controls, with SPF, DKIM and DMARC records. Mail is processed outside
Indonesia, which the Privacy text must state (the `transfer` item in LEGAL_AND_PRIVACY.md).
