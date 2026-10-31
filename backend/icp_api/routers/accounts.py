"""Accounts: self-service registration, login/logout, profile, organisation members, API keys, and
platform-operator tenant administration."""

from __future__ import annotations

import hashlib
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, container
from icp_api.schemas import (
    ActiveSession,
    ApiKeyCreated,
    ApiKeyCreateRequest,
    ApiKeyInfo,
    AuthResponse,
    ChangePasswordRequest,
    DeletionRequest,
    DeletionRequestCreate,
    DeletionRequestList,
    DeletionResolveRequest,
    EmailRequestAccepted,
    EmailTokenConfirm,
    InviteRequest,
    InviteResponse,
    LoginRequest,
    MemberUpdateRequest,
    MeResponse,
    OperatorPlanRequest,
    OperatorStatusRequest,
    OrganizationDetail,
    OrganizationList,
    OrgRenameRequest,
    PasswordResetConfirm,
    PasswordResetRequest,
    PlanChangeRequest,
    PlanChangeRequestCreate,
    PlanChangeRequestList,
    PlanChangeResolveRequest,
    PlanInfo,
    ProfileUpdateRequest,
    RegisterRequest,
    RevokedSessions,
    TemporaryPasswordResponse,
    UserPublic,
)
from icp_api.security import (
    FailureThrottle,
    Principal,
    client_key,
    member_org_id,
    member_user_id,
    rate_limit,
    require_admin,
    require_user,
)
from icp_api.services.accounts import check_password_strength, normalize_email
from icp_api.services.email_tokens import EmailTokenService, token_invalid_error
from icp_api.storage.repository import InvalidCursor, utcnow

# Every route here also counts against the per-client default budget (defence in depth behind the
# gateway; sign-in, registration and password change additionally use the stricter `auth` bucket).
router = APIRouter(
    prefix="/api/v1",
    tags=["accounts"],
    responses=ERRORS,
    dependencies=[Depends(rate_limit("default", "rate_limit_default_per_minute"))],
)


def _actor(p: Principal) -> dict[str, Any]:
    return {"id": p.user_id, "role": p.role, "name": p.name}


def _org_id(p: Principal, request: Request) -> str:
    """Organisation in scope: always the member's own. Operators are refused on /org* (403) and
    administer tenants through /admin/organizations instead."""
    return member_org_id(p)


@router.get("/plans", response_model=list[PlanInfo])
def plans() -> list[dict[str, Any]]:
    from icp_api.entitlements import PLANS

    return [p.to_dict() for p in PLANS.values()]


@router.post(
    "/auth/register",
    response_model=AuthResponse,
    status_code=201,
    dependencies=[Depends(rate_limit("auth", "rate_limit_auth_per_minute"))],
)
def register(request: Request, body: RegisterRequest) -> dict[str, Any]:
    c = container(request)
    if not c.settings.public_registration_enabled:
        raise ApiError(
            403,
            "REGISTRATION_DISABLED",
            "Pendaftaran mandiri dinonaktifkan. Hubungi operator untuk undangan.",
        )
    if not body.accept_terms:
        raise ApiError(
            422, "VALIDATION_ERROR", "Anda harus menyetujui Syarat Penggunaan dan Kebijakan Privasi."
        )
    return c.accounts.register(
        body.organization_name, body.email, body.name, body.password, request_id_var.get()
    )


@router.post(
    "/auth/login",
    response_model=AuthResponse,
    dependencies=[Depends(rate_limit("auth", "rate_limit_auth_per_minute"))],
)
def login(request: Request, body: LoginRequest) -> dict[str, Any]:
    c = container(request)
    s = c.settings
    throttle: FailureThrottle = request.app.state.login_failures
    key = hashlib.sha256(normalize_email(body.email).encode("utf-8")).hexdigest()
    who = client_key(request)
    if s.rate_limit_enabled:
        throttle.check(
            key,
            who,
            s.login_failures_max,
            s.login_account_failures_max,
            float(s.login_failure_window_seconds),
        )
    try:
        result = c.accounts.login(
            body.email, body.password, request_id_var.get(), request.headers.get("user-agent")
        )
    except ApiError as exc:
        if exc.code == "AUTH_INVALID":
            throttle.record_failure(key, who)
        raise
    throttle.reset(key, who)
    return result


@router.post("/auth/logout", status_code=204)
def logout(
    request: Request, principal: Principal = Depends(require_user("viewer", allow_operator=False))
) -> None:
    if principal.session_id and principal.user_id:
        container(request).accounts.logout(principal.session_id, principal.user_id, request_id_var.get())


@router.get("/auth/me", response_model=MeResponse)
def me(
    request: Request, principal: Principal = Depends(require_user("viewer", allow_operator=False))
) -> dict[str, Any]:
    if not principal.user_id:
        raise ApiError(403, "FORBIDDEN", "API keys have no profile.")
    return container(request).accounts.me(principal.user_id)


@router.post("/auth/me", response_model=MeResponse)
def update_profile(
    request: Request,
    body: ProfileUpdateRequest,
    principal: Principal = Depends(require_user("viewer", allow_operator=False)),
) -> dict[str, Any]:
    if not principal.user_id:
        raise ApiError(403, "FORBIDDEN", "API keys have no profile.")
    return container(request).accounts.update_profile(principal.user_id, body.name)


@router.post(
    "/auth/password",
    status_code=204,
    dependencies=[Depends(rate_limit("auth", "rate_limit_auth_per_minute"))],
)
def change_password(
    request: Request,
    body: ChangePasswordRequest,
    principal: Principal = Depends(require_user("viewer", allow_operator=False)),
) -> None:
    if not principal.user_id:
        raise ApiError(403, "FORBIDDEN", "API keys have no password.")
    s = container(request).settings
    # Wrong current passwords are throttled per account, so a stolen session cannot be used to
    # guess the real password (which would outlive the session's revocation).
    throttle: FailureThrottle = request.app.state.password_failures
    key = principal.user_id
    if s.rate_limit_enabled:
        throttle.check(
            key, key, s.login_failures_max, s.login_failures_max, float(s.login_failure_window_seconds)
        )
    try:
        container(request).accounts.change_password(
            principal.user_id, body.current_password, body.new_password, request_id_var.get()
        )
    except ApiError as exc:
        if exc.code == "AUTH_INVALID":
            throttle.record_failure(key, key)
        raise
    throttle.reset(key, key)


# -- sessions of the signed-in user (v7 Wave B) ----------------------------------------------------
SESSION_USER = Depends(require_user("viewer", allow_operator=False, allow_api_key=False))


def _session_id(p: Principal) -> str:
    if not p.session_id:
        raise ApiError(403, "FORBIDDEN", "Tindakan ini memerlukan sesi pengguna, bukan kunci API.")
    return p.session_id


@router.get("/auth/sessions", response_model=list[ActiveSession])
def my_sessions(request: Request, principal: Principal = SESSION_USER) -> list[dict[str, Any]]:
    """Your active sessions (device label, created, last seen, expiry); the current one is flagged."""
    return container(request).accounts.list_my_sessions(member_user_id(principal), _session_id(principal))


@router.post("/auth/sessions/revoke-others", response_model=RevokedSessions)
def revoke_other_sessions(request: Request, principal: Principal = SESSION_USER) -> dict[str, int]:
    """Sign out everywhere else: every session of yours except the one making this request ends."""
    n = container(request).accounts.revoke_my_other_sessions(
        member_user_id(principal), _session_id(principal), request_id_var.get()
    )
    return {"revoked": n}


@router.post("/auth/sessions/{session_id}/revoke", status_code=204)
def revoke_session(request: Request, session_id: str, principal: Principal = SESSION_USER) -> None:
    """End one of your sessions (404 for sessions that are not yours or already ended)."""
    container(request).accounts.revoke_my_session(member_user_id(principal), session_id, request_id_var.get())


# -- e-mail flows (disabled until an e-mail provider exists: 503 EMAIL_DISABLED) ---------------------
AUTH_RL = Depends(rate_limit("auth", "rate_limit_auth_per_minute"))


def _mail(request: Request) -> EmailTokenService:
    service: EmailTokenService = request.app.state.email_tokens
    service.require_enabled()  # 503 EMAIL_DISABLED before anything is read or written
    return service


@router.post(
    "/auth/password-reset/request",
    response_model=EmailRequestAccepted,
    status_code=202,
    dependencies=[AUTH_RL],
)
def request_password_reset(request: Request, body: PasswordResetRequest) -> dict[str, str]:
    """Send a single-use reset link (1 hour). The answer is the same whether or not the address
    has an account."""
    mail = _mail(request)
    c = container(request)
    user = c.repo.get_user_by_email(normalize_email(body.email))
    if user is not None and user["status"] == "active":
        mail.issue_and_send(user, "reset", request_id_var.get())
    return {
        "status": "accepted",
        "note": "Jika alamat ini terdaftar, tautan atur ulang kata sandi telah dikirim.",
    }


@router.post("/auth/password-reset/confirm", status_code=204, dependencies=[AUTH_RL])
def confirm_password_reset(request: Request, body: PasswordResetConfirm) -> None:
    """Set a new password with a reset link; every session of the account ends."""
    mail = _mail(request)
    c = container(request)
    check_password_strength(body.new_password)  # before the token is spent
    token = mail.consume(body.token, "reset")
    user = c.repo.get_user(token["user_id"])
    if user is None or user["status"] != "active":
        raise token_invalid_error()
    c.accounts.set_password(user["id"], body.new_password)
    c.repo.audit(f"user:{user['id']}", "account.password_reset", "success", request_id=request_id_var.get())


@router.post(
    "/auth/email/verify/request", response_model=EmailRequestAccepted, status_code=202, dependencies=[AUTH_RL]
)
def request_email_verification(request: Request, principal: Principal = SESSION_USER) -> dict[str, str]:
    mail = _mail(request)
    user = container(request).repo.get_user(member_user_id(principal))
    if user is None:
        raise ApiError(404, "NOT_FOUND", "Pengguna tidak ditemukan.")
    if user.get("email_verified_at"):
        raise ApiError(409, "EMAIL_ALREADY_VERIFIED", "Alamat email ini sudah terverifikasi.")
    mail.issue_and_send(user, "verify", request_id_var.get())
    return {"status": "accepted", "note": "Tautan verifikasi telah dikirim ke alamat email Anda."}


@router.post("/auth/email/verify/confirm", status_code=204, dependencies=[AUTH_RL])
def confirm_email_verification(request: Request, body: EmailTokenConfirm) -> None:
    token = _mail(request).consume(body.token, "verify")
    c = container(request)
    c.repo.update_user(token["user_id"], email_verified_at=utcnow())
    c.repo.audit(
        f"user:{token['user_id']}", "account.email_verified", "success", request_id=request_id_var.get()
    )


@router.post("/auth/invite/accept", status_code=204, dependencies=[AUTH_RL])
def accept_invitation(request: Request, body: PasswordResetConfirm) -> None:
    """Choose a password with an invitation link (replaces the temporary password; verifies the address)."""
    mail = _mail(request)
    c = container(request)
    check_password_strength(body.new_password)
    token = mail.consume(body.token, "invite")
    user = c.repo.get_user(token["user_id"])
    if user is None or user["status"] != "active":
        raise token_invalid_error()
    c.accounts.set_password(user["id"], body.new_password)
    c.repo.update_user(user["id"], email_verified_at=utcnow())
    c.repo.audit(
        f"user:{user['id']}", "org.member.invite_accepted", "success", request_id=request_id_var.get()
    )


# -- organisation ----------------------------------------------------------------------------------
@router.get("/org", response_model=OrganizationDetail)
def organization(
    request: Request,
    principal: Principal = Depends(require_user("viewer", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    return container(request).accounts.organization(_org_id(principal, request))


@router.post("/org", response_model=OrganizationDetail)
def rename_org(
    request: Request,
    body: OrgRenameRequest,
    principal: Principal = Depends(require_user("admin", allow_operator=False)),
) -> dict[str, Any]:
    return container(request).accounts.rename_organization(
        _org_id(principal, request), _actor(principal), body.name, request_id_var.get()
    )


@router.get("/org/members", response_model=list[UserPublic])
def members(
    request: Request,
    principal: Principal = Depends(require_user("viewer", allow_operator=False, allow_api_key=False)),
) -> list[dict[str, Any]]:
    return container(request).accounts.members(_org_id(principal, request))


@router.post(
    "/org/members",
    response_model=InviteResponse,
    status_code=201,
    dependencies=[Depends(rate_limit("auth", "rate_limit_auth_per_minute"))],
)
def invite(
    request: Request,
    body: InviteRequest,
    principal: Principal = Depends(require_user("admin", allow_operator=False)),
) -> dict[str, Any]:
    actor = _actor(principal)
    return container(request).accounts.invite_member(
        _org_id(principal, request), actor, body.email, body.name, body.role, request_id_var.get()
    )


@router.post("/org/members/{user_id}", response_model=UserPublic)
def update_member(
    request: Request,
    user_id: str,
    body: MemberUpdateRequest,
    principal: Principal = Depends(require_user("admin", allow_operator=False)),
) -> dict[str, Any]:
    actor = _actor(principal)
    return container(request).accounts.update_member(
        _org_id(principal, request), actor, user_id, body.role, body.status, request_id_var.get()
    )


@router.post("/org/members/{user_id}/reset-password", response_model=TemporaryPasswordResponse)
def reset_member_password(
    request: Request,
    user_id: str,
    principal: Principal = Depends(require_user("admin", allow_operator=False)),
) -> dict[str, Any]:
    actor = _actor(principal)
    return container(request).accounts.reset_member_password(
        _org_id(principal, request), actor, user_id, request_id_var.get()
    )


@router.post(
    "/org/members/{user_id}/invitation-email",
    response_model=EmailRequestAccepted,
    status_code=202,
    dependencies=[AUTH_RL],
)
def send_invitation_email(
    request: Request,
    user_id: str,
    principal: Principal = Depends(require_user("admin", allow_operator=False)),
) -> dict[str, str]:
    """E-mail an invitation link to a member who has not chosen a password yet (instead of handing
    over the temporary password). 503 EMAIL_DISABLED while e-mail is not enabled."""
    mail = _mail(request)
    c = container(request)
    org_id = _org_id(principal, request)
    target = c.repo.get_user(user_id)
    if target is None or target["org_id"] != org_id:
        raise ApiError(404, "NOT_FOUND", "Anggota tidak ditemukan.")
    if not target.get("must_change_password"):
        raise ApiError(409, "MEMBER_ALREADY_ACTIVE", "Anggota ini sudah mengaktifkan akunnya.")
    if target["role"] == "admin" and principal.role != "owner":
        raise ApiError(
            403, "FORBIDDEN", "Hanya pemilik organisasi yang dapat mengelola akun administrator lain."
        )
    mail.issue_and_send(target, "invite", request_id_var.get())
    return {"status": "accepted", "note": "Undangan telah dikirim ke alamat email anggota."}


@router.get("/org/api-keys", response_model=list[ApiKeyInfo])
def api_keys(
    request: Request, principal: Principal = Depends(require_user("admin", allow_operator=False))
) -> list[dict[str, Any]]:
    return container(request).accounts.list_api_keys(_org_id(principal, request))


@router.post("/org/api-keys", response_model=ApiKeyCreated, status_code=201)
def create_api_key(
    request: Request,
    body: ApiKeyCreateRequest,
    principal: Principal = Depends(require_user("admin", allow_operator=False)),
) -> dict[str, Any]:
    actor = _actor(principal)
    return container(request).accounts.create_api_key(
        _org_id(principal, request), actor, body.name, request_id_var.get()
    )


@router.post("/org/api-keys/{key_id}/revoke", status_code=204)
def revoke_api_key(
    request: Request, key_id: str, principal: Principal = Depends(require_user("admin", allow_operator=False))
) -> None:
    actor = _actor(principal)
    container(request).accounts.revoke_api_key(
        _org_id(principal, request), actor, key_id, request_id_var.get()
    )


# -- plan change requests (no payment) -------------------------------------------------------------
@router.post("/org/plan-requests", response_model=PlanChangeRequest, status_code=201)
def create_plan_request(
    request: Request,
    body: PlanChangeRequestCreate,
    principal: Principal = Depends(require_user("admin", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    return container(request).plan_requests.create(
        _org_id(principal, request),
        member_user_id(principal),
        body.requested_plan,
        body.note,
        request_id_var.get(),
    )


@router.get("/org/plan-requests", response_model=PlanChangeRequestList)
def list_plan_requests(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    principal: Principal = Depends(require_user("viewer", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    try:
        return container(request).plan_requests.list(_org_id(principal, request), None, limit, cursor)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post("/org/plan-requests/{request_id}/withdraw", response_model=PlanChangeRequest)
def withdraw_plan_request(
    request: Request,
    request_id: str,
    principal: Principal = Depends(require_user("admin", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    return container(request).plan_requests.withdraw(
        request_id, _org_id(principal, request), member_user_id(principal), request_id_var.get()
    )


@router.get("/admin/plan-requests", response_model=PlanChangeRequestList)
def admin_plan_requests(
    request: Request,
    status: Literal["pending", "approved", "declined", "withdrawn"] | None = Query(None),
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    _: Principal = Depends(require_admin),
) -> dict[str, Any]:
    try:
        return container(request).plan_requests.list(None, status, limit, cursor)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post("/admin/plan-requests/{request_id}/resolve", response_model=PlanChangeRequest)
def resolve_plan_request(
    request: Request,
    request_id: str,
    body: PlanChangeResolveRequest,
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    return container(request).plan_requests.resolve(
        request_id, body.decision, principal.name, body.note, request_id_var.get()
    )


# -- privacy: data export and deletion requests (no e-mail, no hard delete) ----------------------------
@router.get(
    "/org/export",
    responses={200: {"content": {"application/zip": {}}}},
    dependencies=[Depends(rate_limit("forecast", "rate_limit_forecast_per_minute"))],
)
def export_organization(
    request: Request,
    principal: Principal = Depends(require_user("owner", allow_operator=False, allow_api_key=False)),
) -> Response:
    """ZIP of every tenant record of the caller's organisation (owner only; generated on demand,
    never stored server-side; no password/token/key hashes; official datasets are not included)."""
    data, filename = container(request).privacy.export(
        _org_id(principal, request), member_user_id(principal), request_id_var.get()
    )
    return Response(
        data,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "private, no-store",
        },
    )


@router.post("/org/deletion-requests", response_model=DeletionRequest, status_code=201)
def create_deletion_request(
    request: Request,
    body: DeletionRequestCreate,
    principal: Principal = Depends(require_user("viewer", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    return container(request).privacy.create(
        _org_id(principal, request),
        member_user_id(principal),
        principal.role,
        body.scope,
        body.reason,
        request_id_var.get(),
    )


@router.get("/org/deletion-requests", response_model=DeletionRequestList)
def list_deletion_requests(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    principal: Principal = Depends(require_user("viewer", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    """The owner sees every request of the organisation; other members only their own."""
    try:
        return container(request).privacy.list(
            _org_id(principal, request),
            None,
            limit,
            cursor,
            requested_by=None if principal.role == "owner" else principal.user_id,
        )
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post("/org/deletion-requests/{request_id}/withdraw", response_model=DeletionRequest)
def withdraw_deletion_request(
    request: Request,
    request_id: str,
    principal: Principal = Depends(require_user("viewer", allow_operator=False, allow_api_key=False)),
) -> dict[str, Any]:
    return container(request).privacy.withdraw(
        request_id,
        _org_id(principal, request),
        member_user_id(principal),
        request_id_var.get(),
    )


@router.get("/admin/deletion-requests", response_model=DeletionRequestList)
def admin_deletion_requests(
    request: Request,
    status: Literal["pending", "executed", "declined", "withdrawn"] | None = Query(None),
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    _: Principal = Depends(require_admin),
) -> dict[str, Any]:
    try:
        return container(request).privacy.list(None, status, limit, cursor)
    except InvalidCursor as exc:
        raise ApiError(400, "BAD_REQUEST", str(exc)) from exc


@router.post("/admin/deletion-requests/{request_id}/resolve", response_model=DeletionRequest)
def resolve_deletion_request(
    request: Request,
    request_id: str,
    body: DeletionResolveRequest,
    principal: Principal = Depends(require_admin),
) -> dict[str, Any]:
    """Execute (anonymise personal data, revoke credentials; audit, usage and official data are kept)
    or decline a pending deletion request."""
    return container(request).privacy.resolve(
        request_id, body.decision, principal.name, body.note, request_id_var.get()
    )


# -- platform operator: tenant administration ------------------------------------------------------
@router.get("/admin/organizations", response_model=OrganizationList)
def list_organizations(
    request: Request,
    limit: int = LimitQuery,
    cursor: str | None = CursorQuery,
    _: Principal = Depends(require_admin),
) -> dict[str, Any]:
    page = container(request).repo.list_organizations(limit, cursor)
    return {
        "items": [{**o, "settings": o.get("settings")} for o in page.items],
        "next_cursor": page.next_cursor,
    }


@router.post("/admin/organizations/{org_id}/plan", response_model=OrganizationDetail)
def set_plan(
    request: Request, org_id: str, body: OperatorPlanRequest, principal: Principal = Depends(require_admin)
) -> dict[str, Any]:
    return container(request).accounts.set_plan(
        org_id, body.plan, principal.name, request_id_var.get(), body.reason
    )


@router.post("/admin/organizations/{org_id}/status", response_model=OrganizationDetail)
def set_status(
    request: Request, org_id: str, body: OperatorStatusRequest, principal: Principal = Depends(require_admin)
) -> dict[str, Any]:
    return container(request).accounts.set_status(
        org_id, body.status, principal.name, request_id_var.get(), body.reason
    )
