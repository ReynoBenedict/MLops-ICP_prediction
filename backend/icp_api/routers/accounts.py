"""Accounts: self-service registration, login/logout, profile, organisation members, API keys, and
platform-operator tenant administration."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request

from icp_api.errors import ApiError
from icp_api.logging_setup import request_id_var
from icp_api.routers.common import ERRORS, CursorQuery, LimitQuery, container
from icp_api.schemas import (
    ApiKeyCreated,
    ApiKeyCreateRequest,
    ApiKeyInfo,
    AuthResponse,
    ChangePasswordRequest,
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
    PlanInfo,
    ProfileUpdateRequest,
    RegisterRequest,
    TemporaryPasswordResponse,
    UserPublic,
)
from icp_api.security import Principal, rate_limit, require_admin, require_user

router = APIRouter(prefix="/api/v1", tags=["accounts"], responses=ERRORS)


def _actor(p: Principal) -> dict[str, Any]:
    return {"id": p.user_id, "role": p.role, "name": p.name}


def _org_id(p: Principal, request: Request) -> str:
    """Organisation in scope: always the member's own. Operators are refused on /org* (403) and
    administer tenants through /admin/organizations instead."""
    assert p.is_user and p.org_id is not None
    return p.org_id


@router.get("/plans", response_model=list[PlanInfo])
def plans() -> list[dict[str, Any]]:
    from icp_api.entitlements import PLANS

    return [p.to_dict() for p in PLANS.values()]


@router.post(
    "/auth/register",
    response_model=AuthResponse,
    status_code=201,
    dependencies=[Depends(rate_limit("auth", "rate_limit_default_per_minute"))],
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
    dependencies=[Depends(rate_limit("auth", "rate_limit_default_per_minute"))],
)
def login(request: Request, body: LoginRequest) -> dict[str, Any]:
    return container(request).accounts.login(
        body.email, body.password, request_id_var.get(), request.headers.get("user-agent")
    )


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


@router.post("/auth/password", status_code=204)
def change_password(
    request: Request,
    body: ChangePasswordRequest,
    principal: Principal = Depends(require_user("viewer", allow_operator=False)),
) -> None:
    if not principal.user_id:
        raise ApiError(403, "FORBIDDEN", "API keys have no password.")
    container(request).accounts.change_password(
        principal.user_id, body.current_password, body.new_password, request_id_var.get()
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


@router.post("/org/members", response_model=InviteResponse, status_code=201)
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
