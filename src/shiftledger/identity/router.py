from fastapi import APIRouter, Request
from sqlalchemy import select

from shiftledger.api.dependencies import AdminDep, ResourcesDep, SessionDep, UserDep
from shiftledger.identity.models import Membership, Organization, Role
from shiftledger.identity.schemas import (
    LoginRequest,
    MemberCreate,
    MemberResponse,
    MembershipResponse,
    TokenResponse,
    UserResponse,
)
from shiftledger.identity.security import issue_token
from shiftledger.identity.service import authenticate, create_member, enforce_login_limit

router = APIRouter(tags=["identity"])


@router.post("/auth/login", response_model=TokenResponse)
async def login(
    data: LoginRequest, request: Request, session: SessionDep, resources: ResourcesDep
) -> TokenResponse:
    client_ip = request.client.host if request.client else "unknown"
    await enforce_login_limit(resources.redis, client_ip, resources.settings)
    user = await authenticate(session, str(data.email), data.password.get_secret_value())
    return TokenResponse(
        access_token=issue_token(user.id, resources.settings),
        expires_in=resources.settings.token_ttl_minutes * 60,
    )


@router.get("/auth/me", response_model=UserResponse)
async def me(user: UserDep) -> UserResponse:
    return UserResponse.model_validate(user)


@router.get("/organizations", response_model=list[MembershipResponse])
async def organizations(user: UserDep, session: SessionDep) -> list[MembershipResponse]:
    rows = (
        await session.execute(
            select(Membership, Organization.name)
            .join(Organization, Membership.organization_id == Organization.id)
            .where(Membership.user_id == user.id)
            .order_by(Organization.name)
        )
    ).all()
    return [
        MembershipResponse(
            organization_id=row.organization_id, organization_name=name, role=Role(row.role)
        )
        for row, name in rows
    ]


@router.post(
    "/organizations/{organization_id}/members", response_model=MemberResponse, status_code=201
)
async def add_member(data: MemberCreate, member: AdminDep, session: SessionDep) -> MemberResponse:
    user = await create_member(session, member, data)
    return MemberResponse(user_id=user.id, email=user.email, role=data.role)
