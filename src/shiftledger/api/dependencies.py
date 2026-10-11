from collections.abc import AsyncIterator
from typing import Annotated, cast
from uuid import UUID

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.errors import ApplicationError
from shiftledger.identity.access import MemberContext
from shiftledger.identity.models import Membership, Role, User
from shiftledger.identity.security import decode_token
from shiftledger.resources import Resources

bearer = HTTPBearer(auto_error=False)


def get_resources(request: Request) -> Resources:
    return cast(Resources, request.app.state.resources)


ResourcesDep = Annotated[Resources, Depends(get_resources)]


async def get_session(resources: ResourcesDep) -> AsyncIterator[AsyncSession]:
    # Function-scoped dependencies finish the transaction before sending the response.
    async with resources.sessions() as session, session.begin():
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session, scope="function")]


async def current_user(
    session: SessionDep,
    resources: ResourcesDep,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> User:
    if credentials is None:
        raise ApplicationError("unauthorized", "Authentication required", 401)
    user_id = decode_token(credentials.credentials, resources.settings)
    user = await session.get(User, user_id)
    if user is None or not user.is_active:
        raise ApplicationError("unauthorized", "Account is inactive or unavailable", 401)
    return user


UserDep = Annotated[User, Depends(current_user)]


async def organization_member(
    organization_id: UUID, user: UserDep, session: SessionDep
) -> MemberContext:
    member = await session.scalar(
        select(Membership).where(
            Membership.organization_id == organization_id, Membership.user_id == user.id
        )
    )
    if member is None:
        raise ApplicationError("forbidden", "Organization access denied", 403)
    return MemberContext(organization_id, user.id, Role(member.role))


MemberDep = Annotated[MemberContext, Depends(organization_member)]


async def organization_admin(member: MemberDep) -> MemberContext:
    if not member.is_admin:
        raise ApplicationError("forbidden", "Administrator access required", 403)
    return member


AdminDep = Annotated[MemberContext, Depends(organization_admin)]
