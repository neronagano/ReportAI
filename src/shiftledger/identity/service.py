import hashlib

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.config import Settings
from shiftledger.errors import ApplicationError
from shiftledger.identity.access import MemberContext
from shiftledger.identity.models import Membership, User
from shiftledger.identity.schemas import MemberCreate
from shiftledger.identity.security import hash_password, verify_password

LOGIN_LIMIT_SCRIPT = """
local requests = redis.call('INCR', KEYS[1])
if requests == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return requests
"""


async def enforce_login_limit(cache: Redis, identity: str, settings: Settings) -> None:
    key = "shiftledger:login:" + hashlib.sha256(identity.encode()).hexdigest()
    try:
        count = await cache.eval(LOGIN_LIMIT_SCRIPT, 1, key, 60)
    except RedisError as exc:
        raise ApplicationError(
            "dependency_unavailable", "Login temporarily unavailable", 503
        ) from exc
    if int(count) > settings.login_requests_per_minute:
        raise ApplicationError("rate_limited", "Too many login attempts; retry in one minute", 429)


async def authenticate(session: AsyncSession, email: str, password: str) -> User:
    user = await session.scalar(select(User).where(User.email == email.casefold()))
    valid = await verify_password(password, user.password_hash if user else None)
    if not valid or user is None or not user.is_active:
        raise ApplicationError("invalid_credentials", "Invalid email or password", 401)
    return user


async def create_member(session: AsyncSession, member: MemberContext, data: MemberCreate) -> User:
    if not member.is_admin:
        raise ApplicationError("forbidden", "Administrator access required", 403)
    user = User(
        email=str(data.email).casefold(),
        password_hash=await hash_password(data.password.get_secret_value()),
    )
    try:
        async with session.begin_nested():
            session.add(user)
            await session.flush()
            session.add(
                Membership(organization_id=member.organization_id, user_id=user.id, role=data.role)
            )
            await session.flush()
    except IntegrityError as exc:
        # Do not reset a pre-existing user's password through membership provisioning.
        raise ApplicationError(
            "email_already_exists", "An account with this email already exists", 409
        ) from exc
    return user
