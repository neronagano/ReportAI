from datetime import timedelta
from uuid import UUID, uuid4

import anyio
import jwt
from pwdlib import PasswordHash

from shiftledger.config import Settings
from shiftledger.database import utc_now
from shiftledger.errors import ApplicationError

PASSWORD_HASHER = PasswordHash.recommended()
DUMMY_HASH = PASSWORD_HASHER.hash("unrelated-dummy-password-for-constant-verification")
TOKEN_ISSUER = "shiftledger"
TOKEN_AUDIENCE = "shiftledger-api"


async def hash_password(password: str) -> str:
    return await anyio.to_thread.run_sync(PASSWORD_HASHER.hash, password)


async def verify_password(password: str, encoded: str | None) -> bool:
    verified = await anyio.to_thread.run_sync(
        PASSWORD_HASHER.verify, password, encoded or DUMMY_HASH
    )
    return verified and encoded is not None


def issue_token(user_id: UUID, settings: Settings) -> str:
    now = utc_now()
    return jwt.encode(
        {
            "sub": str(user_id),
            "iss": TOKEN_ISSUER,
            "aud": TOKEN_AUDIENCE,
            "iat": now,
            "nbf": now,
            "exp": now + timedelta(minutes=settings.token_ttl_minutes),
            "jti": str(uuid4()),
        },
        settings.app_secret_key.get_secret_value(),
        algorithm="HS256",
    )


def decode_token(token: str, settings: Settings) -> UUID:
    try:
        claims = jwt.decode(
            token,
            settings.app_secret_key.get_secret_value(),
            algorithms=["HS256"],
            issuer=TOKEN_ISSUER,
            audience=TOKEN_AUDIENCE,
            options={"require": ["sub", "iss", "aud", "iat", "nbf", "exp", "jti"]},
        )
        return UUID(claims["sub"])
    except (jwt.InvalidTokenError, ValueError, KeyError) as exc:
        raise ApplicationError("unauthorized", "Invalid or expired access token", 401) from exc
