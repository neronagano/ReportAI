import os
from datetime import timedelta
from uuid import uuid4

import jwt
import pytest
from pydantic import ValidationError

from shiftledger.config import Settings
from shiftledger.database import utc_now
from shiftledger.errors import ApplicationError
from shiftledger.identity.security import TOKEN_AUDIENCE, TOKEN_ISSUER, decode_token, issue_token


@pytest.fixture(autouse=True)
def isolated_settings_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    fields = {field.casefold() for field in Settings.model_fields}
    for name in list(os.environ):
        if name.casefold() in fields:
            monkeypatch.delenv(name)


def settings(**overrides: object) -> Settings:
    data = {
        "_env_file": None,
        "app_secret_key": "test-key-0123456789abcdefghABCDEFGH",
        "database_url": "postgresql+asyncpg://local/local",
        "redis_url": "redis://localhost/0",
        "kafka_bootstrap_servers": "localhost:9092",
    }
    return Settings(**{**data, **overrides})


def test_access_token_requires_issuer_audience_and_expiration() -> None:
    config = settings()
    user_id = uuid4()
    assert decode_token(issue_token(user_id, config), config) == user_id
    incomplete = jwt.encode(
        {"sub": str(user_id)}, config.app_secret_key.get_secret_value(), algorithm="HS256"
    )
    with pytest.raises(ApplicationError):
        decode_token(incomplete, config)
    now = utc_now() - timedelta(hours=1)
    expired = jwt.encode(
        {
            "sub": str(user_id),
            "iss": TOKEN_ISSUER,
            "aud": TOKEN_AUDIENCE,
            "iat": now,
            "nbf": now,
            "exp": now + timedelta(minutes=1),
            "jti": str(uuid4()),
        },
        config.app_secret_key.get_secret_value(),
        algorithm="HS256",
    )
    with pytest.raises(ApplicationError):
        decode_token(expired, config)


def test_development_configuration_cannot_be_used_in_production() -> None:
    with pytest.raises(ValidationError):
        settings(app_env="production", app_secret_key="development-change-me-01234567890")


def test_production_configuration_requires_verified_secure_connections() -> None:
    config = settings(
        app_env="production",
        allowed_hosts=["api.example.com"],
        database_url="postgresql+asyncpg://local/local?ssl=verify-full",
        redis_url="rediss://localhost/0",
        kafka_security_protocol="SSL",
        kafka_replication_factor=3,
        metrics_token="a-separate-metrics-token-with-32-characters",
        seed_admin_password=None,
        seed_employee_password=None,
    )
    assert config.app_env == "production"
    with pytest.raises(ValidationError, match="Redis"):
        settings(**{**config.model_dump(), "redis_url": "redis://localhost/0"})
