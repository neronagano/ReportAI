import os
from collections.abc import AsyncIterator

import pytest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from shiftledger.config import Environment, Settings
from shiftledger.database import create_engine, session_factory


@pytest.fixture
def integration_settings() -> Settings:
    database_url = os.environ.get("TEST_DATABASE_URL")
    redis_url = os.environ.get("TEST_REDIS_URL")
    if not database_url or not redis_url:
        pytest.skip("Set TEST_DATABASE_URL and TEST_REDIS_URL to run integration tests")
    if make_url(database_url).database != "shiftledger_test":
        pytest.fail("Integration tests require the dedicated shiftledger_test database")
    return Settings(
        _env_file=None,
        app_env=Environment.TEST,
        app_secret_key="integration-test-key-with-32-characters",
        database_url=database_url,
        redis_url=redis_url,
        kafka_bootstrap_servers="127.0.0.1:19092",
        s3_endpoint_url="http://127.0.0.1:18333",
        s3_access_key_id="integration-test",
        s3_secret_access_key="integration-test-storage",
        login_requests_per_minute=1000,
    )


@pytest.fixture
async def integration_engine(integration_settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(integration_settings)
    try:
        # The database name is validated above. Runtime databases are never cleared by tests.
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "TRUNCATE TABLE inbox_messages, outbox_messages, reconciliation_results, "
                    "attachments, shift_reports, template_versions, templates, memberships, "
                    "organizations, users"
                )
            )
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def db(integration_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    async with session_factory(integration_engine)() as session:
        transaction = await session.begin()
        try:
            yield session
        finally:
            if transaction.is_active:
                await transaction.rollback()


@pytest.fixture
async def cache(integration_settings: Settings) -> AsyncIterator[Redis]:
    client = Redis.from_url(integration_settings.redis_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()
