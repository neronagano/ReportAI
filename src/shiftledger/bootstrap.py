from dataclasses import dataclass

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from shiftledger.attachments.storage import S3Storage
from shiftledger.config import Settings
from shiftledger.database import create_engine, session_factory


@dataclass(frozen=True)
class Resources:
    settings: Settings
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    redis: Redis
    storage: S3Storage

    @classmethod
    def create(cls, settings: Settings) -> "Resources":
        engine = create_engine(settings)
        cache = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        return cls(settings, engine, session_factory(engine), cache, S3Storage(settings))

    async def close(self) -> None:
        await self.redis.aclose()
        await self.storage.close()
        await self.engine.dispose()
