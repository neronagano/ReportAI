import asyncio
from datetime import timedelta
from typing import Protocol

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shiftledger.database import utc_now
from shiftledger.messaging.events import EventEnvelope
from shiftledger.messaging.models import OutboxMessage
from shiftledger.observability import OUTBOX_FAILURES, PUBLISHED_EVENTS

log = structlog.get_logger()


class EventPublisher(Protocol):
    async def publish(self, topic: str, key: str, event: EventEnvelope) -> None: ...


async def publish_next(session: AsyncSession, publisher: EventPublisher) -> bool:
    message = await session.scalar(
        select(OutboxMessage)
        .where(OutboxMessage.published_at.is_(None), OutboxMessage.next_attempt_at <= utc_now())
        .order_by(OutboxMessage.created_at, OutboxMessage.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if message is None:
        return False
    try:
        event = EventEnvelope.model_validate(message.event)
        async with asyncio.timeout(15):
            await publisher.publish(message.topic, message.partition_key, event)
    except Exception as exc:
        # Leave failed events pending indefinitely. They are never discarded after a retry limit.
        message.attempts += 1
        message.next_attempt_at = utc_now() + timedelta(
            seconds=min(2 ** min(message.attempts, 8), 300)
        )
        message.last_error = type(exc).__name__
        OUTBOX_FAILURES.inc()
        log.warning(
            "outbox_publish_failed",
            event_id=str(message.id),
            attempts=message.attempts,
            error_type=type(exc).__name__,
        )
    else:
        message.published_at = utc_now()
        message.last_error = None
        PUBLISHED_EVENTS.inc()
        log.info(
            "outbox_published", event_id=str(message.id), correlation_id=str(event.correlation_id)
        )
    await session.flush()
    return True
