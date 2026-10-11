import asyncio
import base64
import hashlib
import json
from typing import Any

import structlog
from aiokafka import TopicPartition
from prometheus_client import start_http_server
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shiftledger.config import Settings, get_settings
from shiftledger.database import create_engine, session_factory, utc_now
from shiftledger.errors import PermanentMessageError
from shiftledger.messaging.events import EventEnvelope
from shiftledger.messaging.kafka import create_consumer, create_producer
from shiftledger.observability import PROCESSED_EVENTS, configure_logging
from shiftledger.reconciliation.service import process_report_event
from shiftledger.workers.runtime import heartbeat, stop_event, wait_or_stop

log = structlog.get_logger()


async def handle_message(
    record: Any,
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    stopped: asyncio.Event,
) -> str | None:
    try:
        if len(record.value or b"") > 64 * 1024:
            return "oversized_event"
        event = EventEnvelope.model_validate_json(record.value)
    except (ValidationError, ValueError):
        return "invalid_event"
    context = structlog.contextvars.bind_contextvars(
        correlation_id=str(event.correlation_id), event_id=str(event.id)
    )
    try:
        for attempt in range(1, settings.worker_max_attempts + 1):
            heartbeat("reconciliation")
            if stopped.is_set():
                raise asyncio.CancelledError
            try:
                async with sessions() as session, session.begin():
                    changed = await process_report_event(session, event, settings)
                PROCESSED_EVENTS.labels("processed" if changed else "duplicate").inc()
                log.info(
                    "report_event_processed", report_id=str(event.report_id), duplicate=not changed
                )
                return None
            except PermanentMessageError as exc:
                return type(exc).__name__
            except Exception as exc:
                log.warning(
                    "report_processing_failed", attempt=attempt, error_type=type(exc).__name__
                )
                if attempt == settings.worker_max_attempts:
                    return "retry_exhausted:" + type(exc).__name__
                await wait_or_stop(stopped, min(2**attempt, 30))
        raise RuntimeError("Unreachable retry state")
    finally:
        structlog.contextvars.reset_contextvars(**context)


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    sessions = session_factory(engine)
    consumer, producer = create_consumer(settings), create_producer(settings)
    stopped = stop_event()
    metrics_server, metrics_thread = start_http_server(9102)
    try:
        await producer.start()
        await consumer.start()
        while not stopped.is_set():
            batches = await consumer.getmany(timeout_ms=1000, max_records=1)
            for partition, records in batches.items():
                for record in records:
                    reason = await handle_message(record, sessions, settings, stopped)
                    if reason:
                        dead_letter = {
                            "schema_version": 1,
                            "source_topic": record.topic,
                            "source_partition": record.partition,
                            "source_offset": record.offset,
                            "reason": reason,
                            "failed_at": utc_now().isoformat(),
                            "value_base64": base64.b64encode(
                                (record.value or b"")[: 64 * 1024]
                            ).decode(),
                            "payload_truncated": len(record.value or b"") > 64 * 1024,
                            "payload_sha256": hashlib.sha256(record.value or b"").hexdigest(),
                        }
                        # Commit the original offset only after the DLQ broker acknowledges it.
                        async with asyncio.timeout(15):
                            await producer.send_and_wait(
                                settings.kafka_dlq_topic,
                                json.dumps(dead_letter).encode(),
                                key=record.key,
                            )
                        PROCESSED_EVENTS.labels("dead_letter").inc()
                        log.error(
                            "event_dead_lettered",
                            partition=record.partition,
                            offset=record.offset,
                            reason=reason,
                        )
                    await consumer.commit(
                        {TopicPartition(partition.topic, partition.partition): record.offset + 1}
                    )
            heartbeat("reconciliation")
    finally:
        await consumer.stop()
        await producer.stop()
        await engine.dispose()
        metrics_server.shutdown()
        metrics_server.server_close()
        metrics_thread.join(timeout=5)


if __name__ == "__main__":
    asyncio.run(run())
