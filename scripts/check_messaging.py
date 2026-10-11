"""Check a running local Kafka consumer's handling of duplicate and invalid messages."""

import asyncio
import base64
import json
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, TopicPartition
from sqlalchemy import func, select

from shiftledger.config import get_settings
from shiftledger.database import create_engine, session_factory
from shiftledger.messaging.events import EventEnvelope
from shiftledger.messaging.kafka import connection_options, create_producer
from shiftledger.messaging.models import OutboxMessage
from shiftledger.reconciliation.models import ReconciliationResult


async def main() -> None:
    settings = get_settings()
    engine = create_engine(settings)
    sessions = session_factory(engine)
    producer = create_producer(settings)
    consumer = AIOKafkaConsumer(**connection_options(settings), enable_auto_commit=False)
    try:
        await producer.start()
        await consumer.start()
        partitions = [
            TopicPartition(settings.kafka_dlq_topic, partition)
            for partition in range(settings.kafka_topic_partitions)
        ]
        consumer.assign(partitions)
        offsets = await consumer.end_offsets(partitions)
        for partition, offset in offsets.items():
            consumer.seek(partition, offset)
        async with sessions() as session:
            message = await session.scalar(
                select(OutboxMessage)
                .where(
                    OutboxMessage.topic == settings.kafka_report_topic,
                    OutboxMessage.published_at.is_not(None),
                )
                .order_by(OutboxMessage.created_at.desc())
                .limit(1)
            )
            if message is None:
                raise SystemExit("Run scripts/smoke.py first")
            event = EventEnvelope.model_validate(message.event)
            before = await session.scalar(
                select(func.count())
                .select_from(ReconciliationResult)
                .where(ReconciliationResult.report_id == event.report_id)
            )
        await producer.send_and_wait(
            settings.kafka_report_topic,
            event.model_dump_json().encode(),
            key=str(event.report_id).encode(),
        )
        invalid = b'{"schema_version":99,"type":"unsupported"}'
        marker = str(uuid4()).encode()
        await producer.send_and_wait(settings.kafka_report_topic, invalid, key=marker)
        async with asyncio.timeout(30):
            while True:
                record = await consumer.getone()
                if record.key == marker:
                    dead_letter = json.loads(record.value)
                    if (
                        dead_letter["reason"] != "invalid_event"
                        or base64.b64decode(dead_letter["value_base64"]) != invalid
                    ):
                        raise RuntimeError("Invalid message was not preserved in the DLQ")
                    break
        async with sessions() as session:
            after = await session.scalar(
                select(func.count())
                .select_from(ReconciliationResult)
                .where(ReconciliationResult.report_id == event.report_id)
            )
        if before != 1 or after != 1:
            raise RuntimeError("Duplicate delivery produced an extra reconciliation result")
        print(
            json.dumps(
                {
                    "status": "passed",
                    "checks": ["duplicate_delivery", "invalid_event_dlq"],
                    "dead_letter_partition": record.partition,
                    "dead_letter_offset": record.offset,
                }
            )
        )
    finally:
        await consumer.stop()
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
