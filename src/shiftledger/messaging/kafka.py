import ssl
from contextlib import suppress
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from shiftledger.config import Settings


def connection_options(settings: Settings) -> dict[str, Any]:
    options: dict[str, Any] = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        "security_protocol": settings.kafka_security_protocol,
        "request_timeout_ms": 15000,
    }
    if settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
        options["ssl_context"] = ssl.create_default_context(cafile=settings.kafka_ssl_cafile)
    if settings.kafka_security_protocol.startswith("SASL"):
        options.update(
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_plain_username=settings.kafka_sasl_username,
            sasl_plain_password=settings.kafka_sasl_password.get_secret_value()
            if settings.kafka_sasl_password
            else None,
        )
    return options


def create_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        **connection_options(settings),
        client_id="shiftledger-publisher",
        enable_idempotence=True,
        acks="all",
        max_request_size=1024 * 1024,
    )


def create_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.kafka_report_topic,
        **connection_options(settings),
        client_id="shiftledger-reconciliation",
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
        max_poll_interval_ms=300000,
    )


async def ensure_topics(settings: Settings) -> None:
    admin = AIOKafkaAdminClient(**connection_options(settings), client_id="shiftledger-bootstrap")
    await admin.start()
    try:
        for name in [
            settings.kafka_report_topic,
            settings.kafka_completed_topic,
            settings.kafka_dlq_topic,
        ]:
            topic = NewTopic(
                name,
                num_partitions=settings.kafka_topic_partitions,
                replication_factor=settings.kafka_replication_factor,
                topic_configs={
                    "min.insync.replicas": "2" if settings.kafka_replication_factor >= 3 else "1",
                    "retention.ms": "604800000"
                    if name != settings.kafka_dlq_topic
                    else "2592000000",
                },
            )
            with suppress(TopicAlreadyExistsError):
                await admin.create_topics([topic])
    finally:
        await admin.close()
