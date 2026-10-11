import asyncio

from prometheus_client import start_http_server

from shiftledger.config import get_settings
from shiftledger.database import create_engine, session_factory
from shiftledger.messaging.events import EventEnvelope
from shiftledger.messaging.kafka import create_producer
from shiftledger.messaging.outbox import publish_next
from shiftledger.observability import configure_logging
from shiftledger.workers.runtime import heartbeat, stop_event, wait_or_stop


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    sessions = session_factory(engine)
    producer = create_producer(settings)
    stopped = stop_event()
    metrics_server, metrics_thread = start_http_server(9101)

    class KafkaPublisher:
        async def publish(self, topic: str, key: str, event: EventEnvelope) -> None:
            await producer.send_and_wait(topic, event.model_dump_json().encode(), key=key.encode())

    try:
        await producer.start()
        publisher = KafkaPublisher()
        while not stopped.is_set():
            async with sessions() as session, session.begin():
                processed = await publish_next(session, publisher)
            heartbeat("outbox")
            if not processed:
                await wait_or_stop(stopped, settings.outbox_poll_seconds)
    finally:
        await producer.stop()
        await engine.dispose()
        metrics_server.shutdown()
        metrics_server.server_close()
        metrics_thread.join(timeout=5)


if __name__ == "__main__":
    asyncio.run(run())
