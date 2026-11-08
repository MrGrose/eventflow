import argparse
import asyncio
import logging
import os
import signal
import socket
from contextlib import suppress

from sqlalchemy.ext.asyncio import async_sessionmaker

from eventflow.config import get_settings
from eventflow.infrastructure.db.session import create_engine
from eventflow.infrastructure.logging import configure_logging
from eventflow.infrastructure.messaging.factory import publisher_for_settings
from eventflow.infrastructure.messaging.kafka.session import KafkaSession
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.outbox.publish_loop import OutboxPublishLoop

logger = logging.getLogger(__name__)


def _owner_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


async def _run_loop(*, poll_seconds: float, stop_event: asyncio.Event | None = None) -> None:
    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    settings = get_settings()
    engine = create_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    rabbit: RabbitMQSession | None = None
    kafka: KafkaSession | None = None
    if settings.messaging_transport.value == "rabbitmq":
        rabbit = RabbitMQSession(settings)
        await rabbit.connect()
    if settings.messaging_transport.value == "kafka":
        kafka = KafkaSession(settings)
        await kafka.start()
    publisher = publisher_for_settings(settings, rabbit_session=rabbit, kafka_session=kafka)
    owner = _owner_id()
    logger.info(
        "outbox_publisher_started owner=%s transport=%s", owner, settings.messaging_transport.value
    )
    try:
        while not stop.is_set():
            async with factory() as session:
                publish_loop = OutboxPublishLoop(session, publisher, settings, owner_id=owner)
                try:
                    count = await publish_loop.tick()
                    if count:
                        logger.info("outbox_published_batch count=%s", count)
                except Exception:
                    logger.exception("outbox_tick_failed")
                    await session.rollback()
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
    finally:
        if rabbit is not None:
            await rabbit.close()
        if kafka is not None:
            await kafka.close()
        await engine.dispose()


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--poll", type=float, default=2.0)
    args = parser.parse_args()
    asyncio.run(_run_loop(poll_seconds=args.poll))


if __name__ == "__main__":
    main()
