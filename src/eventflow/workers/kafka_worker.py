import argparse
import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventflow.application.consumers.dispatch import ACTIVE_CONSUMERS, dispatch_consumer_event
from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.infrastructure.db.session import create_engine
from eventflow.infrastructure.logging import configure_logging
from eventflow.infrastructure.messaging.kafka.consumer import KafkaConsumerWorker

logger = logging.getLogger(__name__)


def build_kafka_message_handler(
    consumer_name: str, session_factory: async_sessionmaker[AsyncSession]
) -> Callable[[dict, bytes], Awaitable[None]]:
    if consumer_name not in ACTIVE_CONSUMERS:
        raise NotImplementedError(f"consumer '{consumer_name}' is not implemented")

    async def _handle(envelope: dict, _key: bytes) -> None:
        async with session_factory() as session:
            await dispatch_consumer_event(
                session,
                consumer_name=consumer_name,
                envelope=envelope,
                transport=MessagingTransport.KAFKA.value,
            )

    return _handle


async def _run_worker(consumer_name: str, settings: Settings | None = None) -> None:
    cfg = settings or get_settings()
    engine = create_engine(cfg.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    handler = build_kafka_message_handler(consumer_name, session_factory)
    worker = KafkaConsumerWorker(consumer_name, handler, settings=cfg)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, worker.request_stop)
    try:
        await worker.run()
    finally:
        await engine.dispose()


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumer", required=True, choices=list(ACTIVE_CONSUMERS))
    args = parser.parse_args()
    asyncio.run(_run_worker(args.consumer))


if __name__ == "__main__":
    main()
