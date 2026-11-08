import asyncio
import os
import subprocess

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from eventflow.config import get_settings
from eventflow.infrastructure.messaging.kafka.bootstrap import ensure_topics
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.messaging.rabbitmq.topology import (
    CONSUMER_NAMES,
    queue_dlq,
    queue_main,
    queue_retry,
)


async def _purge_rabbit() -> None:
    session = RabbitMQSession()
    await session.connect()
    channel = await session.consumer_channel()
    for name in CONSUMER_NAMES:
        for qn in (queue_main(name), queue_retry(name), queue_dlq(name)):
            queue = await channel.declare_queue(qn, passive=True)
            await queue.purge()
    await session.close()


async def _reset_kafka_topics() -> None:
    from aiokafka.admin import AIOKafkaAdminClient

    settings = get_settings()
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        topics = {settings.kafka_topic_events, settings.kafka_topic_retry, settings.kafka_topic_dlt}
        existing = await admin.list_topics()
        delete = [t for t in topics if t in existing]
        if delete:
            await admin.delete_topics(delete)
            await asyncio.sleep(2)
    finally:
        await admin.close()
    await ensure_topics(settings)


async def _truncate_postgres() -> None:
    settings = get_settings()
    engine = create_async_engine(settings.database_url)
    sql = (
        "TRUNCATE processed_events, outbox_events, order_items, orders, payments, "
        "stock_items RESTART IDENTITY CASCADE"
    )
    async with engine.begin() as conn:
        await conn.execute(text(sql))
    await engine.dispose()


def _alembic_upgrade() -> None:
    subprocess.run(["uv", "run", "alembic", "upgrade", "head"], check=False, env=os.environ.copy())


async def main() -> None:
    _alembic_upgrade()
    await _truncate_postgres()
    await _purge_rabbit()
    await _reset_kafka_topics()
    print("lab-reset: ok")


if __name__ == "__main__":
    asyncio.run(main())
