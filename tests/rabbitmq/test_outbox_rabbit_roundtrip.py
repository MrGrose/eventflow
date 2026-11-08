from uuid import uuid4

import pytest

from eventflow.config import get_settings
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.infrastructure.messaging.rabbitmq.publisher import RabbitMQPublisher
from eventflow.infrastructure.messaging.rabbitmq.topology import queue_main
from eventflow.infrastructure.outbox.publish_loop import OutboxPublishLoop
from eventflow.infrastructure.outbox.repository import OutboxRepository


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_outbox_tick_publishes_to_rabbit(db_session_factory, rabbit_session) -> None:
    settings = get_settings()
    publisher = RabbitMQPublisher(rabbit_session, settings)
    async with db_session_factory() as session:
        repo = OutboxRepository(session, settings)
        row = await repo.insert(
            build_envelope(
                event_type=EventType.ORDER_CONFIRMED,
                aggregate_type="order",
                aggregate_id=uuid4(),
                payload={"ok": True},
            )
        )
        await session.commit()
        loop = OutboxPublishLoop(session, publisher, settings, owner_id="rb-int")
        assert await loop.tick() >= 1
        refreshed = await repo.get_by_event_id(row.event_id)
        assert refreshed is not None
        assert refreshed.status == "published"
        assert refreshed.broker_meta.get("transport") == "rabbitmq"
    channel = await rabbit_session.consumer_channel()
    message = await (await channel.get_queue(queue_main("billing"))).get(timeout=5, fail=False)
    assert message is not None
    await message.ack()
