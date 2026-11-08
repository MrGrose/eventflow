from uuid import uuid4

import pytest

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.infrastructure.messaging.kafka.publisher import KafkaPublisher
from eventflow.infrastructure.outbox.publish_loop import OutboxPublishLoop
from eventflow.infrastructure.outbox.repository import OutboxRepository


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_outbox_to_kafka_published(db_session_factory, kafka_session) -> None:
    base = get_settings()
    settings = Settings(**{**base.model_dump(), "messaging_transport": MessagingTransport.KAFKA})
    publisher = KafkaPublisher(kafka_session, settings)
    async with db_session_factory() as session:
        repo = OutboxRepository(session, settings)
        row = await repo.insert(
            build_envelope(
                event_type=EventType.ORDER_CONFIRMED,
                aggregate_type="order",
                aggregate_id=uuid4(),
                payload={"k": 1},
            )
        )
        await session.commit()
        loop = OutboxPublishLoop(session, publisher, settings, owner_id="kf-int")
        assert await loop.tick() >= 1
        refreshed = await repo.get_by_event_id(row.event_id)
        assert refreshed is not None
        assert refreshed.status == "published"
        assert refreshed.broker_meta.get("transport") == "kafka"
        assert "offset" in refreshed.broker_meta
