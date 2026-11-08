import json
import uuid

import pytest
from aiokafka import AIOKafkaConsumer

from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest
from eventflow.infrastructure.messaging.kafka.publisher import KafkaPublisher
from eventflow.infrastructure.messaging.kafka.session import KafkaSession


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_publish_consume_roundtrip(kafka_session) -> None:
    settings = get_settings()
    publisher = KafkaPublisher(kafka_session, settings)
    agg_id = str(uuid.uuid4())
    req = PublishRequest(
        event_id=str(uuid.uuid4()),
        event_type="order.confirmed",
        aggregate_type="order",
        aggregate_id=agg_id,
        payload={"n": 1},
        target_transport="kafka",
    )
    result = await publisher.publish(req)
    assert result.accepted, result.error
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_events,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-{uuid.uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        import time

        start = time.monotonic()
        found = None
        while time.monotonic() - start < 12:
            batch = await consumer.getmany(timeout_ms=500, max_records=20)
            for records in batch.values():
                for msg in records:
                    body = json.loads(msg.value.decode())
                    if body.get("event_id") == req.event_id:
                        found = msg
                        break
            if found:
                break
        assert found is not None
        assert found.key.decode() == agg_id
    finally:
        await consumer.stop()


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_same_key_same_partition(kafka_session) -> None:
    settings = get_settings()
    publisher = KafkaPublisher(kafka_session, settings)
    agg_id = str(uuid.uuid4())
    partitions: set[int] = set()
    for _ in range(3):
        req = PublishRequest(
            event_id=str(uuid.uuid4()),
            event_type="order.created",
            aggregate_type="order",
            aggregate_id=agg_id,
            payload={},
            target_transport="kafka",
        )
        result = await publisher.publish(req)
        assert result.accepted
        partitions.add(result.broker_meta["partition"])
    assert len(partitions) == 1


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_broker_unavailable() -> None:
    bad = Settings(kafka_bootstrap_servers="127.0.0.1:59888", messaging_transport="kafka")
    session = KafkaSession(bad)
    publisher = KafkaPublisher(session, bad)
    req = PublishRequest(
        event_id=str(uuid.uuid4()),
        event_type="order.created",
        aggregate_type="order",
        aggregate_id=str(uuid.uuid4()),
        payload={},
        target_transport="kafka",
    )
    result = await publisher.publish(req)
    assert not result.accepted
