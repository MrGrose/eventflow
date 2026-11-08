import json

import pytest

from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest
from eventflow.infrastructure.messaging.rabbitmq.publisher import RabbitMQPublisher
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.messaging.rabbitmq.topology import queue_main, routing_key


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_publisher_confirm_and_consume(rabbit_session) -> None:
    settings = get_settings()
    publisher = RabbitMQPublisher(rabbit_session, settings)
    req = PublishRequest(
        event_id="11111111-1111-1111-1111-111111111101",
        event_type="order.confirmed",
        aggregate_type="order",
        aggregate_id="22222222-2222-2222-2222-222222222202",
        payload={"customer_id": "c-1"},
        target_transport="rabbitmq",
    )
    result = await publisher.publish(req)
    assert result.accepted, result.error
    assert result.broker_meta["routing_key"] == routing_key("order", "order.confirmed")
    channel = await rabbit_session.consumer_channel()
    queue = await channel.get_queue(queue_main("billing"))
    message = await queue.get(timeout=5, fail=False)
    assert message is not None
    body = json.loads(message.body.decode())
    assert body["event_id"] == req.event_id
    await message.ack()


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_publisher_channel_recovery(rabbit_session) -> None:
    settings = get_settings()
    publisher = RabbitMQPublisher(rabbit_session, settings)
    req = PublishRequest(
        event_id="11111111-1111-1111-1111-111111111102",
        event_type="order.confirmed",
        aggregate_type="order",
        aggregate_id="22222222-2222-2222-2222-222222222203",
        payload={},
        target_transport="rabbitmq",
    )
    ch = await rabbit_session.publisher_channel()
    await ch.close()
    rabbit_session._publisher_channel = None
    result = await publisher.publish(req)
    assert result.accepted, result.error


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_publisher_broker_unavailable() -> None:
    bad = Settings(
        rabbitmq_url="amqp://guest:guest@127.0.0.1:59777/", rabbitmq_publish_confirm_timeout=2.0
    )
    session = RabbitMQSession(bad)
    publisher = RabbitMQPublisher(session, bad)
    req = PublishRequest(
        event_id="11111111-1111-1111-1111-111111111199",
        event_type="order.created",
        aggregate_type="order",
        aggregate_id="22222222-2222-2222-2222-222222222299",
        payload={},
        target_transport="rabbitmq",
    )
    result = await publisher.publish(req)
    assert not result.accepted
    assert result.error
