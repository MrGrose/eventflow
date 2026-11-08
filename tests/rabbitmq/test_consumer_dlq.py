import asyncio
import json
import time
from uuid import uuid4

import aio_pika
import pytest

from eventflow.config import get_settings
from eventflow.domain.reliability.errors import PermanentProcessingError, RetryableProcessingError
from eventflow.infrastructure.messaging.rabbitmq.consumer import RabbitConsumerWorker
from eventflow.infrastructure.messaging.rabbitmq.topology import queue_dlq, queue_main, routing_key


async def _poll_queue_message(queue, *, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = await queue.get(timeout=1, fail=False)
        if msg is not None:
            return msg
        await asyncio.sleep(0.05)
    return None


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_poison_message_goes_to_dlq(rabbit_session) -> None:
    settings = get_settings()
    channel = await rabbit_session.consumer_channel()
    exchange = await channel.get_exchange(settings.rabbitmq_exchange_events)
    event_id = str(uuid4())
    body = json.dumps(
        {"event_id": event_id, "event_type": "order.confirmed", "payload": {}}
    ).encode()
    await exchange.publish(
        aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key=routing_key("order", "order.confirmed"),
    )
    queue = await channel.get_queue(queue_main("billing"))
    message = await _poll_queue_message(queue, timeout=5.0)
    assert message is not None
    await message.nack(requeue=False)
    dlq = await channel.get_queue(queue_dlq("billing"))
    dead = await _poll_queue_message(dlq, timeout=5.0)
    assert dead is not None
    dead_body = json.loads(dead.body.decode())
    assert dead_body["event_id"] == event_id
    await dead.ack()


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_manual_ack_after_handler(rabbit_session) -> None:
    settings = get_settings()
    channel = await rabbit_session.consumer_channel()
    exchange = await channel.get_exchange(settings.rabbitmq_exchange_events)
    event_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps({"event_id": event_id, "event_type": "order.confirmed"}).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key("order", "order.confirmed"),
    )
    queue = await channel.get_queue(queue_main("billing"))
    message = await _poll_queue_message(queue, timeout=5.0)
    assert message is not None
    body = json.loads(message.body.decode())
    assert body.get("event_id") == event_id
    await message.ack()


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_permanent_error_in_worker_routes_to_dlq_without_retry(rabbit_session) -> None:
    settings = get_settings()
    channel = await rabbit_session.consumer_channel()
    exchange = await channel.get_exchange(settings.rabbitmq_exchange_events)
    event_id = str(uuid4())
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps({"event_id": event_id, "event_type": "order.confirmed"}).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key("order", "order.confirmed"),
    )
    attempts = 0

    async def failing_handler(payload: dict, _msg) -> None:
        nonlocal attempts
        if payload.get("event_id") == event_id:
            attempts += 1
            raise PermanentProcessingError("unrecoverable domain error")

    worker = RabbitConsumerWorker("billing", failing_handler, settings, session=rabbit_session)
    await worker.run(max_messages=1)
    assert attempts == 1
    dlq = await channel.get_queue(queue_dlq("billing"))
    dead = await _poll_queue_message(dlq, timeout=5.0)
    assert dead is not None
    assert json.loads(dead.body.decode())["event_id"] == event_id
    await dead.ack()


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_retryable_error_requeues_and_succeeds_on_second_attempt(rabbit_session) -> None:
    settings = get_settings()
    channel = await rabbit_session.consumer_channel()
    exchange = await channel.get_exchange(settings.rabbitmq_exchange_events)
    event_id = str(uuid4())
    await exchange.publish(
        aio_pika.Message(
            body=json.dumps({"event_id": event_id, "event_type": "order.confirmed"}).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key("order", "order.confirmed"),
    )
    attempts = 0

    async def flaky_handler(payload: dict, _msg) -> None:
        nonlocal attempts
        if payload.get("event_id") == event_id:
            attempts += 1
            if attempts == 1:
                raise RetryableProcessingError("transient glitch")

    worker = RabbitConsumerWorker("billing", flaky_handler, settings, session=rabbit_session)
    await worker.run(max_messages=2)
    assert attempts == 2
    main_q = await channel.get_queue(queue_main("billing"))
    leftover = await main_q.get(timeout=1, fail=False)
    assert leftover is None
