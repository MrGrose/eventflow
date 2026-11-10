import asyncio
import json
import time
import uuid
from decimal import Decimal

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.domain.events.types import EventType
from eventflow.domain.reliability.errors import PermanentProcessingError, RetryableProcessingError
from eventflow.infrastructure.db.models.inventory import StockItem
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.payment import PaymentModel
from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest
from eventflow.infrastructure.messaging.kafka import metrics as kafka_metrics
from eventflow.infrastructure.messaging.kafka.consumer import (
    KafkaConsumerWorker,
    PermanentMessageError,
)
from eventflow.infrastructure.messaging.kafka.publisher import KafkaPublisher
from eventflow.workers.kafka_worker import build_kafka_message_handler


def _inner_payload(body: dict) -> dict:
    return body.get("payload") or {}


async def _find_in_topic(settings: Settings, topic: str, event_id: str, *, timeout: float = 15.0):
    reader = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-reader-{uuid.uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await reader.start()
    try:
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            batch = await reader.getmany(timeout_ms=300)
            for records in batch.values():
                for msg in records:
                    body = json.loads(msg.value.decode())
                    if body.get("event_id") == event_id:
                        return msg
        return None
    finally:
        await reader.stop()


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_manual_commit_and_dlt(kafka_session) -> None:
    kafka_metrics.reset()
    settings = get_settings()
    publisher = KafkaPublisher(kafka_session, settings)
    event_id = str(uuid.uuid4())
    req = PublishRequest(
        event_id=event_id,
        event_type="order.confirmed",
        aggregate_type="order",
        aggregate_id=str(uuid.uuid4()),
        payload={"poison": True},
        target_transport="kafka",
    )
    await publisher.publish(req)
    processed = False

    async def handler(body: dict, _key: bytes) -> None:
        nonlocal processed
        if body.get("event_id") != event_id:
            return
        processed = True
        if _inner_payload(body).get("poison"):
            raise PermanentMessageError("poison")

    worker = KafkaConsumerWorker("billing", handler, session=kafka_session)
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline and (not processed):
        await worker.run(max_messages=1)
        worker._stop.clear()
    assert processed
    assert kafka_metrics.snapshot().get("kafka_consumer_billing_dlt", 0) >= 1
    found = await _find_in_topic(settings, settings.kafka_topic_dlt, event_id, timeout=20.0)
    assert found is not None


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_consumer_commits_offset(kafka_session) -> None:
    settings = get_settings()
    publisher = KafkaPublisher(kafka_session, settings)
    req = PublishRequest(
        event_id=str(uuid.uuid4()),
        event_type="order.created",
        aggregate_type="order",
        aggregate_id=str(uuid.uuid4()),
        payload={"ok": True},
        target_transport="kafka",
    )
    await publisher.publish(req)
    seen: list[str] = []

    async def handler(body: dict, _key: bytes) -> None:
        if body.get("event_id") == req.event_id:
            seen.append(body["event_id"])

    worker = KafkaConsumerWorker("inventory", handler, session=kafka_session)
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline and (not seen):
        await worker.run(max_messages=1)
        worker._stop.clear()
    assert seen == [req.event_id]


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_kafka_retry_lifecycle_reconsumes_and_succeeds(kafka_session) -> None:
    base = get_settings()
    settings = Settings(
        **{
            **base.model_dump(),
            "messaging_transport": MessagingTransport.KAFKA,
            "kafka_max_retries": 2,
            "kafka_retry_backoff_ms": 50,
        }
    )
    publisher = KafkaPublisher(kafka_session, settings)
    event_id = str(uuid.uuid4())
    req = PublishRequest(
        event_id=event_id,
        event_type="order.confirmed",
        aggregate_type="order",
        aggregate_id=str(uuid.uuid4()),
        payload={"retry_case": "recover"},
        target_transport="kafka",
    )
    await publisher.publish(req)
    attempts = 0

    async def flaky_handler(body: dict, _key: bytes) -> None:
        nonlocal attempts
        if body.get("event_id") != event_id:
            return
        attempts += 1
        if attempts == 1:
            raise RetryableProcessingError("transient broker/db error")

    worker = KafkaConsumerWorker("billing", flaky_handler, settings, session=kafka_session)
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline and attempts < 2:
        await worker.run(max_messages=1)
        worker._stop.clear()
    assert attempts == 2
    retry_msg = await _find_in_topic(settings, settings.kafka_topic_retry, event_id, timeout=10.0)
    assert retry_msg is not None
    headers_dict = {k: v.decode() for k, v in retry_msg.headers or []}
    assert headers_dict.get("retry") == "1"
    assert headers_dict.get("target_consumer") == "billing"


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_kafka_retry_exhausted_moves_to_dlt(kafka_session) -> None:
    base = get_settings()
    settings = Settings(
        **{
            **base.model_dump(),
            "messaging_transport": MessagingTransport.KAFKA,
            "kafka_max_retries": 2,
            "kafka_retry_backoff_ms": 20,
        }
    )
    publisher = KafkaPublisher(kafka_session, settings)
    event_id = str(uuid.uuid4())
    req = PublishRequest(
        event_id=event_id,
        event_type="order.confirmed",
        aggregate_type="order",
        aggregate_id=str(uuid.uuid4()),
        payload={"retry_case": "exhaust"},
        target_transport="kafka",
    )
    await publisher.publish(req)
    attempts = 0

    async def always_retryable_handler(body: dict, _key: bytes) -> None:
        nonlocal attempts
        if body.get("event_id") != event_id:
            return
        attempts += 1
        raise RetryableProcessingError(f"still failing attempt {attempts}")

    worker = KafkaConsumerWorker(
        "billing", always_retryable_handler, settings, session=kafka_session
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and attempts < 3:
        await worker.run(max_messages=1)
        worker._stop.clear()
    assert attempts == 3
    dlt_msg = await _find_in_topic(settings, settings.kafka_topic_dlt, event_id, timeout=15.0)
    assert dlt_msg is not None
    headers_dict = {k: v.decode() for k, v in dlt_msg.headers or []}
    assert headers_dict.get("error") == "retry_exhausted"


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_unexpected_error_is_retried_before_dlt(kafka_session) -> None:
    base = get_settings()
    settings = Settings(
        **{
            **base.model_dump(),
            "messaging_transport": MessagingTransport.KAFKA,
            "kafka_max_retries": 2,
            "kafka_retry_backoff_ms": 20,
        }
    )
    publisher = KafkaPublisher(kafka_session, settings)
    event_id = str(uuid.uuid4())
    await publisher.publish(
        PublishRequest(
            event_id=event_id,
            event_type="order.confirmed",
            aggregate_type="order",
            aggregate_id=str(uuid.uuid4()),
            payload={"unexpected_case": "recover"},
            target_transport="kafka",
        )
    )
    attempts = 0

    async def buggy_handler(body: dict, _key: bytes) -> None:
        nonlocal attempts
        if body.get("event_id") != event_id:
            return
        attempts += 1
        if attempts == 1:
            raise RuntimeError("database connection dropped")

    worker = KafkaConsumerWorker("billing", buggy_handler, settings, session=kafka_session)
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline and attempts < 2:
        await worker.run(max_messages=1)
        worker._stop.clear()
    assert attempts == 2
    retry_msg = await _find_in_topic(settings, settings.kafka_topic_retry, event_id, timeout=10.0)
    assert retry_msg is not None
    headers_dict = {k: v.decode() for k, v in retry_msg.headers or []}
    assert headers_dict.get("retry") == "1"


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_kafka_consumer_groups_billing_and_inventory_independent(
    kafka_session, db_session_factory
) -> None:
    base = get_settings()
    settings = Settings(**{**base.model_dump(), "messaging_transport": MessagingTransport.KAFKA})
    order_id = uuid.uuid4()
    event_id = uuid.uuid4()
    sku = f"SKU-GRP-{uuid.uuid4().hex[:8]}"
    async with db_session_factory() as session:
        session.add(OrderModel(id=order_id, customer_id="cust-grp", status="confirmed"))
        session.add(StockItem(sku=sku, available=5))
        await session.commit()
    publisher = KafkaPublisher(kafka_session, settings)
    req = PublishRequest(
        event_id=str(event_id),
        event_type=EventType.ORDER_CONFIRMED.value,
        aggregate_type="order",
        aggregate_id=str(order_id),
        payload={
            "customer_id": "cust-grp",
            "total": "55.00",
            "items": [{"sku": sku, "quantity": 2, "unit_price": "27.50"}],
        },
        target_transport="kafka",
    )
    pub_res = await publisher.publish(req)
    assert pub_res.accepted
    billing_worker = KafkaConsumerWorker(
        "billing",
        build_kafka_message_handler("billing", db_session_factory),
        settings,
        session=kafka_session,
    )
    inventory_worker = KafkaConsumerWorker(
        "inventory",
        build_kafka_message_handler("inventory", db_session_factory),
        settings,
        session=kafka_session,
    )

    async def poll_billing() -> None:
        deadline = time.monotonic() + 25.0
        while time.monotonic() < deadline:
            await billing_worker.run(max_messages=1)
            billing_worker._stop.clear()
            async with db_session_factory() as s:
                if await s.scalar(select(PaymentModel).where(PaymentModel.order_id == order_id)):
                    return

    async def poll_inventory() -> None:
        deadline = time.monotonic() + 25.0
        while time.monotonic() < deadline:
            await inventory_worker.run(max_messages=1)
            inventory_worker._stop.clear()
            async with db_session_factory() as s:
                st = await s.get(StockItem, sku)
                if st is not None and st.available == 3:
                    return

    await asyncio.gather(poll_billing(), poll_inventory())
    async with db_session_factory() as session:
        payment = await session.scalar(
            select(PaymentModel).where(PaymentModel.order_id == order_id)
        )
        assert payment is not None
        assert payment.amount == Decimal("55.00")
        stock = await session.get(StockItem, sku)
        assert stock is not None
        assert stock.available == 3
        processed = (
            await session.scalars(
                select(ProcessedEventModel).where(ProcessedEventModel.event_id == event_id)
            )
        ).all()
        assert {row.consumer_name for row in processed} == {"billing", "inventory"}
        assert isinstance(PermanentProcessingError("check"), Exception)
