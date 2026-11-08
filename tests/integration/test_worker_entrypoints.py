import asyncio
import time
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.infrastructure.db.models.inventory import StockItem
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.outbox import OutboxEventModel
from eventflow.infrastructure.db.models.payment import PaymentModel
from eventflow.infrastructure.messaging.kafka.consumer import KafkaConsumerWorker
from eventflow.infrastructure.messaging.rabbitmq.consumer import RabbitConsumerWorker
from eventflow.infrastructure.outbox.repository import OutboxRepository
from eventflow.workers.outbox_publisher import _run_loop as run_outbox_publisher_loop
from eventflow.workers.rabbitmq_worker import build_rabbit_message_handler


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_rabbit_worker_graceful_shutdown_when_idle_closes_session() -> None:
    settings = get_settings()

    async def noop_handler(_payload: dict, _msg) -> None:
        return

    worker = RabbitConsumerWorker("billing", noop_handler, settings)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.3)
    worker.request_stop()
    await asyncio.wait_for(task, timeout=5.0)
    assert worker._session.connection is None


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_kafka_worker_graceful_shutdown_when_idle_closes_session() -> None:
    base = get_settings()
    settings = Settings(**{**base.model_dump(), "messaging_transport": MessagingTransport.KAFKA})

    async def noop_handler(_payload: dict, _key: bytes) -> None:
        return

    worker = KafkaConsumerWorker("billing", noop_handler, settings)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.8)
    worker.request_stop()
    await asyncio.wait_for(task, timeout=5.0)
    assert worker._session._producer is None


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_outbox_publisher_and_rabbit_worker_entrypoints_end_to_end(
    db_session_factory, monkeypatch
) -> None:
    monkeypatch.setenv("MESSAGING_TRANSPORT", "rabbitmq")
    settings = get_settings()
    order_id = uuid4()
    sku = f"SKU-WKR-{uuid4().hex[:8]}"
    async with db_session_factory() as session:
        session.add(OrderModel(id=order_id, customer_id="c-wkr", status="confirmed"))
        session.add(StockItem(sku=sku, available=4))
        repo = OutboxRepository(session, settings)
        row = await repo.insert(
            build_envelope(
                event_type=EventType.ORDER_CONFIRMED,
                aggregate_type="order",
                aggregate_id=order_id,
                payload={
                    "customer_id": "c-wkr",
                    "total": "20.00",
                    "items": [{"sku": sku, "quantity": 1, "unit_price": "20.00"}],
                },
            )
        )
        await session.commit()
    stop_event = asyncio.Event()
    pub_task = asyncio.create_task(
        run_outbox_publisher_loop(poll_seconds=0.1, stop_event=stop_event)
    )
    billing_worker = RabbitConsumerWorker(
        "billing", build_rabbit_message_handler("billing", db_session_factory), settings
    )
    inventory_worker = RabbitConsumerWorker(
        "inventory", build_rabbit_message_handler("inventory", db_session_factory), settings
    )
    bill_task = asyncio.create_task(billing_worker.run())
    inv_task = asyncio.create_task(inventory_worker.run())
    try:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            async with db_session_factory() as session:
                outbox_row = await session.get(OutboxEventModel, row.id)
                payment = await session.scalar(
                    select(PaymentModel).where(PaymentModel.order_id == order_id)
                )
                stock = await session.get(StockItem, sku)
                if (
                    outbox_row is not None
                    and outbox_row.status == "published"
                    and (payment is not None)
                    and (stock is not None)
                    and (stock.available == 3)
                ):
                    assert payment.amount == Decimal("20.00")
                    break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("workers did not complete end-to-end processing within timeout")
    finally:
        stop_event.set()
        billing_worker.request_stop()
        inventory_worker.request_stop()
        await asyncio.wait_for(asyncio.gather(pub_task, bill_task, inv_task), timeout=5.0)
    assert billing_worker._session.connection is None
    assert inventory_worker._session.connection is None
