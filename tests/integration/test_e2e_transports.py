import json
import time
from decimal import Decimal
from uuid import UUID

import aio_pika
import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import func, select

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.domain.events.types import EventType
from eventflow.infrastructure.db.models.inventory import StockItem
from eventflow.infrastructure.db.models.outbox import OutboxEventModel
from eventflow.infrastructure.db.models.payment import PaymentModel
from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel
from eventflow.infrastructure.messaging.kafka.consumer import KafkaConsumerWorker
from eventflow.infrastructure.messaging.kafka.publisher import KafkaPublisher
from eventflow.infrastructure.messaging.kafka.session import KafkaSession
from eventflow.infrastructure.messaging.rabbitmq.consumer import RabbitConsumerWorker
from eventflow.infrastructure.messaging.rabbitmq.publisher import RabbitMQPublisher
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.messaging.rabbitmq.topology import queue_main, routing_key
from eventflow.infrastructure.outbox.publish_loop import OutboxPublishLoop
from eventflow.infrastructure.outbox.repository import OutboxRepository
from eventflow.workers.kafka_worker import build_kafka_message_handler
from eventflow.workers.rabbitmq_worker import build_rabbit_message_handler


def _settings_for(transport: MessagingTransport) -> Settings:
    base = get_settings()
    return Settings(**{**base.model_dump(), "messaging_transport": transport})


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_e2e_api_to_rabbit_consumers_billing_and_inventory(
    api_client, db_session_factory, monkeypatch
) -> None:
    monkeypatch.setenv("MESSAGING_TRANSPORT", "rabbitmq")
    settings = _settings_for(MessagingTransport.RABBITMQ)
    sku = "SKU-E2E-RABBIT"
    async with db_session_factory() as session:
        session.add(StockItem(sku=sku, available=10))
        await session.commit()
    create_resp = await api_client.post(
        "/orders",
        json={
            "customer_id": "cust-e2e-rabbit",
            "items": [{"sku": sku, "quantity": 3, "unit_price": "12.50"}],
        },
    )
    assert create_resp.status_code == 201
    order_id = UUID(create_resp.json()["id"])
    confirm_resp = await api_client.post(f"/orders/{order_id}/confirm")
    assert confirm_resp.status_code == 200
    assert confirm_resp.json()["status"] == "confirmed"
    assert confirm_resp.json()["total"] == "37.50"
    rabbit = RabbitMQSession(settings)
    await rabbit.connect()
    publisher = RabbitMQPublisher(rabbit, settings)
    try:
        async with db_session_factory() as session:
            repo = OutboxRepository(session, settings)
            pending = await repo.list_pending()
            confirmed_row = next(
                r
                for r in pending
                if r.aggregate_id == order_id and r.event_type == EventType.ORDER_CONFIRMED.value
            )
            assert confirmed_row.payload["total"] == "37.50"
            assert confirmed_row.payload["items"] == [
                {"sku": sku, "quantity": 3, "unit_price": "12.50"}
            ]
            loop = OutboxPublishLoop(session, publisher, settings, owner_id="e2e-rabbit")
            published_count = await loop.tick()
            assert published_count == 2
            refreshed = await repo.get_by_event_id(confirmed_row.event_id)
            assert refreshed is not None
            assert refreshed.status == "published"
        billing_handler = build_rabbit_message_handler("billing", db_session_factory)
        billing_worker = RabbitConsumerWorker("billing", billing_handler, settings, session=rabbit)
        await billing_worker.run(max_messages=1)
        inventory_handler = build_rabbit_message_handler("inventory", db_session_factory)
        inventory_worker = RabbitConsumerWorker(
            "inventory", inventory_handler, settings, session=rabbit
        )
        await inventory_worker.run(max_messages=1)
        async with db_session_factory() as session:
            payment = await session.scalar(
                select(PaymentModel).where(PaymentModel.order_id == order_id)
            )
            assert payment is not None
            assert payment.amount == Decimal("37.50")
            assert payment.status == "completed"
            stock = await session.get(StockItem, sku)
            assert stock is not None
            assert stock.available == 7
            processed_rows = (
                await session.scalars(
                    select(ProcessedEventModel).where(
                        ProcessedEventModel.event_id == confirmed_row.event_id
                    )
                )
            ).all()
            assert {r.consumer_name for r in processed_rows} == {"billing", "inventory"}
        channel = await rabbit.publisher_channel()
        exchange = await channel.get_exchange(settings.rabbitmq_exchange_events)
        duplicate_body = json.dumps(
            {
                "event_id": str(confirmed_row.event_id),
                "event_type": confirmed_row.event_type,
                "aggregate_type": confirmed_row.aggregate_type,
                "aggregate_id": str(confirmed_row.aggregate_id),
                "payload": confirmed_row.payload,
            }
        ).encode()
        await exchange.publish(
            aio_pika.Message(body=duplicate_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=routing_key("order", "order.confirmed"),
        )
        await billing_worker.run(max_messages=1)
        await inventory_worker.run(max_messages=1)
        async with db_session_factory() as session:
            payment_count = await session.scalar(
                select(func.count())
                .select_from(PaymentModel)
                .where(PaymentModel.order_id == order_id)
            )
            stock_after = await session.get(StockItem, sku)
            assert payment_count == 1
            assert stock_after is not None
            assert stock_after.available == 7
        cons_ch = await rabbit.consumer_channel()
        for q_name in (queue_main("billing"), queue_main("inventory")):
            q = await cons_ch.get_queue(q_name)
            assert await q.get(timeout=1, fail=False) is None
    finally:
        await rabbit.close()


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_e2e_api_to_kafka_consumers_billing_and_inventory(
    api_client, db_session_factory, monkeypatch
) -> None:
    monkeypatch.setenv("MESSAGING_TRANSPORT", "kafka")
    settings = _settings_for(MessagingTransport.KAFKA)
    sku = "SKU-E2E-KAFKA"
    async with db_session_factory() as session:
        session.add(StockItem(sku=sku, available=8))
        await session.commit()
    create_resp = await api_client.post(
        "/orders",
        json={
            "customer_id": "cust-e2e-kafka",
            "items": [{"sku": sku, "quantity": 2, "unit_price": "19.00"}],
        },
    )
    assert create_resp.status_code == 201
    order_id = UUID(create_resp.json()["id"])
    confirm_resp = await api_client.post(f"/orders/{order_id}/confirm")
    assert confirm_resp.status_code == 200
    assert confirm_resp.json()["status"] == "confirmed"
    assert confirm_resp.json()["total"] == "38.00"
    kafka = KafkaSession(settings)
    await kafka.start()
    publisher = KafkaPublisher(kafka, settings)
    try:
        async with db_session_factory() as session:
            repo = OutboxRepository(session, settings)
            pending = await repo.list_pending()
            confirmed_row = next(
                r
                for r in pending
                if r.aggregate_id == order_id and r.event_type == EventType.ORDER_CONFIRMED.value
            )
            assert confirmed_row.target_transport == "kafka"
            assert confirmed_row.payload["total"] == "38.00"
            assert confirmed_row.payload["items"] == [
                {"sku": sku, "quantity": 2, "unit_price": "19.00"}
            ]
            loop = OutboxPublishLoop(session, publisher, settings, owner_id="e2e-kafka")
            assert await loop.tick() == 2
            refreshed = await session.get(OutboxEventModel, confirmed_row.id)
            assert refreshed is not None
            assert refreshed.status == "published"
            assert refreshed.broker_meta.get("transport") == "kafka"
        billing_worker = KafkaConsumerWorker(
            "billing",
            build_kafka_message_handler("billing", db_session_factory),
            settings,
            session=kafka,
        )
        inventory_worker = KafkaConsumerWorker(
            "inventory",
            build_kafka_message_handler("inventory", db_session_factory),
            settings,
            session=kafka,
        )
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            await billing_worker.run(max_messages=2)
            billing_worker._stop.clear()
            async with db_session_factory() as session:
                paid = await session.scalar(
                    select(PaymentModel).where(PaymentModel.order_id == order_id)
                )
                if paid is not None:
                    break
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            await inventory_worker.run(max_messages=2)
            inventory_worker._stop.clear()
            async with db_session_factory() as session:
                stock = await session.get(StockItem, sku)
                if stock is not None and stock.available == 6:
                    break
        async with db_session_factory() as session:
            payment = await session.scalar(
                select(PaymentModel).where(PaymentModel.order_id == order_id)
            )
            assert payment is not None
            assert payment.amount == Decimal("38.00")
            assert payment.status == "completed"
            stock = await session.get(StockItem, sku)
            assert stock is not None
            assert stock.available == 6
            processed_rows = (
                await session.scalars(
                    select(ProcessedEventModel).where(
                        ProcessedEventModel.event_id == confirmed_row.event_id
                    )
                )
            ).all()
            assert {r.consumer_name for r in processed_rows} == {"billing", "inventory"}
        for group_name in ("eventflow-billing", "eventflow-inventory"):
            probe = AIOKafkaConsumer(
                settings.kafka_topic_events,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=group_name,
                enable_auto_commit=False,
            )
            await probe.start()
            try:
                partitions = probe.assignment()
                deadline_part = time.monotonic() + 5.0
                while not partitions and time.monotonic() < deadline_part:
                    await probe.getmany(timeout_ms=100)
                    partitions = probe.assignment()
                committed_offsets = [await probe.committed(tp) for tp in partitions]
                assert any(offset is not None and offset > 0 for offset in committed_offsets)
            finally:
                await probe.stop()
    finally:
        await kafka.close()
