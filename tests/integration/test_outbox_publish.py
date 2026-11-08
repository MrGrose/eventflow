import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from eventflow.application.consumers.dispatch import handle_order_confirmed_billing
from eventflow.config import get_settings
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.domain.outbox.status import OutboxStatus
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.outbox import OutboxEventModel
from eventflow.infrastructure.db.models.payment import PaymentModel
from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel
from eventflow.infrastructure.messaging.contracts.noop import NoopPublisher
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest, PublishResult
from eventflow.infrastructure.outbox.publish_loop import OutboxPublishLoop
from eventflow.infrastructure.outbox.repository import OutboxRepository


class _CrashAfterBrokerAckPublisher:
    def __init__(self) -> None:
        self.broker_deliveries: list[PublishRequest] = []

    async def publish(self, message: PublishRequest) -> PublishResult:
        self.broker_deliveries.append(message)
        raise RuntimeError("simulated worker crash after broker ack before mark_published")


async def _insert_pending(
    session,
    *,
    transport: str | None = None,
    event_type: EventType = EventType.ORDER_CREATED,
    aggregate_id=None,
    payload: dict | None = None,
) -> OutboxEventModel:
    repo = OutboxRepository(session)
    settings = get_settings()
    transport = transport or settings.messaging_transport.value
    env = build_envelope(
        event_type=event_type,
        aggregate_type="order",
        aggregate_id=aggregate_id or uuid4(),
        payload=payload or {"k": "v"},
    )
    row = await repo.insert(env)
    row.target_transport = transport
    await session.commit()
    return row


@pytest.mark.asyncio
async def test_scenario_a_successful_publish(db_session_factory) -> None:
    async with db_session_factory() as session:
        row = await _insert_pending(session)
        publisher = NoopPublisher()
        loop = OutboxPublishLoop(session, publisher, get_settings(), owner_id="worker-a")
        assert await loop.tick() == 1
        refreshed = await session.get(OutboxEventModel, row.id)
        assert refreshed is not None
        assert refreshed.status == OutboxStatus.PUBLISHED.value
        assert refreshed.broker_meta.get("transport") == "noop"
        assert len(publisher.published) == 1


@pytest.mark.asyncio
async def test_scenario_b_crash_after_broker_then_recover_and_idempotent_consume(
    db_session_factory,
) -> None:
    order_id = uuid4()
    async with db_session_factory() as session:
        session.add(OrderModel(id=order_id, customer_id="cust-b", status="confirmed"))
        await session.commit()
        row = await _insert_pending(
            session,
            event_type=EventType.ORDER_CONFIRMED,
            aggregate_id=order_id,
            payload={"customer_id": "cust-b", "total": "42.00"},
        )
    crashing_publisher = _CrashAfterBrokerAckPublisher()
    settings = get_settings()
    async with db_session_factory() as session:
        loop = OutboxPublishLoop(
            session, crashing_publisher, settings, owner_id="worker-b", lease_seconds=1
        )
        with pytest.raises(RuntimeError, match="simulated worker crash"):
            await loop.tick()
    assert len(crashing_publisher.broker_deliveries) == 1
    async with db_session_factory() as session:
        stale = await session.get(OutboxEventModel, row.id)
        assert stale is not None
        assert stale.status == OutboxStatus.PUBLISHING.value
        stale.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
    second_publisher = NoopPublisher()
    async with db_session_factory() as session:
        loop = OutboxPublishLoop(session, second_publisher, settings, owner_id="worker-b2")
        assert await loop.tick() == 1
        again = await session.get(OutboxEventModel, row.id)
        assert again is not None
        assert again.status == OutboxStatus.PUBLISHED.value
    assert len(second_publisher.published) == 1
    delivered_twice = [crashing_publisher.broker_deliveries[0], second_publisher.published[0]]
    outcomes: list[bool] = []
    for msg in delivered_twice:
        envelope = {
            "event_id": msg.event_id,
            "event_type": msg.event_type,
            "aggregate_type": msg.aggregate_type,
            "aggregate_id": msg.aggregate_id,
            "payload": msg.payload,
        }
        async with db_session_factory() as session:
            ok = await handle_order_confirmed_billing(
                session, consumer_name="billing", envelope=envelope
            )
            outcomes.append(ok)
    assert outcomes == [True, False]
    async with db_session_factory() as session:
        payments_count = await session.scalar(
            select(func.count()).select_from(PaymentModel).where(PaymentModel.order_id == order_id)
        )
        payment = await session.scalar(
            select(PaymentModel).where(PaymentModel.order_id == order_id)
        )
        processed_count = await session.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == row.event_id)
        )
        assert payments_count == 1
        assert payment is not None
        assert payment.amount == Decimal("42.00")
        assert processed_count == 1


@pytest.mark.asyncio
async def test_scenario_c_lease_expired_recovery(db_session_factory) -> None:
    async with db_session_factory() as session:
        row = await _insert_pending(session)
        row.status = OutboxStatus.PUBLISHING.value
        row.lock_owner = "ghost"
        row.lease_expires_at = datetime.now(UTC) - timedelta(seconds=5)
        await session.commit()
    async with db_session_factory() as session:
        repo = OutboxRepository(session)
        assert await repo.recover_stale_leases() == 1
        await session.commit()
        recovered = await session.get(OutboxEventModel, row.id)
        assert recovered is not None
        assert recovered.status == OutboxStatus.PENDING.value
        assert recovered.lock_owner is None


@pytest.mark.asyncio
async def test_scenario_d_double_claim_no_overlap(db_session_factory) -> None:
    async with db_session_factory() as session:
        for _ in range(4):
            await _insert_pending(session)

    async def claim(owner: str) -> list:
        async with db_session_factory() as session:
            repo = OutboxRepository(session)
            rows = await repo.claim_pending(
                owner=owner,
                transport=get_settings().messaging_transport.value,
                limit=3,
                lease_seconds=30,
            )
            await session.commit()
            return [r.id for r in rows]

    first, second = await asyncio.gather(claim("w1"), claim("w2"))
    assert not set(first) & set(second)
    assert len(first) + len(second) == 4


@pytest.mark.asyncio
async def test_scenario_e_transport_guard(db_session_factory, monkeypatch) -> None:
    monkeypatch.setenv("MESSAGING_TRANSPORT", "rabbitmq")
    async with db_session_factory() as session:
        row = await _insert_pending(session, transport="kafka")
        loop = OutboxPublishLoop(session, NoopPublisher(), get_settings(), owner_id="w3")
        assert await loop.tick() == 0
        still = await session.get(OutboxEventModel, row.id)
        assert still is not None
        assert still.status == OutboxStatus.PENDING.value


@pytest.mark.asyncio
async def test_broker_down_stays_pending(db_session_factory) -> None:
    async with db_session_factory() as session:
        row = await _insert_pending(session)
        loop = OutboxPublishLoop(session, NoopPublisher(fail=True), get_settings(), owner_id="w4")
        assert await loop.tick() == 0
        refreshed = await session.get(OutboxEventModel, row.id)
        assert refreshed is not None
        assert refreshed.status == OutboxStatus.PENDING.value
        assert refreshed.next_attempt_at is not None


@pytest.mark.asyncio
async def test_stale_owner_cannot_mark_published(db_session_factory) -> None:
    async with db_session_factory() as session:
        row = await _insert_pending(session)
        repo = OutboxRepository(session)
        claimed = await repo.claim_pending(
            owner="right-owner",
            transport=get_settings().messaging_transport.value,
            limit=1,
            lease_seconds=30,
        )
        assert claimed
        ok = await repo.mark_published(row.id, owner="wrong-owner", broker_meta={})
        assert ok is False
        await repo.mark_published(row.id, owner="right-owner", broker_meta={"ok": True})
        await session.commit()
        final = await session.get(OutboxEventModel, row.id)
        assert final is not None
        assert final.status == OutboxStatus.PUBLISHED.value
