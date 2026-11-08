from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.config import MessagingTransport, Settings
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.domain.outbox.status import OutboxStatus
from eventflow.infrastructure.outbox.repository import OutboxRepository


class _RecordingSession:
    def __init__(self) -> None:
        self.added: list[object] = []

    def add(self, instance: object) -> None:
        self.added.append(instance)


@pytest.mark.asyncio
async def test_outbox_insert_populates_pending_row_and_target_transport(monkeypatch) -> None:
    monkeypatch.setenv("MESSAGING_TRANSPORT", "kafka")
    rec = _RecordingSession()
    repo = OutboxRepository(rec)
    agg_id = UUID("00000000-0000-0000-0000-000000000201")
    envelope = build_envelope(
        event_type=EventType.ORDER_CONFIRMED,
        aggregate_type="order",
        aggregate_id=agg_id,
        payload={"customer_id": "c-1", "total": "15.00"},
    )
    row = await repo.insert(envelope)
    assert len(rec.added) == 1
    assert rec.added[0] is row
    assert row.event_id == envelope.event_id
    assert row.event_type == "order.confirmed"
    assert row.aggregate_type == "order"
    assert row.aggregate_id == agg_id
    assert row.payload == {"customer_id": "c-1", "total": "15.00"}
    assert row.target_transport == MessagingTransport.KAFKA.value
    assert row.status == OutboxStatus.PENDING.value


@pytest.mark.asyncio
async def test_outbox_insert_respects_explicit_settings() -> None:
    rec = _RecordingSession()
    settings = Settings(messaging_transport=MessagingTransport.RABBITMQ)
    session: AsyncSession = rec
    repo = OutboxRepository(session, settings)
    envelope = build_envelope(
        event_type=EventType.ORDER_CREATED,
        aggregate_type="order",
        aggregate_id=UUID("00000000-0000-0000-0000-000000000202"),
        payload={"x": 1},
    )
    row = await repo.insert(envelope)
    assert row.target_transport == "rabbitmq"
    assert row.status == OutboxStatus.PENDING.value
