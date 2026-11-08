from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from eventflow.domain.events.ids import new_event_id
from eventflow.domain.events.types import EventType


class EventEnvelope(BaseModel):
    event_id: UUID
    event_type: EventType
    aggregate_type: str
    aggregate_id: UUID
    occurred_at: datetime
    correlation_id: UUID | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "forbid"}


def build_envelope(
    *,
    event_type: EventType,
    aggregate_type: str,
    aggregate_id: UUID,
    payload: dict[str, Any] | None = None,
    correlation_id: UUID | None = None,
    event_id: UUID | None = None,
    occurred_at: datetime | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id or new_event_id(),
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=occurred_at or datetime.now(UTC),
        correlation_id=correlation_id,
        payload=payload or {},
    )
