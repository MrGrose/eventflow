from uuid import UUID

from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType


def test_build_envelope_fields() -> None:
    envelope = build_envelope(
        event_type=EventType.ORDER_CREATED,
        aggregate_type="order",
        aggregate_id=UUID("00000000-0000-0000-0000-000000000101"),
        payload={"customer_id": "c-1"},
    )
    assert envelope.event_type == EventType.ORDER_CREATED
    assert envelope.aggregate_type == "order"
    assert envelope.payload["customer_id"] == "c-1"
