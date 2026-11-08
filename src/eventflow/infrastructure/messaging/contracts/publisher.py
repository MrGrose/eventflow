from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class PublishRequest:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload: dict[str, Any]
    target_transport: str


@dataclass(frozen=True, slots=True)
class PublishResult:
    accepted: bool
    broker_meta: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class MessagePublisher(Protocol):
    async def publish(self, message: PublishRequest) -> PublishResult: ...
