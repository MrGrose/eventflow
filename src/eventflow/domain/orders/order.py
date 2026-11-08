from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from eventflow.domain.orders.exceptions import InvalidOrderTransition
from eventflow.domain.orders.status import OrderStatus


@dataclass
class OrderItem:
    sku: str
    quantity: int
    unit_price: Decimal

    def line_total(self) -> Decimal:
        return self.unit_price * self.quantity


@dataclass
class Order:
    customer_id: str
    items: list[OrderItem]
    id: UUID = field(default_factory=uuid4)
    status: OrderStatus = OrderStatus.DRAFT
    idempotency_key: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def total(self) -> Decimal:
        return sum((item.line_total() for item in self.items), Decimal("0"))

    def confirm(self) -> None:
        if self.status != OrderStatus.DRAFT:
            raise InvalidOrderTransition(f"cannot confirm from {self.status}")
        self.status = OrderStatus.CONFIRMED
        self._touch()

    def cancel(self) -> None:
        if self.status not in (OrderStatus.DRAFT, OrderStatus.CONFIRMED):
            raise InvalidOrderTransition(f"cannot cancel from {self.status}")
        self.status = OrderStatus.CANCELLED
        self._touch()

    def _touch(self) -> None:
        self.updated_at = datetime.now(UTC)
