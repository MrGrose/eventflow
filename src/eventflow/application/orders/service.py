from decimal import Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.application.orders.ports import OrderRepository
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.domain.orders.exceptions import InvalidOrderTransition
from eventflow.domain.orders.order import Order, OrderItem
from eventflow.infrastructure.db.repositories.order_repository import SqlAlchemyOrderRepository
from eventflow.infrastructure.logging import get_logger
from eventflow.infrastructure.outbox.repository import OutboxRepository

logger = get_logger(__name__)


def _order_payload(order: Order) -> dict[str, object]:
    return {
        "customer_id": order.customer_id,
        "total": str(order.total()),
        "items": [
            {"sku": item.sku, "quantity": item.quantity, "unit_price": str(item.unit_price)}
            for item in order.items
        ],
    }


class OrderService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repo: OrderRepository = SqlAlchemyOrderRepository(session)

    async def create_order(
        self,
        *,
        customer_id: str,
        items: list[dict[str, object]],
        idempotency_key: str | None = None,
    ) -> Order:
        if idempotency_key:
            existing = await self._repo.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing
        order_items = [
            OrderItem(
                sku=str(row["sku"]),
                quantity=int(row["quantity"]),
                unit_price=Decimal(str(row["unit_price"])),
            )
            for row in items
        ]
        order = Order(customer_id=customer_id, items=order_items, idempotency_key=idempotency_key)
        await self._repo.add(order)
        await self._append_outbox(EventType.ORDER_CREATED, order, _order_payload(order))
        await self._session.commit()
        logger.info("order_created", extra={"order_id": str(order.id), "customer_id": customer_id})
        return order

    async def get_order(self, order_id: UUID) -> Order | None:
        return await self._repo.get(order_id)

    async def confirm(self, order_id: UUID) -> Order:
        order = await self._require(order_id)
        order.confirm()
        await self._repo.save(order)
        await self._append_outbox(EventType.ORDER_CONFIRMED, order, _order_payload(order))
        await self._session.commit()
        logger.info("order_confirmed", extra={"order_id": str(order.id)})
        return order

    async def cancel(self, order_id: UUID) -> Order:
        order = await self._require(order_id)
        order.cancel()
        await self._repo.save(order)
        await self._append_outbox(
            EventType.ORDER_CANCELLED, order, {"customer_id": order.customer_id}
        )
        await self._session.commit()
        logger.info("order_cancelled", extra={"order_id": str(order.id)})
        return order

    async def _append_outbox(
        self, event_type: EventType, order: Order, payload: dict[str, object]
    ) -> None:
        outbox = OutboxRepository(self._session)
        await outbox.insert(
            build_envelope(
                event_type=event_type,
                aggregate_type="order",
                aggregate_id=order.id,
                payload=payload,
            )
        )

    async def _require(self, order_id: UUID) -> Order:
        order = await self._repo.get(order_id)
        if order is None:
            raise LookupError("order not found")
        return order


def map_domain_error(exc: Exception) -> tuple[int, str]:
    if isinstance(exc, InvalidOrderTransition):
        return (409, str(exc))
    if isinstance(exc, LookupError):
        return (404, str(exc))
    return (500, "internal error")
