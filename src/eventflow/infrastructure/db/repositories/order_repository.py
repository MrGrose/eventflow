from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from eventflow.domain.orders.order import Order, OrderItem
from eventflow.domain.orders.status import OrderStatus
from eventflow.infrastructure.db.models.order import OrderItemModel, OrderModel


def _to_domain(model: OrderModel) -> Order:
    return Order(
        id=model.id,
        customer_id=model.customer_id,
        status=OrderStatus(model.status),
        idempotency_key=model.idempotency_key,
        created_at=model.created_at,
        updated_at=model.updated_at,
        items=[
            OrderItem(sku=item.sku, quantity=item.quantity, unit_price=Decimal(item.unit_price))
            for item in model.items
        ],
    )


class SqlAlchemyOrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, order_id: UUID) -> Order | None:
        stmt = (
            select(OrderModel)
            .where(OrderModel.id == order_id)
            .options(selectinload(OrderModel.items))
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return _to_domain(model)

    async def get_by_idempotency_key(self, key: str) -> Order | None:
        stmt = (
            select(OrderModel)
            .where(OrderModel.idempotency_key == key)
            .options(selectinload(OrderModel.items))
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return _to_domain(model)

    async def add(self, order: Order) -> None:
        model = OrderModel(
            id=order.id,
            customer_id=order.customer_id,
            status=order.status.value,
            idempotency_key=order.idempotency_key,
            created_at=order.created_at,
            updated_at=order.updated_at,
            items=[
                OrderItemModel(sku=item.sku, quantity=item.quantity, unit_price=item.unit_price)
                for item in order.items
            ],
        )
        self._session.add(model)

    async def save(self, order: Order) -> None:
        stmt = (
            select(OrderModel)
            .where(OrderModel.id == order.id)
            .options(selectinload(OrderModel.items))
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one()
        model.status = order.status.value
        model.updated_at = order.updated_at
