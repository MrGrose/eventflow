from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.domain.reliability.errors import PermanentProcessingError
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.payment import PaymentModel


async def record_payment_for_order(
    session: AsyncSession, *, order_id: UUID, amount: Decimal
) -> bool:
    if amount <= Decimal("0"):
        raise PermanentProcessingError(f"invalid payment amount {amount} for order {order_id}")
    existing = await session.scalar(
        select(PaymentModel).where(
            PaymentModel.order_id == order_id, PaymentModel.status == "completed"
        )
    )
    if existing is not None:
        return False
    order = await session.get(OrderModel, order_id)
    if order is None:
        raise PermanentProcessingError(f"order not found: {order_id}")
    payment = PaymentModel(order_id=order_id, amount=amount, status="completed")
    session.add(payment)
    await session.flush()
    return True
