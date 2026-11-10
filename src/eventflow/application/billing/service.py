from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.domain.reliability.errors import PermanentProcessingError
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.payment import PaymentModel


async def record_payment_for_order(
    session: AsyncSession, *, order_id: UUID, amount: Decimal
) -> bool:
    if amount <= Decimal("0"):
        raise PermanentProcessingError(f"invalid payment amount {amount} for order {order_id}")
    order = await session.get(OrderModel, order_id)
    if order is None:
        raise PermanentProcessingError(f"order not found: {order_id}")
    stmt = (
        insert(PaymentModel)
        .values(id=uuid4(), order_id=order_id, amount=amount, status="completed")
        .on_conflict_do_nothing(index_elements=["order_id"])
        .returning(PaymentModel.id)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None
