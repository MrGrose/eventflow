from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.domain.reliability.errors import PermanentProcessingError
from eventflow.infrastructure.db.models.inventory import StockItem


async def reserve_stock(session: AsyncSession, *, sku: str, quantity: int) -> bool:
    if quantity <= 0:
        raise PermanentProcessingError(f"invalid quantity {quantity} for {sku}")
    stmt = select(StockItem).where(StockItem.sku == sku).with_for_update()
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    if row is None or row.available < quantity:
        raise PermanentProcessingError(f"insufficient stock for {sku}")
    row.available -= quantity
    await session.flush()
    return True
