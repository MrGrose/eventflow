import pytest
from sqlalchemy import func, select

from eventflow.application.orders.service import OrderService
from eventflow.infrastructure.db.models.outbox import OutboxEventModel


@pytest.mark.asyncio
async def test_order_and_outbox_in_same_transaction(db_session_factory) -> None:
    async with db_session_factory() as session:
        service = OrderService(session)
        order = await service.create_order(
            customer_id="atomic-c1", items=[{"sku": "A", "quantity": 1, "unit_price": "1.00"}]
        )
        count = await session.scalar(
            select(func.count())
            .select_from(OutboxEventModel)
            .where(OutboxEventModel.aggregate_id == order.id)
        )
        assert count == 1
