from uuid import uuid4

import pytest
from sqlalchemy import func, select

from eventflow.application.consumers.dispatch import (
    handle_order_confirmed_billing,
    handle_order_confirmed_inventory,
)
from eventflow.application.idempotency.consumer import run_idempotent
from eventflow.application.inventory.service import reserve_stock
from eventflow.domain.reliability.errors import PermanentProcessingError, RetryableProcessingError
from eventflow.infrastructure.db.models.inventory import StockItem
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.payment import PaymentModel
from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel


@pytest.mark.failure
@pytest.mark.asyncio
async def test_duplicate_event_no_double_payment(db_session_factory) -> None:
    order_id = uuid4()
    event_id = uuid4()
    async with db_session_factory() as session:
        session.add(OrderModel(id=order_id, customer_id="c1", status="confirmed"))
        await session.commit()
    envelope = {
        "event_id": str(event_id),
        "aggregate_id": str(order_id),
        "event_type": "order.confirmed",
        "payload": {"total": "10.00"},
    }
    for _ in range(2):
        async with db_session_factory() as session:
            await handle_order_confirmed_billing(
                session, consumer_name="billing", envelope=envelope
            )
    async with db_session_factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(PaymentModel).where(PaymentModel.order_id == order_id)
        )
        assert count == 1


@pytest.mark.failure
@pytest.mark.asyncio
async def test_inventory_no_negative_stock_and_missing_sku_fails(db_session_factory) -> None:
    sku = f"SKU-{uuid4().hex[:8]}"
    async with db_session_factory() as session:
        session.add(StockItem(sku=sku, available=1))
        await session.commit()
    event_id = uuid4()
    async with db_session_factory() as session:

        async def work() -> None:
            await reserve_stock(session, sku=sku, quantity=1)

        await run_idempotent(session, consumer_name="inventory", event_id=event_id, handler=work)
    async with db_session_factory() as session:
        row = await session.get(StockItem, sku)
        assert row is not None
        assert row.available == 0
    event_id2 = uuid4()
    async with db_session_factory() as session:

        async def work2() -> None:
            await reserve_stock(session, sku=sku, quantity=1)

        with pytest.raises(PermanentProcessingError):
            await run_idempotent(
                session, consumer_name="inventory", event_id=event_id2, handler=work2
            )
    missing_sku = f"SKU-MISSING-{uuid4().hex[:8]}"
    async with db_session_factory() as session:
        with pytest.raises(PermanentProcessingError):
            await reserve_stock(session, sku=missing_sku, quantity=1)
        ghost = await session.get(StockItem, missing_sku)
        assert ghost is None


@pytest.mark.failure
@pytest.mark.asyncio
async def test_rollback_on_partial_multi_item_inventory_and_retryable_recovery(
    db_session_factory,
) -> None:
    sku_ok = f"SKU-OK-{uuid4().hex[:8]}"
    sku_short = f"SKU-SHORT-{uuid4().hex[:8]}"
    async with db_session_factory() as session:
        session.add_all([StockItem(sku=sku_ok, available=5), StockItem(sku=sku_short, available=0)])
        await session.commit()
    fail_event_id = uuid4()
    envelope = {
        "event_id": str(fail_event_id),
        "aggregate_id": str(uuid4()),
        "event_type": "order.confirmed",
        "payload": {"items": [{"sku": sku_ok, "quantity": 2}, {"sku": sku_short, "quantity": 1}]},
    }
    async with db_session_factory() as session:
        with pytest.raises(PermanentProcessingError):
            await handle_order_confirmed_inventory(
                session, consumer_name="inventory", envelope=envelope
            )
        ok_row = await session.get(StockItem, sku_ok)
        assert ok_row is not None
        assert ok_row.available == 5
        processed = await session.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == fail_event_id)
        )
        assert processed == 0
    retry_event_id = uuid4()
    attempts = 0
    async with db_session_factory() as session:

        async def flaky_work() -> None:
            nonlocal attempts
            attempts += 1
            await reserve_stock(session, sku=sku_ok, quantity=2)
            if attempts == 1:
                raise RetryableProcessingError("temporary db/network error")

        with pytest.raises(RetryableProcessingError):
            await run_idempotent(
                session, consumer_name="inventory", event_id=retry_event_id, handler=flaky_work
            )
        stock_after_fail = await session.get(StockItem, sku_ok)
        assert stock_after_fail is not None
        assert stock_after_fail.available == 5
        ok = await run_idempotent(
            session, consumer_name="inventory", event_id=retry_event_id, handler=flaky_work
        )
        assert ok is True
        stock_after_retry = await session.get(StockItem, sku_ok)
        assert stock_after_retry is not None
        assert stock_after_retry.available == 3
