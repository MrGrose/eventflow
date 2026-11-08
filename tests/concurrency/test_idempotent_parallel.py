import asyncio
from uuid import uuid4

import pytest

from eventflow.application.consumers.dispatch import handle_order_confirmed_inventory
from eventflow.application.idempotency.consumer import run_idempotent
from eventflow.config import get_settings
from eventflow.domain.reliability.errors import PermanentProcessingError
from eventflow.infrastructure.db.models.inventory import StockItem
from eventflow.infrastructure.db.session import create_session_factory


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_two_consumers_same_event_id(db_session_factory) -> None:
    event_id = uuid4()
    settings = get_settings()
    factory = create_session_factory(settings)
    results: list[bool] = []

    async def attempt() -> None:
        async with factory() as session:
            calls = {"n": 0}

            async def work() -> None:
                calls["n"] += 1

            ok = await run_idempotent(
                session, consumer_name="billing", event_id=event_id, handler=work
            )
            results.append(ok)

    await asyncio.gather(attempt(), attempt())
    assert results.count(True) == 1
    assert results.count(False) == 1


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_concurrent_inventory_reservation_last_unit_never_negative(
    db_session_factory,
) -> None:
    sku = f"SKU-CONC-{uuid4().hex[:8]}"
    async with db_session_factory() as session:
        session.add(StockItem(sku=sku, available=1))
        await session.commit()
    outcomes: list[str] = []

    async def reserve_once() -> None:
        envelope = {
            "event_id": str(uuid4()),
            "aggregate_id": str(uuid4()),
            "event_type": "order.confirmed",
            "payload": {"items": [{"sku": sku, "quantity": 1}]},
        }
        async with db_session_factory() as session:
            try:
                await handle_order_confirmed_inventory(
                    session, consumer_name="inventory", envelope=envelope
                )
                outcomes.append("reserved")
            except PermanentProcessingError:
                outcomes.append("insufficient")

    await asyncio.gather(reserve_once(), reserve_once())
    assert sorted(outcomes) == ["insufficient", "reserved"]
    async with db_session_factory() as session:
        stock = await session.get(StockItem, sku)
        assert stock is not None
        assert stock.available == 0
