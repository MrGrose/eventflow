from decimal import Decimal

import pytest
from sqlalchemy import text

from eventflow.application.billing.service import record_payment_for_order

pytestmark = pytest.mark.asyncio


async def test_second_payment_for_order_is_absorbed(api_client, db_session_factory) -> None:
    created = await api_client.post(
        "/orders",
        json={
            "customer_id": "cust-billing",
            "items": [{"sku": "SKU-3", "quantity": 1, "unit_price": "11.00"}],
        },
    )
    assert created.status_code == 201, created.text
    order_id = created.json()["id"]

    async with db_session_factory() as session:
        first = await record_payment_for_order(
            session, order_id=created.json()["id"], amount=Decimal("11.00")
        )
        await session.commit()
    async with db_session_factory() as session:
        second = await record_payment_for_order(
            session, order_id=created.json()["id"], amount=Decimal("11.00")
        )
        await session.commit()

    assert first is True
    assert second is False
    async with db_session_factory() as session:
        count = await session.scalar(
            text("select count(*) from payments where order_id = :order_id"),
            {"order_id": order_id},
        )
    assert count == 1
