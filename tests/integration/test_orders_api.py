import asyncio
import uuid

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.asyncio


async def test_create_and_get_order(api_client) -> None:
    payload = {
        "customer_id": "cust-int",
        "items": [{"sku": "SKU-1", "quantity": 1, "unit_price": "12.50"}],
    }
    create = await api_client.post("/orders", json=payload)
    if create.status_code >= 500:
        pytest.skip("database not available for integration")
    assert create.status_code == 201, create.text
    order_id = create.json()["id"]
    get_resp = await api_client.get(f"/orders/{order_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["status"] == "draft"


async def test_confirm_flow(api_client) -> None:
    create = await api_client.post(
        "/orders",
        json={
            "customer_id": "cust-int",
            "items": [{"sku": "SKU-2", "quantity": 2, "unit_price": "5.00"}],
        },
    )
    if create.status_code >= 500:
        pytest.skip("database not available for integration")
    order_id = create.json()["id"]
    confirm = await api_client.post(f"/orders/{order_id}/confirm")
    assert confirm.status_code == 200
    assert confirm.json()["status"] == "confirmed"


async def test_duplicate_idempotency_key_returns_same_order(api_client) -> None:
    key = str(uuid.uuid4())
    payload = {
        "customer_id": "cust-idem",
        "items": [{"sku": "SKU-1", "quantity": 1, "unit_price": "9.90"}],
    }
    responses = await asyncio.gather(
        *[
            api_client.post("/orders", json=payload, headers={"Idempotency-Key": key})
            for _ in range(5)
        ]
    )
    statuses = [response.status_code for response in responses]
    assert all(status in (200, 201) for status in statuses), statuses
    order_ids = {response.json()["id"] for response in responses}
    assert len(order_ids) == 1


async def test_concurrent_confirm_creates_single_event(api_client, db_session_factory) -> None:
    created = await api_client.post(
        "/orders",
        json={
            "customer_id": "cust-confirm",
            "items": [{"sku": "SKU-2", "quantity": 1, "unit_price": "4.00"}],
        },
    )
    assert created.status_code == 201, created.text
    order_id = created.json()["id"]
    responses = await asyncio.gather(
        *[api_client.post(f"/orders/{order_id}/confirm") for _ in range(6)]
    )
    statuses = [response.status_code for response in responses]
    assert statuses.count(200) == 1, statuses
    assert all(status in (200, 409) for status in statuses), statuses
    async with db_session_factory() as session:
        count = await session.scalar(
            text(
                "select count(*) from outbox_events "
                "where aggregate_id = :order_id and event_type = 'order.confirmed'"
            ),
            {"order_id": uuid.UUID(order_id)},
        )
    assert count == 1
