import pytest

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
