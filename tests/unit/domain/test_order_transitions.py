from decimal import Decimal

import pytest

from eventflow.domain.orders.exceptions import InvalidOrderTransition
from eventflow.domain.orders.order import Order, OrderItem
from eventflow.domain.orders.status import OrderStatus


def _order() -> Order:
    return Order(
        customer_id="cust-1",
        items=[OrderItem(sku="SKU-1", quantity=2, unit_price=Decimal("10.00"))],
    )


def test_confirm_from_draft() -> None:
    order = _order()
    order.confirm()
    assert order.status == OrderStatus.CONFIRMED


def test_cancel_from_draft() -> None:
    order = _order()
    order.cancel()
    assert order.status == OrderStatus.CANCELLED


def test_cannot_confirm_twice() -> None:
    order = _order()
    order.confirm()
    with pytest.raises(InvalidOrderTransition):
        order.confirm()


def test_cannot_cancel_paid() -> None:
    order = _order()
    order.status = OrderStatus.PAID
    with pytest.raises(InvalidOrderTransition):
        order.cancel()
