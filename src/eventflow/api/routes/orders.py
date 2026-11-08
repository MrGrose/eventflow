from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.api.dependencies import get_db_session
from eventflow.api.schemas.orders import CreateOrderRequest, OrderItemOut, OrderOut
from eventflow.application.orders.service import OrderService, map_domain_error
from eventflow.domain.orders.exceptions import DomainError
from eventflow.domain.orders.order import Order

router = APIRouter(prefix="/orders", tags=["orders"])


def _order_out(order: Order) -> OrderOut:
    items = [
        OrderItemOut(sku=i.sku, quantity=i.quantity, unit_price=i.unit_price) for i in order.items
    ]
    return OrderOut(
        id=order.id,
        customer_id=order.customer_id,
        status=order.status.value,
        items=items,
        total=order.total(),
    )


@router.post("", response_model=OrderOut, status_code=status.HTTP_201_CREATED)
async def create_order(
    body: CreateOrderRequest,
    session: AsyncSession = Depends(get_db_session),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> OrderOut:
    service = OrderService(session)
    items = [item.model_dump() for item in body.items]
    order = await service.create_order(
        customer_id=body.customer_id, items=items, idempotency_key=idempotency_key
    )
    return _order_out(order)


@router.get("/{order_id}", response_model=OrderOut)
async def get_order(order_id: UUID, session: AsyncSession = Depends(get_db_session)) -> OrderOut:
    service = OrderService(session)
    order = await service.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="order not found")
    return _order_out(order)


@router.post("/{order_id}/confirm", response_model=OrderOut)
async def confirm_order(
    order_id: UUID, session: AsyncSession = Depends(get_db_session)
) -> OrderOut:
    service = OrderService(session)
    try:
        order = await service.confirm(order_id)
    except DomainError as exc:
        code, detail = map_domain_error(exc)
        raise HTTPException(status_code=code, detail=detail) from exc
    except LookupError as exc:
        code, detail = map_domain_error(exc)
        raise HTTPException(status_code=code, detail=detail) from exc
    return _order_out(order)


@router.post("/{order_id}/cancel", response_model=OrderOut)
async def cancel_order(order_id: UUID, session: AsyncSession = Depends(get_db_session)) -> OrderOut:
    service = OrderService(session)
    try:
        order = await service.cancel(order_id)
    except DomainError as exc:
        code, detail = map_domain_error(exc)
        raise HTTPException(status_code=code, detail=detail) from exc
    except LookupError as exc:
        code, detail = map_domain_error(exc)
        raise HTTPException(status_code=code, detail=detail) from exc
    return _order_out(order)
