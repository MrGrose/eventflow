from decimal import Decimal, InvalidOperation
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.application.billing.service import record_payment_for_order
from eventflow.application.idempotency.consumer import run_idempotent
from eventflow.application.inventory.service import reserve_stock
from eventflow.domain.events.types import EventType
from eventflow.domain.reliability.errors import PermanentProcessingError

ACTIVE_CONSUMERS: tuple[str, ...] = ("billing", "inventory")


def _parse_uuid(value: object, field_name: str) -> UUID:
    try:
        return UUID(str(value))
    except (ValueError, TypeError) as exc:
        raise PermanentProcessingError(f"invalid {field_name}: {value}") from exc


def _extract_inventory_items(
    payload: dict, *, sku: str | None = None, quantity: int | None = None
) -> list[tuple[str, int]]:
    if sku is not None and quantity is not None:
        return [(sku, int(quantity))]
    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        raise PermanentProcessingError("order.confirmed payload missing items")
    parsed: list[tuple[str, int]] = []
    for item in raw_items:
        if not isinstance(item, dict) or "sku" not in item or "quantity" not in item:
            raise PermanentProcessingError(f"invalid order item in payload: {item}")
        try:
            item_sku = str(item["sku"]).strip()
            item_qty = int(item["quantity"])
        except (ValueError, TypeError) as exc:
            raise PermanentProcessingError(f"invalid quantity in item: {item}") from exc
        if not item_sku or item_qty <= 0:
            raise PermanentProcessingError(f"non-positive quantity or empty sku: {item}")
        parsed.append((item_sku, item_qty))
    return parsed


async def handle_order_confirmed_billing(
    session: AsyncSession, *, consumer_name: str, envelope: dict, transport: str | None = None
) -> bool:
    event_id = _parse_uuid(envelope.get("event_id"), "event_id")
    aggregate_id = _parse_uuid(envelope.get("aggregate_id"), "aggregate_id")
    payload = envelope.get("payload") or {}
    if payload.get("poison"):
        raise PermanentProcessingError("poison payload")
    raw_total = payload.get("total")
    if raw_total is None:
        raise PermanentProcessingError("order.confirmed payload missing total")
    try:
        total = Decimal(str(raw_total))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise PermanentProcessingError(f"invalid total in payload: {raw_total}") from exc

    async def work() -> None:
        await record_payment_for_order(session, order_id=aggregate_id, amount=total)

    return await run_idempotent(
        session, consumer_name=consumer_name, event_id=event_id, handler=work, transport=transport
    )


async def handle_order_confirmed_inventory(
    session: AsyncSession,
    *,
    consumer_name: str,
    envelope: dict,
    sku: str | None = None,
    quantity: int | None = None,
    transport: str | None = None,
) -> bool:
    event_id = _parse_uuid(envelope.get("event_id"), "event_id")
    payload = envelope.get("payload") or {}
    if payload.get("poison"):
        raise PermanentProcessingError("poison payload")
    items = _extract_inventory_items(payload, sku=sku, quantity=quantity)

    async def work() -> None:
        for item_sku, item_qty in sorted(items, key=lambda pair: pair[0]):
            await reserve_stock(session, sku=item_sku, quantity=item_qty)

    return await run_idempotent(
        session, consumer_name=consumer_name, event_id=event_id, handler=work, transport=transport
    )


async def dispatch_consumer_event(
    session: AsyncSession, *, consumer_name: str, envelope: dict, transport: str | None = None
) -> bool:
    if consumer_name not in ACTIVE_CONSUMERS:
        raise NotImplementedError(f"consumer '{consumer_name}' is not implemented")
    payload = envelope.get("payload") or {}
    if isinstance(payload, dict) and payload.get("poison"):
        raise PermanentProcessingError("poison payload")
    event_type = envelope.get("event_type")
    if event_type != EventType.ORDER_CONFIRMED.value:
        return False
    if consumer_name == "billing":
        return await handle_order_confirmed_billing(
            session, consumer_name=consumer_name, envelope=envelope, transport=transport
        )
    if consumer_name == "inventory":
        return await handle_order_confirmed_inventory(
            session, consumer_name=consumer_name, envelope=envelope, transport=transport
        )
    return False
