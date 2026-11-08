from typing import Protocol
from uuid import UUID

from eventflow.domain.orders.order import Order


class OrderRepository(Protocol):
    async def get(self, order_id: UUID) -> Order | None: ...

    async def get_by_idempotency_key(self, key: str) -> Order | None: ...

    async def add(self, order: Order) -> None: ...

    async def save(self, order: Order) -> None: ...
