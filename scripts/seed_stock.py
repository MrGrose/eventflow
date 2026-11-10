"""Seed stock rows used by the inventory consumer in local runs and demos."""

import asyncio

from sqlalchemy.dialects.postgresql import insert

from eventflow.config import get_settings
from eventflow.infrastructure.db.models.inventory import StockItem
from eventflow.infrastructure.db.session import create_engine

DEFAULT_STOCK = {"SKU-1": 100, "SKU-2": 100, "SKU-3": 100}


async def seed() -> None:
    engine = create_engine(get_settings().database_url)
    try:
        async with engine.begin() as conn:
            for sku, available in DEFAULT_STOCK.items():
                stmt = (
                    insert(StockItem)
                    .values(sku=sku, available=available)
                    .on_conflict_do_update(index_elements=["sku"], set_={"available": available})
                )
                await conn.execute(stmt)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(seed())
