"""Delete idempotency rows older than the retention window."""

import argparse
import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete

from eventflow.config import get_settings
from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel
from eventflow.infrastructure.db.session import create_engine


async def prune(days: int) -> int:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    engine = create_engine(get_settings().database_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                delete(ProcessedEventModel).where(ProcessedEventModel.processed_at < cutoff)
            )
            return result.rowcount or 0
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    removed = asyncio.run(prune(args.days))
    print(f"removed {removed} processed_events older than {args.days} days")


if __name__ == "__main__":
    main()
