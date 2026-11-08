from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel


class ProcessedEventsRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def register_or_duplicate(self, *, consumer_name: str, event_id: UUID) -> bool:
        stmt = (
            insert(ProcessedEventModel)
            .values(consumer_name=consumer_name, event_id=event_id)
            .on_conflict_do_nothing(index_elements=["consumer_name", "event_id"])
            .returning(ProcessedEventModel.event_id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
