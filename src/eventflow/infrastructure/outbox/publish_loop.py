from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.config import Settings
from eventflow.infrastructure.db.models.outbox import OutboxEventModel
from eventflow.infrastructure.messaging.contracts.publisher import MessagePublisher, PublishRequest
from eventflow.infrastructure.observability.metrics import EVENTS_FAILED, EVENTS_PUBLISHED
from eventflow.infrastructure.outbox.repository import OutboxRepository


class OutboxPublishLoop:
    def __init__(
        self,
        session: AsyncSession,
        publisher: MessagePublisher,
        settings: Settings,
        *,
        owner_id: str,
        lease_seconds: int = 30,
        batch_size: int = 20,
    ) -> None:
        self._session = session
        self._publisher = publisher
        self._settings = settings
        self._owner = owner_id
        self._lease_seconds = lease_seconds
        self._batch_size = batch_size
        self._repo = OutboxRepository(session, settings)

    async def tick(self) -> int:
        await self._repo.recover_stale_leases()
        claimed = await self._repo.claim_pending(
            owner=self._owner,
            transport=self._settings.messaging_transport.value,
            limit=self._batch_size,
            lease_seconds=self._lease_seconds,
        )
        await self._session.commit()
        published = 0
        transport = self._settings.messaging_transport.value
        for row in claimed:
            result = await self._publisher.publish(_to_request(row))
            if result.accepted:
                updated = await self._repo.mark_published(
                    row.id, owner=self._owner, broker_meta=result.broker_meta
                )
                if updated:
                    EVENTS_PUBLISHED.labels(transport=transport, outcome="ok").inc()
                    published += 1
            else:
                EVENTS_FAILED.labels(transport=transport, stage="publish").inc()
                await self._repo.mark_publish_failed(
                    row.id, owner=self._owner, error=result.error or "publish rejected"
                )
            await self._session.commit()
        return published


def _to_request(row: OutboxEventModel) -> PublishRequest:
    return PublishRequest(
        event_id=str(row.event_id),
        event_type=row.event_type,
        aggregate_type=row.aggregate_type,
        aggregate_id=str(row.aggregate_id),
        payload=row.payload,
        target_transport=row.target_transport,
    )
