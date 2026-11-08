from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.config import Settings, get_settings
from eventflow.domain.events.envelope import EventEnvelope
from eventflow.domain.outbox.backoff import backoff_seconds
from eventflow.domain.outbox.status import OutboxStatus
from eventflow.infrastructure.db.models.outbox import OutboxEventModel


class OutboxRepository:
    def __init__(self, session: AsyncSession, settings: Settings | None = None) -> None:
        self._session = session
        self._settings = settings or get_settings()

    async def insert(self, envelope: EventEnvelope) -> OutboxEventModel:
        row = OutboxEventModel(
            event_id=envelope.event_id,
            event_type=envelope.event_type.value,
            aggregate_type=envelope.aggregate_type,
            aggregate_id=envelope.aggregate_id,
            payload=envelope.payload,
            target_transport=self._settings.messaging_transport.value,
            status=OutboxStatus.PENDING.value,
        )
        self._session.add(row)
        return row

    async def list_pending(self, limit: int = 100) -> list[OutboxEventModel]:
        stmt = (
            select(OutboxEventModel)
            .where(OutboxEventModel.status == OutboxStatus.PENDING.value)
            .order_by(OutboxEventModel.created_at)
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_event_id(self, event_id: UUID) -> OutboxEventModel | None:
        stmt = select(OutboxEventModel).where(OutboxEventModel.event_id == event_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, row_id: UUID) -> OutboxEventModel | None:
        stmt = select(OutboxEventModel).where(OutboxEventModel.id == row_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def recover_stale_leases(self, *, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        stmt = (
            update(OutboxEventModel)
            .where(
                OutboxEventModel.status == OutboxStatus.PUBLISHING.value,
                OutboxEventModel.lease_expires_at.is_not(None),
                OutboxEventModel.lease_expires_at < now,
            )
            .values(
                status=OutboxStatus.PENDING.value,
                lock_owner=None,
                locked_at=None,
                lease_expires_at=None,
                last_error="lease expired; recovered to pending",
            )
            .returning(OutboxEventModel.id)
        )
        result = await self._session.execute(stmt)
        return len(result.scalars().all())

    async def claim_pending(
        self,
        *,
        owner: str,
        transport: str,
        limit: int,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> list[OutboxEventModel]:
        now = now or datetime.now(UTC)
        lease_until = now + timedelta(seconds=lease_seconds)
        stmt = (
            select(OutboxEventModel)
            .where(
                OutboxEventModel.status == OutboxStatus.PENDING.value,
                OutboxEventModel.target_transport == transport,
                or_(
                    OutboxEventModel.next_attempt_at.is_(None),
                    OutboxEventModel.next_attempt_at <= now,
                ),
            )
            .order_by(OutboxEventModel.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        for row in rows:
            row.status = OutboxStatus.PUBLISHING.value
            row.lock_owner = owner
            row.locked_at = now
            row.lease_expires_at = lease_until
            row.attempts = row.attempts + 1
        await self._session.flush()
        return rows

    async def mark_published(
        self,
        row_id: UUID,
        *,
        owner: str,
        broker_meta: dict | None = None,
        now: datetime | None = None,
    ) -> bool:
        now = now or datetime.now(UTC)
        stmt = (
            update(OutboxEventModel)
            .where(
                OutboxEventModel.id == row_id,
                OutboxEventModel.status == OutboxStatus.PUBLISHING.value,
                OutboxEventModel.lock_owner == owner,
            )
            .values(
                status=OutboxStatus.PUBLISHED.value,
                published_at=now,
                broker_meta=broker_meta or {},
                lock_owner=None,
                locked_at=None,
                lease_expires_at=None,
                last_error=None,
            )
            .returning(OutboxEventModel.id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def mark_publish_failed(
        self, row_id: UUID, *, owner: str, error: str, now: datetime | None = None
    ) -> bool:
        now = now or datetime.now(UTC)
        row = await self.get_by_id(row_id)
        if row is None or row.lock_owner != owner:
            return False
        delay = backoff_seconds(row.attempts)
        stmt = (
            update(OutboxEventModel)
            .where(
                OutboxEventModel.id == row_id,
                OutboxEventModel.lock_owner == owner,
                OutboxEventModel.status == OutboxStatus.PUBLISHING.value,
            )
            .values(
                status=OutboxStatus.PENDING.value,
                next_attempt_at=now + timedelta(seconds=delay),
                last_error=error,
                lock_owner=None,
                locked_at=None,
                lease_expires_at=None,
            )
            .returning(OutboxEventModel.id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None
