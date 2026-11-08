import time
from collections.abc import Awaitable, Callable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.config import get_settings
from eventflow.infrastructure.idempotency.repository import ProcessedEventsRepository
from eventflow.infrastructure.logging import get_logger
from eventflow.infrastructure.observability.metrics import (
    EVENTS_PROCESSED,
    PROCESSING_DURATION,
    RETRY_TOTAL,
)

Handler = Callable[[], Awaitable[None]]
logger = get_logger(__name__)


def _has_external_transaction(session: AsyncSession) -> bool:
    return session.in_transaction() or bool(session.new or session.dirty or session.deleted)


async def run_idempotent(
    session: AsyncSession,
    *,
    consumer_name: str,
    event_id: UUID,
    handler: Handler,
    transport: str | None = None,
) -> bool:
    settings = get_settings()
    transport = transport or settings.messaging_transport.value
    external_tx = _has_external_transaction(session)
    repo = ProcessedEventsRepository(session)
    start = time.perf_counter()
    if external_tx:
        if session.new or session.dirty or session.deleted:
            await session.flush()
        try:
            async with session.begin_nested():
                is_new = await repo.register_or_duplicate(
                    consumer_name=consumer_name, event_id=event_id
                )
                if not is_new:
                    EVENTS_PROCESSED.labels(
                        transport=transport, consumer=consumer_name, outcome="duplicate"
                    ).inc()
                    logger.info(
                        "consumer_duplicate",
                        extra={
                            "event_id": str(event_id),
                            "consumer": consumer_name,
                            "transport": transport,
                        },
                    )
                    return False
                await handler()
            duration = time.perf_counter() - start
            PROCESSING_DURATION.labels(transport=transport, consumer=consumer_name).observe(
                duration
            )
            EVENTS_PROCESSED.labels(transport=transport, consumer=consumer_name, outcome="ok").inc()
            logger.info(
                "consumer_ok",
                extra={
                    "event_id": str(event_id),
                    "consumer": consumer_name,
                    "transport": transport,
                    "duration_sec": round(duration, 4),
                },
            )
            return True
        except Exception:
            duration = time.perf_counter() - start
            PROCESSING_DURATION.labels(transport=transport, consumer=consumer_name).observe(
                duration
            )
            EVENTS_PROCESSED.labels(
                transport=transport, consumer=consumer_name, outcome="error"
            ).inc()
            RETRY_TOTAL.labels(transport=transport, stage="consumer").inc()
            logger.exception(
                "consumer_error",
                extra={
                    "event_id": str(event_id),
                    "consumer": consumer_name,
                    "transport": transport,
                    "duration_sec": round(duration, 4),
                },
            )
            raise
    is_new = await repo.register_or_duplicate(consumer_name=consumer_name, event_id=event_id)
    if not is_new:
        await session.rollback()
        EVENTS_PROCESSED.labels(
            transport=transport, consumer=consumer_name, outcome="duplicate"
        ).inc()
        logger.info(
            "consumer_duplicate",
            extra={"event_id": str(event_id), "consumer": consumer_name, "transport": transport},
        )
        return False
    try:
        await handler()
        await session.commit()
        duration = time.perf_counter() - start
        PROCESSING_DURATION.labels(transport=transport, consumer=consumer_name).observe(duration)
        EVENTS_PROCESSED.labels(transport=transport, consumer=consumer_name, outcome="ok").inc()
        logger.info(
            "consumer_ok",
            extra={
                "event_id": str(event_id),
                "consumer": consumer_name,
                "transport": transport,
                "duration_sec": round(duration, 4),
            },
        )
        return True
    except Exception:
        await session.rollback()
        duration = time.perf_counter() - start
        PROCESSING_DURATION.labels(transport=transport, consumer=consumer_name).observe(duration)
        EVENTS_PROCESSED.labels(transport=transport, consumer=consumer_name, outcome="error").inc()
        RETRY_TOTAL.labels(transport=transport, stage="consumer").inc()
        logger.exception(
            "consumer_error",
            extra={
                "event_id": str(event_id),
                "consumer": consumer_name,
                "transport": transport,
                "duration_sec": round(duration, 4),
            },
        )
        raise
