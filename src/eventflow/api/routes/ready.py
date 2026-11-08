import asyncio

import aio_pika
from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from eventflow.api.dependencies import get_db_session
from eventflow.config import MessagingTransport, get_settings

router = APIRouter(tags=["observability"])


@router.get("/ready")
async def ready(session: AsyncSession = Depends(get_db_session)) -> dict[str, object]:
    settings = get_settings()
    checks: dict[str, str] = {}
    try:
        await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
    transport = settings.messaging_transport
    if transport == MessagingTransport.RABBITMQ:
        try:
            conn = await asyncio.wait_for(
                aio_pika.connect_robust(settings.rabbitmq_url), timeout=3.0
            )
            await conn.close()
            checks["broker"] = "ok"
        except Exception as exc:
            checks["broker"] = f"error: {exc}"
    else:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        try:
            await asyncio.wait_for(producer.start(), timeout=5.0)
            await producer.stop()
            checks["broker"] = "ok"
        except Exception as exc:
            checks["broker"] = f"error: {exc}"
    status = "ready" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": status, "checks": checks, "transport": transport.value}
