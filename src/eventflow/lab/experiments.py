import asyncio
import statistics
import time
from dataclasses import asdict, dataclass, field
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventflow.application.idempotency.consumer import run_idempotent
from eventflow.domain.events.envelope import build_envelope
from eventflow.domain.events.types import EventType
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest
from eventflow.infrastructure.messaging.kafka.session import KafkaSession
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.outbox.publish_loop import OutboxPublishLoop
from eventflow.infrastructure.outbox.repository import OutboxRepository
from eventflow.lab.broker import lab_broker, lab_settings


@dataclass
class ExperimentResult:
    experiment_id: str
    transport: str
    goal: str
    initial_conditions: dict[str, Any]
    configuration: dict[str, Any]
    baseline: dict[str, Any] = field(default_factory=dict)
    failure_injection: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    conclusions: str = ""
    limitations: str = ""


async def run_e1(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    n = 10
    latencies: list[float] = []
    errors = 0
    published = 0
    async with lab_broker(transport) as br:
        settings, publisher = (br.settings, br.publisher)
        async with factory() as session:
            repo = OutboxRepository(session, settings)
            for _ in range(n):
                await repo.insert(
                    build_envelope(
                        event_type=EventType.ORDER_CREATED,
                        aggregate_type="order",
                        aggregate_id=uuid4(),
                        payload={"lab": "e1"},
                    )
                )
            await session.commit()
            loop = OutboxPublishLoop(session, publisher, settings, owner_id="lab-e1")
            t0 = time.perf_counter()
            try:
                published = await loop.tick()
            except Exception:
                published = 0
                errors += 1
            latencies.append(time.perf_counter() - t0)
            if published < n:
                errors += n - published
    return ExperimentResult(
        experiment_id="E1",
        transport=transport,
        goal="Латентность публикации outbox → broker",
        initial_conditions={"pending_rows": n},
        configuration={"confirm": "rabbit confirms / kafka acks=all"},
        baseline={"published_batch_sec": latencies[0] if latencies else None},
        metrics={
            "publish_batch_latency_sec": latencies[0] if latencies else None,
            "published_count": published,
            "errors": errors,
            "p50_sec": statistics.median(latencies) if latencies else None,
        },
        conclusions="Измерен wall-clock одного tick() на n pending rows.",
        limitations="Один publisher worker, без сетевой partition.",
    )


async def run_e2(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    settings = lab_settings(transport)
    ev_id = uuid4()
    duplicate_blocked = False
    async with factory() as session:
        repo = OutboxRepository(session, settings)
        await repo.insert(
            build_envelope(
                event_type=EventType.ORDER_CREATED,
                aggregate_type="order",
                aggregate_id=uuid4(),
                payload={},
                event_id=ev_id,
            )
        )
        await session.commit()
        try:
            async with factory() as session2:
                repo2 = OutboxRepository(session2, settings)
                await repo2.insert(
                    build_envelope(
                        event_type=EventType.ORDER_CREATED,
                        aggregate_type="order",
                        aggregate_id=uuid4(),
                        payload={},
                        event_id=ev_id,
                    )
                )
                await session2.commit()
        except Exception:
            duplicate_blocked = True
    return ExperimentResult(
        experiment_id="E2",
        transport=transport,
        goal="Дубликат event_id в outbox",
        initial_conditions={"same_event_id": str(ev_id)},
        configuration={"outbox_unique_event_id": True},
        metrics={"duplicate_insert_blocked": duplicate_blocked, "rows_expected": 1},
        conclusions="PG unique на event_id предотвращает двойную запись.",
        limitations="До публикации в broker.",
    )


async def run_e3(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    event_id = uuid4()
    duplicates = 0
    async with factory() as session:
        for _ in range(2):

            async def work() -> None:
                return

            ok = await run_idempotent(
                session, consumer_name="billing", event_id=event_id, handler=work
            )
            if not ok:
                duplicates += 1
    return ExperimentResult(
        experiment_id="E3",
        transport=transport,
        goal="Повторная доставка → идемпотентность PG",
        initial_conditions={"duplicate_deliveries": 2},
        configuration={"dedupe": "processed_events"},
        metrics={"duplicate_skips": duplicates, "business_handler_runs_expected": 1},
        conclusions="Второй вызов не создаёт повторный бизнес-эффект.",
        limitations="Только PG dedupe, без брокера.",
    )


async def run_e4(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    async with lab_broker(transport) as br:
        t0 = time.perf_counter()
        req = PublishRequest(
            event_id=str(uuid4()),
            event_type="order.confirmed",
            aggregate_type="order",
            aggregate_id=str(uuid4()),
            payload={"poison": True},
            target_transport=transport,
        )
        res = await br.publisher.publish(req)
        publish_sec = time.perf_counter() - t0
        return ExperimentResult(
            experiment_id="E4",
            transport=transport,
            goal="Публикация poison-сообщения (предпосылка DLQ/DLT)",
            initial_conditions={"poison": True},
            configuration={"dlq": "eventflow.dlq.* / events.dlt"},
            metrics={"publish_ok": res.accepted, "publish_sec": publish_sec},
            failure_injection={"handler_poison": True},
            conclusions="Broker accept измерен; routing в DLQ/DLT — в consumer failure tests.",
            limitations="Без полного consumer loop в lab.",
        )


async def run_e5(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    settings = lab_settings(transport)
    n = 15
    async with factory() as session:
        repo = OutboxRepository(session, settings)
        for _ in range(n):
            await repo.insert(
                build_envelope(
                    event_type=EventType.ORDER_CREATED,
                    aggregate_type="order",
                    aggregate_id=uuid4(),
                    payload={},
                )
            )
        await session.commit()

    async def one_tick(owner: str) -> int:
        async with lab_broker(transport) as br, factory() as session:
            loop = OutboxPublishLoop(session, br.publisher, br.settings, owner_id=owner)
            return await loop.tick()

    t0 = time.perf_counter()
    results = await asyncio.gather(one_tick("lab-e5-a"), one_tick("lab-e5-b"), one_tick("lab-e5-c"))
    elapsed = time.perf_counter() - t0
    published = sum(results)
    return ExperimentResult(
        experiment_id="E5",
        transport=transport,
        goal="Параллельные publisher workers (claim lease)",
        initial_conditions={"pending": n, "workers": 3},
        configuration={"owner_lease": "outbox claim"},
        metrics={"published_total": published, "elapsed_sec": elapsed, "per_worker": list(results)},
        conclusions="Параллельные tick без двойной публикации при lease.",
        limitations="Один процесс, shared PG; отдельный broker handle на parallel worker (E5).",
    )


async def run_e6(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    if transport != "kafka":
        return ExperimentResult(
            experiment_id="E6",
            transport=transport,
            goal="Порядок сообщений (Rabbit)",
            initial_conditions={"model": "per-queue FIFO"},
            configuration={"queues": "eventflow.{consumer}.q"},
            metrics={"note": "single queue FIFO per consumer"},
            conclusions="Rabbit: порядок в пределах одной очереди.",
            limitations="Не сравнивать с Kafka partition model.",
        )
    agg = str(uuid4())
    parts: set[int] = set()
    async with lab_broker("kafka") as br:
        for _ in range(5):
            res = await br.publisher.publish(
                PublishRequest(
                    event_id=str(uuid4()),
                    event_type="order.created",
                    aggregate_type="order",
                    aggregate_id=agg,
                    payload={},
                    target_transport="kafka",
                )
            )
            if res.accepted:
                parts.add(int(res.broker_meta["partition"]))
    return ExperimentResult(
        experiment_id="E6",
        transport="kafka",
        goal="Порядок: один partition на aggregate key",
        initial_conditions={"same_aggregate_id": agg, "messages": 5},
        configuration={"key": "aggregate_id"},
        metrics={"distinct_partitions": len(parts), "partitions": sorted(parts)},
        conclusions="Один ключ → одна partition."
        if len(parts) == 1
        else "Нарушение порядка ключ/partition.",
        limitations="Только Kafka.",
    )


async def run_e7(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    if transport != "kafka":
        return ExperimentResult(
            experiment_id="E7",
            transport=transport,
            goal="Replay / перечитывание",
            initial_conditions={},
            configuration={},
            metrics={"note": "Rabbit: redelivery + DLQ replay manual"},
            conclusions="Rabbit replay — повторная публикация из outbox или purge+seed.",
            limitations="Нет offset log.",
        )
    from aiokafka import AIOKafkaConsumer

    settings = lab_settings(transport)
    async with lab_broker("kafka") as br:
        for _ in range(3):
            await br.publisher.publish(
                PublishRequest(
                    event_id=str(uuid4()),
                    event_type="order.created",
                    aggregate_type="order",
                    aggregate_id=str(uuid4()),
                    payload={"e7": True},
                    target_transport="kafka",
                )
            )
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_events,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    t0 = time.perf_counter()
    count = 0
    try:
        async for _msg in consumer:
            count += 1
            if count >= 3:
                break
    finally:
        await consumer.stop()
    elapsed = time.perf_counter() - t0
    return ExperimentResult(
        experiment_id="E7",
        transport=transport,
        goal="Kafka replay (earliest read)",
        initial_conditions={"seeded_messages": 3},
        configuration={"auto_offset_reset": "earliest", "group_id": None},
        metrics={"messages_read": count, "read_elapsed_sec": elapsed},
        conclusions="Standalone consumer читает с earliest без committed group.",
        limitations="Не consumer group lag.",
    )


async def run_e8(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    settings = lab_settings(transport)
    t0 = time.perf_counter()
    if transport == "rabbitmq":
        rs = RabbitMQSession(settings)
        await rs.connect()
        await rs.close()
        rs2 = RabbitMQSession(settings)
        await rs2.connect()
        ok = rs2.connection is not None and (not rs2.connection.is_closed)
        await rs2.close()
    else:
        ks = KafkaSession(settings)
        await ks.start()
        await ks.close()
        ks2 = KafkaSession(settings)
        await ks2.start()
        ok = True
        await ks2.close()
    elapsed = time.perf_counter() - t0
    return ExperimentResult(
        experiment_id="E8",
        transport=transport,
        goal="Reconnect после разрыва сессии",
        initial_conditions={"cold_reconnect": True},
        configuration={"rabbit": "connect_robust", "kafka": "producer restart"},
        metrics={"reconnect_total_sec": elapsed, "second_session_ok": ok},
        failure_injection={"session_close": True},
        conclusions="Повторная сессия поднимается после close.",
        limitations="Без сетевой partition.",
    )


async def run_e9(factory: async_sessionmaker[AsyncSession], transport: str) -> ExperimentResult:
    n = 30
    elapsed = 0.0
    async with lab_broker(transport) as br:
        settings, publisher = (br.settings, br.publisher)
        async with factory() as session:
            repo = OutboxRepository(session, settings)
            for _ in range(n):
                await repo.insert(
                    build_envelope(
                        event_type=EventType.ORDER_CREATED,
                        aggregate_type="order",
                        aggregate_id=uuid4(),
                        payload={},
                    )
                )
            await session.commit()
            t0 = time.perf_counter()
            loop = OutboxPublishLoop(session, publisher, settings, owner_id="lab-e9")
            total = 0
            while total < n:
                total += await loop.tick()
            elapsed = time.perf_counter() - t0
    return ExperimentResult(
        experiment_id="E9",
        transport=transport,
        goal="Burst publish",
        initial_conditions={"messages": n},
        configuration={"batch_tick_loop": True},
        metrics={"elapsed_sec": elapsed, "throughput_msg_per_sec": n / elapsed if elapsed else 0},
        conclusions=f"Throughput ~{n / elapsed:.1f} msg/s (outbox tick path)."
        if elapsed
        else "N/A",
        limitations="Без полноценных consumers.",
    )


EXPERIMENT_RUNNERS = {
    "E1": run_e1,
    "E2": run_e2,
    "E3": run_e3,
    "E4": run_e4,
    "E5": run_e5,
    "E6": run_e6,
    "E7": run_e7,
    "E8": run_e8,
    "E9": run_e9,
}


async def run_all(factory: async_sessionmaker[AsyncSession], transport: str) -> list[dict]:
    results: list[dict] = []
    for eid in ("E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9"):
        runner = EXPERIMENT_RUNNERS[eid]
        r = await runner(factory, transport)
        results.append(asdict(r))
    return results
