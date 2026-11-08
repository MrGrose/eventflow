import aio_pika
from aio_pika.abc import AbstractChannel, AbstractExchange

from eventflow.config import Settings, get_settings

CONSUMER_NAMES = ("billing", "inventory", "notifications", "analytics")
BINDINGS: dict[str, list[str]] = {
    "billing": ["order.confirmed", "payment.completed", "payment.failed"],
    "inventory": ["order.confirmed"],
    "notifications": ["order.*", "payment.*", "inventory.*"],
    "analytics": ["#"],
}


def queue_main(name: str) -> str:
    return f"eventflow.{name}.q"


def queue_retry(name: str) -> str:
    return f"eventflow.{name}.retry.q"


def queue_dlq(name: str) -> str:
    return f"eventflow.dlq.{name}"


def routing_key(aggregate_type: str, event_type: str) -> str:
    if "." in event_type:
        return event_type
    return f"{aggregate_type}.{event_type}"


async def declare_topology(
    channel: AbstractChannel, settings: Settings | None = None
) -> AbstractExchange:
    cfg = settings or get_settings()
    events = await channel.declare_exchange(
        cfg.rabbitmq_exchange_events, aio_pika.ExchangeType.TOPIC, durable=True
    )
    dlx = await channel.declare_exchange(
        cfg.rabbitmq_exchange_dlx, aio_pika.ExchangeType.DIRECT, durable=True
    )
    for name in CONSUMER_NAMES:
        main_q = await channel.declare_queue(
            queue_main(name),
            durable=True,
            arguments={
                "x-dead-letter-exchange": cfg.rabbitmq_exchange_dlx,
                "x-dead-letter-routing-key": name,
            },
        )
        await channel.declare_queue(
            queue_retry(name),
            durable=True,
            arguments={
                "x-message-ttl": cfg.rabbitmq_retry_ttl_ms,
                "x-dead-letter-exchange": cfg.rabbitmq_exchange_events,
                "x-dead-letter-routing-key": BINDINGS[name][0],
            },
        )
        dlq = await channel.declare_queue(queue_dlq(name), durable=True)
        await dlq.bind(dlx, routing_key=name)
        for key in BINDINGS[name]:
            await main_q.bind(events, routing_key=key)
    return events
