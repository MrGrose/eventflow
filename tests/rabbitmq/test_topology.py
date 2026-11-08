import pytest

from eventflow.infrastructure.messaging.rabbitmq.topology import (
    CONSUMER_NAMES,
    queue_dlq,
    queue_main,
    routing_key,
)


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_declare_topology_idempotent(rabbit_session) -> None:
    ch1 = await rabbit_session.publisher_channel()
    ch2 = await rabbit_session.publisher_channel()
    assert ch1 is ch2


def test_routing_key_format() -> None:
    assert routing_key("order", "order.confirmed") == "order.confirmed"


def test_queue_names_contract() -> None:
    for name in CONSUMER_NAMES:
        assert queue_main(name).startswith("eventflow.")
        assert queue_dlq(name).startswith("eventflow.dlq.")
