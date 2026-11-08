import pytest
import pytest_asyncio

from eventflow.config import get_settings
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.messaging.rabbitmq.topology import (
    CONSUMER_NAMES,
    queue_dlq,
    queue_main,
    queue_retry,
)


def _all_queue_names() -> list[str]:
    names: list[str] = []
    for consumer in CONSUMER_NAMES:
        names.extend([queue_main(consumer), queue_retry(consumer), queue_dlq(consumer)])
    return names


@pytest_asyncio.fixture
async def rabbit_session():
    settings = get_settings()
    session = RabbitMQSession(settings)
    try:
        await session.connect()
        yield session
    except OSError as exc:
        pytest.skip(f"rabbitmq unreachable: {exc}")
    except Exception as exc:
        pytest.skip(f"rabbitmq unavailable: {exc}")
    finally:
        await session.close()


@pytest_asyncio.fixture(autouse=True)
async def _purge_rabbit_queues(rabbit_session):
    channel = await rabbit_session.consumer_channel()
    for name in _all_queue_names():
        queue = await channel.declare_queue(name, passive=True)
        await queue.purge()
    yield
