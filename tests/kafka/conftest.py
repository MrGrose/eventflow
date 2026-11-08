import pytest
import pytest_asyncio

from eventflow.config import get_settings
from eventflow.infrastructure.messaging.kafka.session import KafkaSession


@pytest_asyncio.fixture
async def kafka_session():
    settings = get_settings()
    session = KafkaSession(settings)
    try:
        await session.start()
        yield session
    except Exception as exc:
        pytest.skip(f"kafka unavailable: {exc}")
    finally:
        await session.close()
