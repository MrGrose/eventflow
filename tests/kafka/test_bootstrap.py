import pytest

from eventflow.config import get_settings
from eventflow.infrastructure.messaging.kafka.bootstrap import ensure_topics


@pytest.mark.kafka
@pytest.mark.asyncio
async def test_ensure_topics_idempotent(kafka_session) -> None:
    settings = get_settings()
    await ensure_topics(settings)
    await ensure_topics(settings)
