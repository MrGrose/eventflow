import aio_pika
import pytest

from eventflow.config import get_settings


@pytest.mark.rabbitmq
@pytest.mark.asyncio
async def test_rabbitmq_connect_and_ping() -> None:
    settings = get_settings()
    try:
        connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=5)
    except OSError:
        pytest.skip("rabbitmq unreachable")
    async with connection:
        channel = await connection.channel()
        assert channel.is_initialized
