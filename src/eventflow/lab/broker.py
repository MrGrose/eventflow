from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.infrastructure.messaging.contracts.publisher import MessagePublisher
from eventflow.infrastructure.messaging.kafka.publisher import KafkaPublisher
from eventflow.infrastructure.messaging.kafka.session import KafkaSession
from eventflow.infrastructure.messaging.rabbitmq.publisher import RabbitMQPublisher
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession


def lab_settings(transport: str) -> Settings:
    base = get_settings()
    mt = MessagingTransport.RABBITMQ if transport == "rabbitmq" else MessagingTransport.KAFKA
    return Settings(**{**base.model_dump(), "messaging_transport": mt})


@dataclass(slots=True)
class LabBrokerHandle:
    transport: str
    settings: Settings
    publisher: MessagePublisher
    _rabbit: RabbitMQSession | None = None
    _kafka: KafkaSession | None = None

    async def close(self) -> None:
        if self._kafka is not None:
            await self._kafka.close()
            self._kafka = None
        if self._rabbit is not None:
            await self._rabbit.close()
            self._rabbit = None


async def _open(transport: str) -> LabBrokerHandle:
    settings = lab_settings(transport)
    if transport == "rabbitmq":
        rs = RabbitMQSession(settings)
        await rs.connect()
        return LabBrokerHandle(
            transport=transport,
            settings=settings,
            publisher=RabbitMQPublisher(rs, settings),
            _rabbit=rs,
        )
    ks = KafkaSession(settings)
    await ks.start()
    return LabBrokerHandle(
        transport=transport, settings=settings, publisher=KafkaPublisher(ks, settings), _kafka=ks
    )


@asynccontextmanager
async def lab_broker(transport: str) -> AsyncIterator[LabBrokerHandle]:
    handle = await _open(transport)
    try:
        yield handle
    finally:
        await handle.close()
