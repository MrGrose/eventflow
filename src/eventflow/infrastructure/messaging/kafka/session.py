import logging

from aiokafka import AIOKafkaProducer

from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.kafka.bootstrap import ensure_topics

logger = logging.getLogger(__name__)


class KafkaSession:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    async def start(self) -> None:
        if self._started and self._producer is not None:
            return
        await ensure_topics(self._settings)
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            acks=self._settings.kafka_producer_acks,
            enable_idempotence=self._settings.kafka_producer_idempotent,
        )
        await self._producer.start()
        self._started = True
        logger.info("kafka_producer_started")

    async def producer(self) -> AIOKafkaProducer:
        await self.start()
        assert self._producer is not None
        return self._producer

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._started = False

    async def __aenter__(self) -> "KafkaSession":
        await self.start()
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()
