import asyncio
import json

from aiokafka.errors import KafkaConnectionError, KafkaError

from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest, PublishResult
from eventflow.infrastructure.messaging.kafka.session import KafkaSession


class KafkaPublisher:
    def __init__(self, session: KafkaSession, settings: Settings | None = None) -> None:
        self._session = session
        self._settings = settings or get_settings()

    async def publish(self, message: PublishRequest) -> PublishResult:
        if message.target_transport != "kafka":
            return PublishResult(accepted=False, error="transport mismatch")
        payload = json.dumps(
            {
                "event_id": message.event_id,
                "event_type": message.event_type,
                "aggregate_type": message.aggregate_type,
                "aggregate_id": message.aggregate_id,
                "payload": message.payload,
            }
        ).encode()
        headers = [
            ("event_id", message.event_id.encode()),
            ("event_type", message.event_type.encode()),
        ]
        try:
            producer = await self._session.producer()
            record = await asyncio.wait_for(
                producer.send_and_wait(
                    self._settings.kafka_topic_events,
                    key=message.aggregate_id.encode(),
                    value=payload,
                    headers=headers,
                ),
                timeout=30.0,
            )
            return PublishResult(
                accepted=True,
                broker_meta={
                    "transport": "kafka",
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "event_id": message.event_id,
                },
            )
        except TimeoutError as exc:
            return PublishResult(accepted=False, error=f"kafka send timeout: {exc}")
        except (KafkaConnectionError, KafkaError, OSError) as exc:
            return PublishResult(accepted=False, error=str(exc))
