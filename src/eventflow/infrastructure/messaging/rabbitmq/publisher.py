import asyncio
import json

import aio_pika

from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest, PublishResult
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.messaging.rabbitmq.topology import routing_key


class RabbitMQPublisher:
    def __init__(self, session: RabbitMQSession, settings: Settings | None = None) -> None:
        self._session = session
        self._settings = settings or get_settings()

    async def publish(self, message: PublishRequest) -> PublishResult:
        if message.target_transport != "rabbitmq":
            return PublishResult(accepted=False, error="transport mismatch")
        try:
            if self._session.connection is None or self._session.connection.is_closed:
                await asyncio.wait_for(
                    self._session.connect(), timeout=self._settings.rabbitmq_publish_confirm_timeout
                )
            channel = await self._session.publisher_channel()
            exchange = await channel.get_exchange(self._settings.rabbitmq_exchange_events)
            body = json.dumps(
                {
                    "event_id": message.event_id,
                    "event_type": message.event_type,
                    "aggregate_type": message.aggregate_type,
                    "aggregate_id": message.aggregate_id,
                    "payload": message.payload,
                }
            ).encode()
            rk = routing_key(message.aggregate_type, message.event_type)
            amqp_message = aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                headers={"event_id": message.event_id},
            )
            await exchange.publish(
                amqp_message,
                routing_key=rk,
                timeout=self._settings.rabbitmq_publish_confirm_timeout,
            )
            return PublishResult(
                accepted=True,
                broker_meta={
                    "transport": "rabbitmq",
                    "exchange": self._settings.rabbitmq_exchange_events,
                    "routing_key": rk,
                    "event_id": message.event_id,
                },
            )
        except TimeoutError as exc:
            return PublishResult(accepted=False, error=f"confirm timeout: {exc}")
        except (aio_pika.exceptions.AMQPError, OSError, ConnectionError) as exc:
            return PublishResult(accepted=False, error=str(exc))
