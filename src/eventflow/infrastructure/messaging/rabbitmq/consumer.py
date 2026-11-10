import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.domain.reliability.errors import PermanentProcessingError, RetryableProcessingError
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession
from eventflow.infrastructure.messaging.rabbitmq.topology import queue_main, queue_retry
from eventflow.infrastructure.observability.metrics import DLQ_TOTAL

logger = logging.getLogger(__name__)
MessageHandler = Callable[[dict, AbstractIncomingMessage], Awaitable[None]]
PoisonMessageError = PermanentProcessingError
RetryableMessageError = RetryableProcessingError


def _inc_dlq(consumer: str) -> None:
    DLQ_TOTAL.labels(transport=MessagingTransport.RABBITMQ.value, consumer=consumer).inc()


class RabbitConsumerWorker:
    def __init__(
        self,
        consumer_name: str,
        handler: MessageHandler,
        settings: Settings | None = None,
        *,
        session: RabbitMQSession | None = None,
    ) -> None:
        self._name = consumer_name
        self._handler = handler
        self._settings = settings or get_settings()
        self._session = session or RabbitMQSession(self._settings)
        self._own_session = session is None
        self._stop = asyncio.Event()
        self._in_flight = 0
        self._in_flight_lock = asyncio.Lock()

    def request_stop(self) -> None:
        self._stop.set()

    @staticmethod
    def _retry_attempt(message: AbstractIncomingMessage) -> int:
        raw = (message.headers or {}).get("x-retry", 0)
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    async def _publish_retry(self, channel, message: AbstractIncomingMessage, attempt: int) -> None:
        headers = dict(message.headers or {})
        headers["x-retry"] = attempt
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=message.body,
                content_type=message.content_type,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                headers=headers,
            ),
            routing_key=queue_retry(self._name),
        )

    async def run(self, *, max_messages: int | None = None) -> None:
        await self._session.connect()
        channel = await self._session.consumer_channel()
        queue = await channel.get_queue(queue_main(self._name))
        logger.info("rabbit_consumer_started name=%s queue=%s", self._name, queue.name)
        processed = 0
        try:
            async with queue.iterator() as queue_iter:
                q_aiter = queue_iter.__aiter__()
                while not self._stop.is_set():
                    next_task = asyncio.create_task(q_aiter.__anext__())
                    stop_task = asyncio.create_task(self._stop.wait())
                    done, pending = await asyncio.wait(
                        {next_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in pending:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                    if stop_task in done and next_task not in done:
                        break
                    try:
                        message = next_task.result()
                    except StopAsyncIteration:
                        break
                    if self._stop.is_set():
                        await message.nack(requeue=True)
                        break
                    async with self._in_flight_lock:
                        self._in_flight += 1
                    try:
                        if message.redelivered:
                            logger.warning(
                                "rabbit_redelivered consumer=%s event_id=%s",
                                self._name,
                                (message.headers or {}).get("event_id"),
                            )
                        payload = json.loads(message.body.decode())
                        await self._handler(payload, message)
                        await message.ack()
                    except (
                        RetryableProcessingError,
                        TimeoutError,
                        ConnectionError,
                        OSError,
                    ) as exc:
                        attempt = self._retry_attempt(message) + 1
                        if attempt <= self._settings.rabbitmq_max_retries:
                            logger.warning(
                                "rabbit_retry consumer=%s attempt=%s err=%s",
                                self._name,
                                attempt,
                                exc,
                            )
                            await self._publish_retry(channel, message, attempt)
                            await message.ack()
                        else:
                            logger.error(
                                "rabbit_retry_exhausted consumer=%s attempts=%s err=%s",
                                self._name,
                                attempt,
                                exc,
                            )
                            await message.nack(requeue=False)
                            _inc_dlq(self._name)
                    except PermanentProcessingError as exc:
                        logger.error("rabbit_poison consumer=%s err=%s", self._name, exc)
                        await message.nack(requeue=False)
                        _inc_dlq(self._name)
                    except Exception:
                        logger.exception("rabbit_handler_failed consumer=%s", self._name)
                        await message.nack(requeue=False)
                        _inc_dlq(self._name)
                    finally:
                        async with self._in_flight_lock:
                            self._in_flight -= 1
                    processed += 1
                    if max_messages is not None and processed >= max_messages:
                        break
        finally:
            await self._wait_in_flight()
            if not channel.is_closed:
                await channel.close()
            if self._own_session:
                await self._session.close()

    async def _wait_in_flight(self, *, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            async with self._in_flight_lock:
                if self._in_flight <= 0:
                    return
            await asyncio.sleep(0.1)
        logger.warning("rabbit_shutdown_in_flight_timeout consumer=%s", self._name)
