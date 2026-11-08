import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.structs import OffsetAndMetadata

from eventflow.config import MessagingTransport, Settings, get_settings
from eventflow.domain.reliability.errors import PermanentProcessingError, RetryableProcessingError
from eventflow.infrastructure.messaging.kafka.metrics import inc_kafka_consumer
from eventflow.infrastructure.messaging.kafka.session import KafkaSession
from eventflow.infrastructure.messaging.kafka.topology import consumer_group
from eventflow.infrastructure.observability.metrics import DLQ_TOTAL

logger = logging.getLogger(__name__)
KafkaHandler = Callable[[dict, bytes], Awaitable[None]]
PermanentMessageError = PermanentProcessingError
RetryableMessageError = RetryableProcessingError


def _inc_dlt(consumer: str) -> None:
    DLQ_TOTAL.labels(transport=MessagingTransport.KAFKA.value, consumer=consumer).inc()


def _retry_count(headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None) -> int:
    if not headers:
        return 0
    count = 0
    for key, val in headers:
        if key == "retry":
            try:
                count = max(count, int(val.decode()))
            except (ValueError, UnicodeDecodeError):
                count += 1
    return count


def _with_header(
    headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None, key: str, value: bytes
) -> list[tuple[str, bytes]]:
    filtered = [(k, v) for k, v in headers or [] if k != key]
    filtered.append((key, value))
    return filtered


class LoggingRebalanceListener(ConsumerRebalanceListener):
    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        if revoked:
            logger.warning("kafka_partitions_revoked %s", revoked)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        if assigned:
            logger.info("kafka_partitions_assigned %s", assigned)


class KafkaConsumerWorker:
    def __init__(
        self,
        consumer_name: str,
        handler: KafkaHandler,
        settings: Settings | None = None,
        *,
        session: KafkaSession | None = None,
    ) -> None:
        self._name = consumer_name
        self._handler = handler
        self._settings = settings or get_settings()
        self._session = session or KafkaSession(self._settings)
        self._own_session = session is None
        self._stop = asyncio.Event()
        self._in_flight = 0

    def request_stop(self) -> None:
        self._stop.set()

    async def _send_dlt(self, msg, *, reason: str = "permanent") -> None:
        producer = await self._session.producer()
        headers = _with_header(msg.headers, "error", reason.encode())
        headers = _with_header(headers, "target_consumer", self._name.encode())
        await producer.send_and_wait(
            self._settings.kafka_topic_dlt, key=msg.key, value=msg.value, headers=headers
        )

    async def _send_retry(self, msg, *, attempt: int) -> None:
        producer = await self._session.producer()
        headers = _with_header(msg.headers, "retry", str(attempt).encode())
        headers = _with_header(headers, "target_consumer", self._name.encode())
        await producer.send_and_wait(
            self._settings.kafka_topic_retry, key=msg.key, value=msg.value, headers=headers
        )

    def _matches_target_consumer(self, msg) -> bool:
        if msg.topic != self._settings.kafka_topic_retry:
            return True
        for key, val in msg.headers or []:
            if key == "target_consumer":
                return val.decode(errors="ignore") == self._name
        return True

    async def _get_next_message(self, consumer: AIOKafkaConsumer, *, idle_timeout: float | None):
        deadline = time.monotonic() + idle_timeout if idle_timeout is not None else None
        while not self._stop.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                return None
            batch = await consumer.getmany(timeout_ms=200, max_records=1)
            for records in batch.values():
                for msg in records:
                    return msg
        return None

    async def run(
        self, *, max_messages: int | None = None, idle_timeout: float | None = None
    ) -> None:
        await self._session.start()
        group = consumer_group(self._name, self._settings)
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset=self._settings.kafka_consumer_auto_offset_reset,
            session_timeout_ms=self._settings.kafka_session_timeout_ms,
        )
        await consumer.start()
        consumer.subscribe(
            [self._settings.kafka_topic_events, self._settings.kafka_topic_retry],
            listener=LoggingRebalanceListener(),
        )
        logger.info("kafka_consumer_started name=%s group=%s", self._name, group)
        processed = 0
        eff_idle = idle_timeout if idle_timeout is not None else 5.0 if max_messages else None
        try:
            while not self._stop.is_set():
                msg = await self._get_next_message(consumer, idle_timeout=eff_idle)
                if msg is None:
                    break
                tp = TopicPartition(msg.topic, msg.partition)
                if not self._matches_target_consumer(msg):
                    await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                    continue
                attempts_so_far = _retry_count(msg.headers)
                if attempts_so_far > 0 and self._settings.kafka_retry_backoff_ms > 0:
                    await asyncio.sleep(self._settings.kafka_retry_backoff_ms / 1000.0)
                self._in_flight += 1
                try:
                    payload = json.loads(msg.value.decode())
                    await self._handler(payload, msg.key or b"")
                    await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                    inc_kafka_consumer(self._name, "committed")
                except (RetryableProcessingError, TimeoutError, ConnectionError, OSError) as exc:
                    next_attempt = attempts_so_far + 1
                    if next_attempt <= self._settings.kafka_max_retries:
                        logger.warning(
                            "kafka_retry name=%s attempt=%s err=%s", self._name, next_attempt, exc
                        )
                        await self._send_retry(msg, attempt=next_attempt)
                        await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                        inc_kafka_consumer(self._name, "retry")
                    else:
                        logger.error(
                            "kafka_retry_exhausted_to_dlt name=%s attempts=%s err=%s",
                            self._name,
                            next_attempt,
                            exc,
                        )
                        await self._send_dlt(msg, reason="retry_exhausted")
                        await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                        inc_kafka_consumer(self._name, "dlt")
                        _inc_dlt(self._name)
                except PermanentProcessingError as exc:
                    logger.error("kafka_dlt name=%s err=%s", self._name, exc)
                    await self._send_dlt(msg, reason="permanent")
                    await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                    inc_kafka_consumer(self._name, "dlt")
                    _inc_dlt(self._name)
                except Exception:
                    logger.exception("kafka_handler_failed name=%s", self._name)
                    await self._send_dlt(msg, reason="unhandled")
                    await consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "")})
                    inc_kafka_consumer(self._name, "dlt")
                    _inc_dlt(self._name)
                finally:
                    self._in_flight -= 1
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    break
        finally:
            deadline = time.monotonic() + 30
            while self._in_flight > 0 and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            await consumer.stop()
            if self._own_session:
                await self._session.close()
