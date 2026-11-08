# RabbitMQ Transport

RabbitMQ используется как брокер очередей с маршрутизацией через exchanges, подтверждением доставки от брокера (`publisher confirms`), ручным подтверждением обработки консьюмерами (`manual ACK`) и маршрутизацией сбойных сообщений через Dead Letter Exchange (DLX).

## Роль в EventFlow

- `outbox-publisher` публикует события в durable topic exchange `eventflow.events`.
- Ключ маршрутизации (`routing_key`) соответствует типу события (`order.confirmed`, `order.created` и т.д.).
- Каждый консьюмер читает из собственной durable-очереди с настроенными привязками (`bindings`).

## Топология (`infrastructure/messaging/rabbitmq/topology.py`)

```text
                    eventflow.events (topic, durable)
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
  eventflow.billing.q  eventflow.inventory.q  ...
         │                 │
   billing worker    inventory worker


Dead letter:
  eventflow.dlx (direct, durable) → eventflow.dlq.{consumer}
Retry queue:
  eventflow.{consumer}.retry.q (x-message-ttl) → eventflow.events
```

Основные параметры топологии:

- **Exchanges:** `eventflow.events` (`topic`, durable) и `eventflow.dlx` (`direct`, durable).
- **Очереди:**
  - `eventflow.{consumer}.q` — основная очередь консьюмера с аргументами `x-dead-letter-exchange: eventflow.dlx` и `x-dead-letter-routing-key: {consumer}`.
  - `eventflow.{consumer}.retry.q` — очередь отложенного повтора с `x-message-ttl` (`RABBITMQ_RETRY_TTL_MS`, по умолчанию 5000 мс) и возвратом в `eventflow.events`.
  - `eventflow.dlq.{consumer}` — очередь недоставленных (poison) сообщений, привязанная к `eventflow.dlx`.
- **Bindings:**
  - `billing`: `order.confirmed`, `payment.completed`, `payment.failed`
  - `inventory`: `order.confirmed`
  - `notifications`: `order.*`, `payment.*`, `inventory.*`
  - `analytics`: `#`

## Publisher (`outbox → RabbitMQ`)

1. Захват строк в `outbox_events` (`FOR UPDATE SKIP LOCKED`, перевод в статус `publishing`).
2. Публикация persistent-сообщения (`DeliveryMode.PERSISTENT`) с ожиданием **publisher confirm** от брокера (`RABBITMQ_PUBLISH_CONFIRM_TIMEOUT`).
3. После получения confirm: перевод записи outbox в статус `published`, заполнение `published_at` и `broker_meta` (`exchange`, `routing_key`, `event_id`).
4. При ошибке или таймауте: возврат строки в `pending` с расчётом `next_attempt_at` по экспоненциальному backoff.

Подтверждение брокера (`publisher confirm`) означает только то, что сообщение принято RabbitMQ, и не заменяет подтверждение обработки консьюмером.

## Consumer (`RabbitConsumerWorker`)

1. Канал консьюмера создаётся с ограничением `prefetch_count` (`RABBITMQ_PREFETCH`, по умолчанию 10).
2. На каждое сообщение открывается транзакция в PostgreSQL через `run_idempotent()`.
3. Вызов `message.ack()` выполняется только после успешного `COMMIT` в БД.
4. При временной ошибке (`RetryableProcessingError`, сетевой сбой) транзакция БД откатывается и вызывается `message.nack(requeue=True)`.
5. При постоянной ошибке (`PermanentProcessingError`) транзакция БД откатывается, вызывается `message.nack(requeue=False)`, сообщение перенаправляется через `eventflow.dlx` в `eventflow.dlq.{consumer}`, а счётчик `eventflow_dlq_messages_total` увеличивается.

## Локальная отладка

В `docker-compose.yml` сервис `rabbitmq` использует образ `rabbitmq:3.13-management-alpine` (AMQP порт `5672`, Management UI `http://localhost:15672`, healthcheck `rabbitmq-diagnostics -q ping`).

Сравнение с транспортом Kafka приведено в [messaging-comparison.md](messaging-comparison.md).
