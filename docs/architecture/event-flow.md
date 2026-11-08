# Event Flow (домен заказов)

## События (контракт)

| `event_type` | Описание | Источник |
|------------|----------|----------|
| `order.created` | Заказ создан | API / `OrderService.create_order` |
| `order.confirmed` | Заказ подтверждён | API / `OrderService.confirm` |
| `order.cancelled` | Заказ отменён | API / `OrderService.cancel` |
| `payment.requested` | Запрошен платёж | Доменная модель событий |
| `payment.completed` | Успешная оплата | Billing |
| `payment.failed` | Ошибка оплаты | Billing |
| `inventory.reserved` | Резерв создан | Inventory |
| `inventory.reservation_failed` | Недостаточно остатка | Inventory |

Конверт события (`EventEnvelope`), сохраняемый в `outbox_events` и передаваемый через брокер:

```json
{
  "event_id": "2c5f4e6a-8b9d-4c11-9e12-4a2b3c4d5e6f",
  "event_type": "order.confirmed",
  "aggregate_type": "order",
  "aggregate_id": "8f14e45f-ceea-467a-9a38-123456789abc",
  "occurred_at": "2026-11-08T10:00:00Z",
  "correlation_id": null,
  "payload": {
    "customer_id": "cust-1",
    "total": "37.50",
    "items": [
      {"sku": "SKU-1", "quantity": 3, "unit_price": "12.50"}
    ]
  }
}
```

Транспортный адаптер передаёт метаданные в заголовках (AMQP headers в RabbitMQ / record headers в Kafka), сохраняя неизменным `event_id`.

## Основной сценарий обработки

```text
POST /orders               →  order.created (outbox)
POST /orders/{id}/confirm  →  order.confirmed (outbox)

outbox-publisher           →  RabbitMQ или Kafka

Billing worker             ←  order.confirmed  → запись в payments (status=completed)
Inventory worker           ←  order.confirmed  → списание резерва в stock_items (FOR UPDATE)
```

Воркеры не вызывают друг друга синхронно и работают в изолированных транзакциях БД.

## Подписчики (Consumers)

| Consumer | Подписка | Механизм идемпотентности и изоляции |
|----------|----------|-------------------------------------|
| `billing` | `order.confirmed`, `payment.*` | `processed_events` + проверка завершённого платежа по `order_id` |
| `inventory` | `order.confirmed` | `processed_events` + `SELECT ... FOR UPDATE` по строкам `stock_items` |
| `notifications` | `order.*`, `payment.*`, `inventory.*` | Топология очередей; дедупликация по `event_id` |
| `analytics` | `#` (все события) | Топология очередей; дедупликация по `event_id` |

## Запись в Transactional Outbox

```text
BEGIN;
  -- запись/обновление заказа в orders и order_items
  INSERT INTO outbox_events (... status='pending' ...);
COMMIT;
-- возврат HTTP 201 / 200 клиенту

-- асинхронно в outbox-publisher:
FOR UPDATE SKIP LOCKED claim (status='publishing', lease_expires_at)
  → publish в брокер
  → ожидание подтверждения транспорта
  → UPDATE outbox_events SET status='published', published_at=now()
```

## Идемпотентная обработка в Consumer

```text
получение сообщения из брокера
  → разбор конверта (event_id, payload)
  → BEGIN
       INSERT INTO processed_events (consumer_name, event_id)
       ON CONFLICT DO NOTHING RETURNING event_id;
       -- если строка уже была: COMMIT и подтверждение сообщения без повтора бизнес-логики
       -- если событие новое: выполнение бизнес-операции (payments / stock_items)
     COMMIT
  → ACK сообщения в RabbitMQ / ручной commit offset в Kafka
```

Если `COMMIT` в PostgreSQL прошёл успешно, но процесс воркера завершился до отправки `ACK` или коммита offset, брокер доставит сообщение повторно (**at-least-once**), а таблица `processed_events` предотвратит повторный бизнес-эффект.

## Состояния заказа

Заказ создаётся в статусе `draft`, после чего может быть переведён в `confirmed` или `cancelled`. В модели статуса заказа также предусмотрены состояния оплаты `paid` и `payment_failed`.

## Обработка ошибок

- **Временные сбои (`RetryableProcessingError`, сетевые таймауты):** транзакция БД откатывается (`rollback`), в RabbitMQ выполняется `nack(requeue=True)`, в Kafka сообщение направляется в `eventflow.events.retry`.
- **Постоянные ошибки (`PermanentProcessingError`, невалидный payload, нехватка остатка на складе):** транзакция БД откатывается, сообщение отправляется в `eventflow.dlq.{consumer}` (RabbitMQ) или `eventflow.events.dlt` (Kafka) с инкрементом метрики `eventflow_dlq_messages_total`.
