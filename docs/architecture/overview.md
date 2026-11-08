# Architecture Overview

## Цель системы

Синхронный HTTP API фиксирует бизнес-состояние и доменные события в PostgreSQL в рамках одной транзакции. Асинхронные воркеры доставляют и обрабатывают события через один выбранный брокер сообщений (**RabbitMQ** или **Apache Kafka**). Общая бизнес-логика и таблица outbox не скрывают архитектурные различия между очередями сообщений и партиционированным логом.

## Контекст

```text
┌─────────┐     ┌──────────────┐     ┌─────────────────┐
│ Client  │────▶│ FastAPI      │────▶│ PostgreSQL      │
└─────────┘     │ (sync API)   │     │ source of truth │
                └──────┬───────┘     └────────▲────────┘
                       │                      │
                       │ same transaction     │ consumers
                       ▼                      │
                outbox_events                  │
                       │                      │
                ┌──────▼───────┐              │
                │ outbox-      │              │
                │ publisher    │              │
                └──────┬───────┘              │
           ┌───────────┴───────────┐          │
           ▼                       ▼          │
    ┌─────────────┐         ┌─────────────┐   │
    │ RabbitMQ    │   OR    │ Kafka       │   │
    │ (transport) │         │ (transport) │   │
    └──────┬──────┘         └──────┬──────┘   │
           │                       │          │
    worker-rabbitmq         worker-kafka ────┘
    (consumers)             (consumers)
```

## Общие компоненты (transport-agnostic)

| Компонент | Ответственность |
|-----------|-----------------|
| FastAPI | Приём HTTP-запросов, валидация схем, управление транзакцией БД |
| Domain & Application | Заказы, платежи, складской резерв, идемпотентный запуск обработчиков |
| PostgreSQL | Хранение заказов, `outbox_events`, `processed_events`, платежей и остатков |
| Outbox publisher | Захват записей (`SKIP LOCKED`), публикация в активный транспорт, обновление статуса |
| Observability | Структурированные JSON-логи, метрики Prometheus, проброс `X-Correlation-ID` |

## Отдельные компоненты (per transport)

| RabbitMQ | Kafka |
|----------|-------|
| Publisher (`publisher confirms`, persistent) | Producer (`acks=all`, ключ партиционирования `aggregate_id`) |
| Consumer (`manual ACK`, `prefetch`) | Consumer (`manual offset commit`, consumer group) |
| Exchanges (`eventflow.events`, `eventflow.dlx`), bindings | Topics (`eventflow.events`, `retry`, `dlt`), partitions |
| Retry queues (`x-message-ttl`) + DLQ per consumer | Retry topic + DLT topic |

## Переключение транспорта

- Активный транспорт задаётся переменной окружения `MESSAGING_TRANSPORT=rabbitmq|kafka`.
- В Docker Compose воркеры разделены по профилям (`--profile rabbitmq` и `--profile kafka`).
- Поле `target_transport` в таблице `outbox_events` фиксируется при записи события и проверяется при выборке в `outbox-publisher` — см. [ADR-002](../decisions/adr-002-outbox-dual-transport.md).

## Структура кода

```text
src/eventflow/
  domain/
  application/
  infrastructure/
    db/
    idempotency/
    observability/
    outbox/
    messaging/
      contracts/
      rabbitmq/
      kafka/
  api/
  workers/
  lab/
```

## Инварианты

1. API никогда не публикует сообщения в брокер напрямую внутри HTTP-запроса.
2. Подписчики идемпотентны по ключу `(consumer_name, event_id)` в таблице `processed_events`.
3. Подтверждение брокера (`publisher confirm` / `RecordMetadata`) означает доставку в брокер, а `ACK` / `commit offset` консьюмера выполняется только после `COMMIT` бизнес-транзакции в PostgreSQL.
4. Невосстановимые сообщения (poison messages / постоянные бизнес-ошибки) направляются в DLQ (RabbitMQ) или DLT (Kafka).

## Связанные документы

- [event-flow.md](event-flow.md)
- [rabbitmq.md](rabbitmq.md)
- [kafka.md](kafka.md)
- [messaging-comparison.md](messaging-comparison.md)
