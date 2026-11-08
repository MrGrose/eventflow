# ADR-003: Семантика доставки и идемпотентность

**Статус:** Accepted (architecture)

## Решение

Модель проекта:

```text
at-least-once delivery (broker)
+
idempotent processing (PostgreSQL)
=
effectively-once business effect
```

**Не** заявлять true exactly-once end-to-end.

## `processed_events`

```text
UNIQUE (consumer_name, event_id)
```

Обработка:

```text
BEGIN;
  INSERT INTO processed_events ...;  -- или SELECT FOR UPDATE order row
  -- business mutations
COMMIT;
-- затем broker ack / offset commit
```

При дубликате: unique violation → skip business work, всё равно ack (уже обработано).

## Сценарии

| Сценарий | Поведение |
|----------|-----------|
| Дубликат сообщения | Нет второго эффекта |
| Crash до COMMIT | Повтор, обработка с нуля |
| Crash после COMMIT, до ack | Повтор, idempotent skip |
| Конкурентные два consumer'а | Один выигрывает unique constraint |

## Kafka

- Replay увеличивает дубликаты — норма.
- Manual commit после успешной DB work.
- Transactions Kafka — только в experimental track; не замена outbox для PG.

## RabbitMQ

- Manual ack после COMMIT.
- Redelivered flag — метрика, не логика «skip».
