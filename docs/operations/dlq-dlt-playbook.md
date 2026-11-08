# Playbook: DLQ / DLT (read-only recovery)

## RabbitMQ (`eventflow.dlq.{consumer}`)

1. Management UI или `rabbitmqadmin list queues name messages`.
2. Прочитать сообщение, сверить `event_id` с `processed_events`.
3. Если бизнес-эффект уже есть — **purge** сообщения из DLQ (дубликат).
4. Если эффекта нет — исправить payload/данные, **re-publish** в `eventflow.events` с тем же `event_id` (at-least-once).
5. Не включать автоматический requeue без анализа poison.

## Kafka (`eventflow.events.dlt`)

1. `uv run python -m eventflow.infrastructure.messaging.kafka.replay_cli --topic eventflow.events.dlt --limit 20`
2. Проверить headers `error`, `event_id`.
3. Идемпотентный повтор через consumer после фикса причины; offset DLT не коммитить вручную в prod без runbook.

## Общее

- Не заявлять exactly-once.
- Повторная обработка всегда через `processed_events`.
