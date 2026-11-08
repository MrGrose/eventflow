# Cross-transport idempotency matrix

| Сценарий | RabbitMQ | Kafka | PG |
|----------|----------|-------|-----|
| Повторная доставка | redeliver / DLQ | offset replay | `processed_events` ON CONFLICT |
| Два worker, один event_id | prefetch + dedupe | partition rebalance + dedupe | unique (consumer, event_id) |
| Crash до ack/commit | requeue / lease | uncommitted offset | tx rollback |
| Crash после commit PG | safe redeliver | safe redeliver | инвариант сохранён |
| Billing duplicate | — | — | один payment row |
| Inventory race | — | — | `SELECT FOR UPDATE` on stock |

Модель: **at-least-once** end-to-end; business idempotency в PostgreSQL.
