# Observability (EventFlow)

## Логи

- Формат: JSON в stdout (`eventflow.infrastructure.logging`).
- Поля: `correlation_id`, `event_id`, `consumer`, `transport`, `duration_sec`, `retry_count` (если заданы в `extra`).

## Метрики

- Endpoint: `GET /metrics` (Prometheus text).
- Readiness: `GET /ready` — PostgreSQL + активный брокер (`MESSAGING_TRANSPORT`).

Локально:

```bash
docker compose --profile observability up -d prometheus grafana
# Grafana: http://localhost:3000 (admin / admin)
```

## Проверка при отказах

1. Остановить RabbitMQ или Kafka — `/ready` → `degraded`, `checks.broker` ≠ ok.
2. Прогнать `tests/failure` — счётчики `eventflow_events_failed_total` и `eventflow_retries_total` растут.
3. DLQ/DLT — см. `docs/operations/dlq-dlt-playbook.md`.
