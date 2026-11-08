# Локальный запуск

## Требования

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Docker и Docker Compose v2

## 1. Подготовка окружения

```bash
cp .env.example .env
make sync
make unit
```

## 2. Запуск инфраструктуры и миграций

```bash
make compose-up
uv run alembic upgrade head
```

Доступные локальные порты инфраструктуры:

- **PostgreSQL:** `localhost:5435` (база `eventflow`, пользователь `eventflow`)
- **RabbitMQ:** AMQP `localhost:5672`, Management UI `http://localhost:15672` (`guest` / `guest`)
- **Kafka (KRaft):** `localhost:29092`

## 3. Запуск API и воркеров

Локально через `uv`:

```bash
# HTTP API
uv run uvicorn eventflow.api.app:app --reload --port 8000

# Outbox Publisher
uv run python -m eventflow.workers.outbox_publisher

# Consumer Worker (billing или inventory)
uv run python -m eventflow.workers.rabbitmq_worker --consumer billing
# или для Kafka:
MESSAGING_TRANSPORT=kafka uv run python -m eventflow.workers.kafka_worker --consumer billing
```

Или через профили Docker Compose:

```bash
docker compose --profile app up -d --build
curl http://localhost:8000/health

# Запуск консьюмера для выбранного транспорта:
docker compose --profile rabbitmq up -d worker-rabbitmq
# или
docker compose --profile kafka up -d worker-kafka
```
