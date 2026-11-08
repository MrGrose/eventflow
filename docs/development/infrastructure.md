# Инфраструктура Docker Compose

В файле `docker-compose.yml` описаны инфраструктурные сервисы, контейнеры приложения и стек мониторинга.

## Сервисы

| Сервис | Профиль Compose | Назначение |
|---------|-----------------|------------|
| `postgres` | по умолчанию | Основная БД PostgreSQL 16 (проброшен порт `5435:5432`) |
| `rabbitmq` | по умолчанию | Брокер RabbitMQ 3.13 с Management UI (`5672`, `15672`) |
| `kafka` | по умолчанию | Одноузловой брокер Apache Kafka 3.8 в режиме KRaft (`29092`) |
| `api` | `app` | HTTP API на FastAPI (`8000`) |
| `outbox-publisher` | `app` | Фоновый воркер публикации событий из `outbox_events` |
| `worker-rabbitmq` | `rabbitmq` | Консьюмер событий для транспорта RabbitMQ |
| `worker-kafka` | `kafka` | Консьюмер событий для транспорта Kafka |
| `prometheus` | `observability` | Сбор метрик с `/metrics` (`9090`) |
| `grafana` | `observability` | Визуализация дашборда `EventFlow Overview` (`3000`) |

## Порядок запуска и зависимости

```text
postgres (healthcheck: pg_isready)
rabbitmq (healthcheck: rabbitmq-diagnostics ping)
kafka    (healthcheck: kafka-broker-api-versions.sh)

api              → зависит от postgres (service_healthy)
outbox-publisher → зависит от postgres (service_healthy)
worker-rabbitmq  → зависит от postgres и rabbitmq (service_healthy)
worker-kafka     → зависит от postgres и kafka (service_healthy)
grafana          → зависит от prometheus
```

## Настройка Apache Kafka (KRaft)

- Образ: `apache/kafka:3.8.0` без ZooKeeper (`KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`).
- `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093`.
- Слушатели (`listeners`):
  - `CONTROLLER://:9093` — управляющий контур KRaft;
  - `PLAINTEXT://:9092` (`advertised: kafka:9092`) — для контейнеров внутри Docker-сети;
  - `PLAINTEXT_HOST://:29092` (`advertised: localhost:29092`) — для доступа с хоста.
- Фактор репликации внутренних топиков (`OFFSETS_TOPIC_REPLICATION_FACTOR`, `TRANSACTION_STATE_LOG_REPLICATION_FACTOR`) равен `1` для локального запуска.
- Данные сохраняются в volume `kafka_data:/var/lib/kafka/data`.

## Настройка RabbitMQ

- Образ: `rabbitmq:3.13-management-alpine`.
- Порт AMQP: `5672`, веб-интерфейс управления: `http://localhost:15672`.
- Данные сохраняются в volume `rabbitmq_data:/var/lib/rabbitmq`.
