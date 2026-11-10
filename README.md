# EventFlow

EventFlow is an asynchronous Python backend for order processing and domain events. It combines a FastAPI HTTP API, PostgreSQL, a transactional outbox, and **one** message broker per deployment (RabbitMQ or Kafka) with idempotent consumers.

## Overview

EventFlow handles the full path from order APIs to downstream side effects:

- **Orders** are created and confirmed through the HTTP API.
- **Business state and outbox rows** are written in the same PostgreSQL transaction.
- **Events** are published asynchronously by a dedicated outbox worker (not inside the request transaction).
- **Billing** and **inventory** consumers process each event independently.
- **Duplicate deliveries** are absorbed via `processed_events` and `run_idempotent()` so business effects are not applied twice.

The system is designed around explicit delivery semantics: at-least-once messaging with idempotent handlers—not end-to-end exactly-once.

## Key Features

- FastAPI HTTP API (`/orders`, health, readiness, OpenAPI docs)
- SQLAlchemy 2.x async + `asyncpg`
- PostgreSQL as the system of record
- Transactional outbox (`outbox_events` + publisher worker)
- RabbitMQ transport (`aio-pika`) or Kafka transport (`aiokafka`), selected by `MESSAGING_TRANSPORT`
- Active consumers: **billing** and **inventory** only
- Idempotent consumer pipeline (`run_idempotent()`, `processed_events`)
- RabbitMQ: manual ACK, retry queue with TTL and bounded attempts, DLQ
- Kafka: retry topic, DLT, manual offset commit after successful processing
- Docker Compose for local infrastructure (and optional app/observability profiles)
- Alembic schema migrations
- Prometheus metrics (`GET /metrics`) and Grafana dashboard (optional profile)
- Automated tests (`pytest`, transport markers, comparative lab harness)

## Technology Stack

| Area | Technology |
|------|------------|
| Language | Python **≥ 3.12** |
| API | FastAPI **≥ 0.115** |
| Database | PostgreSQL |
| ORM | SQLAlchemy **≥ 2.0** (async) |
| Migrations | Alembic **≥ 1.14** |
| Messaging | RabbitMQ (`aio-pika` **≥ 9.4**) **or** Kafka (`aiokafka` **≥ 0.10**) |
| Monitoring | Prometheus (`prometheus-client` **≥ 0.21**), Grafana (Compose profile) |
| Runtime | Docker Compose, `uv` |
| Testing | pytest **≥ 8**, pytest-asyncio, httpx |

## Architecture

### Components

| Component | Role |
|-----------|------|
| **FastAPI** | HTTP edge, validation, routing |
| **Order Service** | Order use cases (create, confirm, cancel) |
| **PostgreSQL** | Orders, payments/stock tables, `outbox_events`, `processed_events` |
| **Transactional Outbox** | Durable event intent in the same DB transaction as order changes |
| **Outbox Publisher** | Claims pending rows and publishes to the active broker |
| **Messaging transport** | **Either** RabbitMQ **or** Kafka (`MESSAGING_TRANSPORT`) |
| **Billing Worker** | Consumes events; billing side effects |
| **Inventory Worker** | Consumes events; inventory side effects |
| **Idempotency layer** | `run_idempotent()` + `processed_events` |
| **Monitoring** | Prometheus scrape of `/metrics`; Grafana dashboard (optional) |

### Diagram

Only **one** broker is active per deployment. The diagram shows both options; production wiring follows `MESSAGING_TRANSPORT`.

```mermaid
flowchart TB
  Client([Client])
  API[FastAPI]
  OS[Order Service]
  PG[(PostgreSQL)]
  OP[Outbox Publisher]
  BW[Billing Worker]
  IW[Inventory Worker]
  MON[Prometheus / Grafana]

  Client --> API
  API --> OS
  OS --> PG
  OP --> PG
  OP --> Broker{MESSAGING_TRANSPORT}
  Broker -.->|rabbitmq| RMQ[RabbitMQ]
  Broker -.->|kafka| KFK[Kafka]
  RMQ --> BW
  RMQ --> IW
  KFK --> BW
  KFK --> IW
  BW --> PG
  IW --> PG
  MON -.-> API
```

## Event Processing

Typical lifecycle for an order-domain event:

1. The API accepts a request (e.g. `POST /orders`).
2. **Order Service** runs the business operation.
3. Order row and **outbox event** are inserted/updated in **one PostgreSQL transaction**.
4. **Outbox Publisher** polls and claims pending outbox rows.
5. The event is published to the **configured broker** (RabbitMQ or Kafka).
6. **Billing** and **inventory** workers consume the message **in parallel** (separate consumer processes).
7. Each handler runs inside **`run_idempotent()`**.
8. **`processed_events`** records the consumer + event id; duplicates short-circuit before business logic.
9. Business updates (e.g. payments, stock) commit in PostgreSQL.
10. **RabbitMQ**: manual **ACK** after successful processing. **Kafka**: **offset commit** after the handler succeeds, and after publishing to the retry topic or DLT when the attempt is not retried.

```mermaid
sequenceDiagram
  participant C as Client
  participant API as FastAPI
  participant OS as Order Service
  participant PG as PostgreSQL
  participant OP as Outbox Publisher
  participant B as Broker
  participant W as Consumer
  participant ID as processed_events

  C->>API: POST /orders
  API->>OS: create order
  OS->>PG: BEGIN txn (order + outbox)
  OS->>PG: COMMIT
  OP->>PG: claim pending outbox
  OP->>B: publish envelope
  B->>W: deliver (at-least-once)
  W->>ID: run_idempotent()
  alt already processed
    ID-->>W: skip business effect
  else first time
    W->>PG: business writes + idempotency row
  end
  Note over W,B: Rabbit: ACK · Kafka: commit offset
```

## Reliability and Delivery Semantics

- **Transactional Outbox** — The API does not publish to a broker inside the HTTP transaction. Publishing happens only via the outbox publisher after the DB commit.
- **At-least-once delivery** — Brokers may redeliver messages; consumers must tolerate duplicates.
- **Idempotent processing** — `run_idempotent()` wraps handler work and uses `processed_events` for deduplication keys (`consumer_name`, `event_id`).

**At-least-once delivery + idempotent processing = effectively-once business effect.**

This is **not** true exactly-once delivery end-to-end.

| Concern | RabbitMQ | Kafka |
|---------|----------|-------|
| Success | Manual **ACK** | **Commit offset** after success |
| Transient failure | Publish to **retry queue** with `x-retry`, then DLQ after `RABBITMQ_MAX_RETRIES` | Publish to **retry topic**, bounded retries |
| Permanent failure | **DLQ** (dead-letter exchange) | **DLT** (dead-letter topic) |
| Duplicate delivery | Same idempotency key → no duplicate business effect | Same |

See [docs/operations/dlq-dlt-playbook.md](docs/operations/dlq-dlt-playbook.md) and [docs/decisions/adr-003-delivery-semantics.md](docs/decisions/adr-003-delivery-semantics.md).

## Project Structure

```
scripts/            # Demo seed and idempotency cleanup helpers
src/eventflow/
  api/              # FastAPI app, routes, request/response schemas
  lab/              # Comparative broker experiments (excluded from the wheel)
  application/      # Use cases, order service, consumer dispatch, idempotency
  domain/           # Order aggregate, domain errors
  infrastructure/   # DB, messaging (rabbitmq/kafka), outbox, observability
  workers/          # Outbox publisher, RabbitMQ/Kafka consumer entrypoints
  config/           # Pydantic settings from environment
alembic/            # Database migrations
tests/              # Unit, integration, transport-specific and lab tests
observability/      # Prometheus config, Grafana provisioning and dashboards
docs/               # Architecture notes, ADRs, operations playbooks
```

Layering keeps HTTP thin; rules live in `application/` and `domain/`.

## Requirements

- **Python** 3.12+
- **[uv](https://github.com/astral-sh/uv)** (recommended) or another PEP 517 install path
- **Docker** and **Docker Compose** for PostgreSQL and brokers (and optional observability)

Redis is not used in this project.

## Quick Start

1. **Clone** the repository.

2. **Configure environment:**

   ```bash
   cp .env.example .env
   ```

3. **Start infrastructure:**

   ```bash
   docker compose up -d postgres rabbitmq kafka
   ```

4. **Install dependencies and migrate:**

   ```bash
   uv sync --all-extras
   uv run alembic upgrade head
   uv run python scripts/seed_stock.py
   ```

5. **Run the API:**

   ```bash
   uv run uvicorn eventflow.api.app:app --reload --host 0.0.0.0 --port 8000
   ```

6. **Smoke-check the API:**

   ```bash
   curl -s http://localhost:8000/health
   curl -s http://localhost:8000/ready
   ```

7. **Open interactive API docs:** [http://localhost:8000/docs](http://localhost:8000/docs)

8. **Create an order (example):**

   ```bash
   curl -s -X POST http://localhost:8000/orders \
     -H 'Content-Type: application/json' \
     -d '{"customer_id":"cust-1","items":[{"sku":"SKU-1","quantity":2,"unit_price":"19.99"}]}'
   ```

9. **Run background workers** (separate terminals; match `MESSAGING_TRANSPORT` in `.env`):

   ```bash
   uv run python -m eventflow.workers.outbox_publisher
   ```

   RabbitMQ:

   ```bash
   uv run python -m eventflow.workers.rabbitmq_worker --consumer billing
   uv run python -m eventflow.workers.rabbitmq_worker --consumer inventory
   ```

   Kafka:

   ```bash
   uv run python -m eventflow.workers.kafka_worker --consumer billing
   uv run python -m eventflow.workers.kafka_worker --consumer inventory
   ```

Optional: run the full stack in containers with Compose profiles (`app`, `rabbitmq` or `kafka`, `observability`)—see `docker-compose.yml`. The `migrate` service applies Alembic migrations before the API and workers start.

## Configuration

| Variable | Purpose | Typical value |
|----------|---------|----------------|
| `DATABASE_URL` | Async SQLAlchemy URL | `postgresql+asyncpg://eventflow:eventflow@localhost:5435/eventflow` (host) |
| `MESSAGING_TRANSPORT` | Active broker | `rabbitmq` or `kafka` |
| `RABBITMQ_URL` | AMQP connection | `amqp://guest:guest@localhost:5672/` |
| `RABBITMQ_EXCHANGE_EVENTS` | Main events exchange | `eventflow.events` |
| `RABBITMQ_EXCHANGE_DLX` | Dead-letter exchange | `eventflow.dlx` |
| `RABBITMQ_RETRY_TTL_MS` | Retry queue delay | `5000` |
| `RABBITMQ_MAX_RETRIES` | Retry attempts before DLQ | `3` |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker list | `localhost:29092` (host port mapping) |
| `KAFKA_TOPIC_EVENTS` | Primary topic | `eventflow.events` |
| `KAFKA_TOPIC_RETRY` | Retry topic | `eventflow.events.retry` |
| `KAFKA_TOPIC_DLT` | Dead-letter topic | `eventflow.events.dlt` |
| `KAFKA_MAX_RETRIES` | Consumer retry budget | `3` |
| `KAFKA_RETRY_BACKOFF_MS` | Backoff between retries | `200` |
| `APP_ENV` | Environment label | `development` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `GRAFANA_URL` / `GRAFANA_USER` / `GRAFANA_PASSWORD` | Dashboard validation script (optional) | see `.env.example` |

Do not commit real secrets. Use `.env` locally and your platform’s secret store in deployed environments.

## Testing

Tests live under `tests/` with markers:

- Default: unit and integration tests
- `@pytest.mark.rabbitmq` — needs RabbitMQ
- `@pytest.mark.kafka` — needs Kafka
- `@pytest.mark.lab` — comparative broker lab (E1–E9)

Infrastructure must be up (`docker compose up -d postgres rabbitmq kafka`), migrations applied. For a clean broker/DB baseline before a full run:

```bash
make lab-reset   # optional
uv run pytest -q
uv run pytest -q -m rabbitmq
uv run pytest -q -m kafka
```

CI runs `uv run ruff check .`, `uv run ruff format --check .`, the unit suite, the RabbitMQ and integration suites, and the Kafka suite against a Compose broker (see `.github/workflows/ci.yml`).

## Observability

- **Metrics:** `GET /metrics` (Prometheus text format)
- **Counters/histograms** include `eventflow_events_published_total`, `eventflow_events_processed_total`, `eventflow_events_failed_total`, `eventflow_processing_duration_seconds`, `eventflow_retries_total`, `eventflow_dlq_total` (labels vary by transport/stage).

Start Prometheus and Grafana locally:

```bash
docker compose --profile observability up -d prometheus grafana
```

- Prometheus: [http://localhost:9090](http://localhost:9090)
- Grafana: [http://localhost:3000](http://localhost:3000) (default compose credentials; change for anything beyond local use)
- Dashboard JSON: `observability/grafana/dashboards/eventflow.json`

Observability in Compose is intended for **local** insight, not a complete production monitoring setup.

## Engineering Decisions

| Topic | Decision | Reference |
|-------|----------|-----------|
| System of record | PostgreSQL | [docs/architecture/overview.md](docs/architecture/overview.md) |
| Event publication | Transactional outbox | [docs/decisions/adr-002-outbox-dual-transport.md](docs/decisions/adr-002-outbox-dual-transport.md) |
| Transports | Separate RabbitMQ and Kafka packages; one active per deploy | [docs/decisions/adr-001-dual-transport.md](docs/decisions/adr-001-dual-transport.md) |
| Delivery semantics | At-least-once + idempotent consumers | [docs/decisions/adr-003-delivery-semantics.md](docs/decisions/adr-003-delivery-semantics.md) |
| Kafka client | `aiokafka`, explicit commit policy | [docs/decisions/adr-004-kafka-client.md](docs/decisions/adr-004-kafka-client.md) |
| Caching | No Redis | — |
| Layering | API / application / domain / infrastructure | [docs/architecture/overview.md](docs/architecture/overview.md) |

More ADRs: [docs/decisions/](docs/decisions/).

## Operational Limitations

Current gaps to account for when hardening for production:

- **No API authentication or authorization** on HTTP routes.
- **Broker connections** in the default Compose stack use plain TCP (no TLS/SASL).
- **Default database and broker credentials** are suitable for local development only.
- **Secrets** must be supplied via environment or a secret manager; nothing in-repo replaces rotation policy.
- **One messaging transport per deployment**—do not publish the same outbox event to both RabbitMQ and Kafka in one scenario.
- **Comparative lab** and Compose profiles are development tooling; they are not a deployment blueprint.

EventFlow is a **complete software system** with explicit semantics, but it is **not** positioned as production-ready without additional security, deployment, and operations work.

## Documentation

- Architecture: [docs/architecture/](docs/architecture/)
- Operations: [docs/operations/](docs/operations/)
- Broker comparison: [docs/architecture/messaging-comparison.md](docs/architecture/messaging-comparison.md)
- Idempotency: [docs/architecture/idempotency-transactions.md](docs/architecture/idempotency-transactions.md)

## License

MIT. See [LICENSE](LICENSE).
