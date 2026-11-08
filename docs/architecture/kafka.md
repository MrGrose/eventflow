# Apache Kafka Transport

Kafka используется как партиционированный распределённый лог событий с гарантией порядка в пределах партиции, независимыми consumer groups, ручным коммитом смещений (`manual offset commit`) и возможностью повторного чтения истории (`replay`).

## Роль в EventFlow

- События публикуются в топик `eventflow.events` с ключом партиционирования `key = aggregate_id` (`order_id`), что обеспечивает строгий порядок всех событий одного заказа.
- Подписчики запускаются в отдельных consumer groups вида `eventflow-{consumer}` (`eventflow-billing`, `eventflow-inventory`).

## Топология топиков (`infrastructure/messaging/kafka/bootstrap.py`)

| Топик | Партиции | Фактор репликации (локально) | Назначение |
|-------|----------|------------------------------|------------|
| `eventflow.events` | 6 | 1 | Основной поток доменных событий |
| `eventflow.events.retry` | 6 | 1 | Топик для сообщений с временными ошибками обработки |
| `eventflow.events.dlt` | 3 | 1 | Dead Letter Topic для невосстановимых сообщений |

При старте сессии `ensure_topics()` проверяет наличие топиков через `AIOKafkaAdminClient` и создаёт недостающие.

## Конфигурация KRaft (Docker Compose)

- Кластер работает в одноузловом режиме **KRaft** без ZooKeeper (`KAFKA_PROCESS_ROLES=broker,controller`).
- Слушатели (`listeners`):
  - `PLAINTEXT://kafka:9092` — для контейнеров внутри сети Docker Compose;
  - `PLAINTEXT_HOST://localhost:29092` — для подключения с хост-машины.
- Описание инфраструктурных настроек — в [infrastructure.md](../development/infrastructure.md).

## Producer (`outbox → Kafka`)

1. Захват строки в `outbox_events` (`FOR UPDATE SKIP LOCKED`).
2. Вызов `producer.send_and_wait(topic, key=aggregate_id, value=payload, headers=...)` с политикой подтверждения `acks=all`.
3. Опциональный режим идемпотентного продюсера управляется настройкой `KAFKA_PRODUCER_IDEMPOTENT`.
4. После получения `RecordMetadata` строка outbox переводится в `published`, а в `broker_meta` сохраняются `topic`, `partition` и `offset`.

Транзакции Kafka не обеспечивают атомарность совместно с PostgreSQL, поэтому связка с базой данных всегда выполняется через Transactional Outbox — см. [kafka-transactions-vs-outbox.md](../decisions/kafka-transactions-vs-outbox.md).

## Consumer (`KafkaConsumerWorker`)

1. `AIOKafkaConsumer` запускается с `enable_auto_commit=False` и отслеживанием ребалансировки партиций через `ConsumerRebalanceListener`.
2. Для каждого сообщения выполняется идемпотентная транзакция в PostgreSQL (`processed_events` + бизнес-обработчик).
3. Смещение (`OffsetAndMetadata(msg.offset + 1, "")`) коммитится вручную только после успешного `COMMIT` в БД.
4. При временной ошибке (`RetryableProcessingError`) транзакция в БД откатывается, сообщение пересылается в `eventflow.events.retry`, после чего смещение коммитится.
5. При постоянной ошибке (`PermanentProcessingError`) транзакция в БД откатывается, сообщение с заголовком `("error", b"permanent")` публикуется в `eventflow.events.dlt`, увеличивается метрика `eventflow_dlq_messages_total`, и смещение коммитится.

## Сравнение с RabbitMQ

См. [messaging-comparison.md](messaging-comparison.md).
