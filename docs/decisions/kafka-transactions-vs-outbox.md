# Kafka transactions vs PostgreSQL (EventFlow)

## Вывод

**Основной путь доставки — transactional outbox в PostgreSQL.**
Kafka transactions (read-process-write) **не** дают атомарность с PostgreSQL и **не** используются в production-потоке EventFlow.

## Почему

- EOS Kafka относится к границам внутри Kafka, не к внешней БД.
- «Exactly-once» между PG и Kafka требует отдельных паттернов (outbox, CDC); без этого — **at-least-once** + идемпотентные consumers.

## Лаборатория

`kafka_producer_idempotent` (enable.idempotence) — отдельный эксперимент против дубликатов **на стороне producer**, не бизнес-идемпотентность.
