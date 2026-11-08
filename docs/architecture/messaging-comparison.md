# Сравнительная лаборатория RabbitMQ и Kafka

В этом документе приведены протокол сравнительного тестирования и фактические результаты замеров обоих транспортов на одинаковых сценариях (E1–E9).

## Условия проведения замеров

- В каждом прогоне активен один транспорт (`MESSAGING_TRANSPORT=rabbitmq` или `MESSAGING_TRANSPORT=kafka`).
- Перед прогоном состояние таблиц PostgreSQL, очередей RabbitMQ и топиков Kafka сбрасывается командой `make lab-reset`.
- Прогон выполняется командами `make lab-rabbit` и `make lab-kafka` (модуль `eventflow.lab.runner`), а сырые результаты записываются в `docs/lab/results/*.json`.

## Матрица сценариев

| # | Сценарий | RabbitMQ | Kafka | Что измеряем |
|---|----------|----------|-------|--------------|
| E1 | Публикация пачки событий из outbox | publisher confirms | producer `acks=all` | латентность пачки, число ошибок |
| E2 | Защита от дублей в outbox | `UNIQUE (event_id)` | `UNIQUE (event_id)` | блокировка повторной вставки того же `event_id` |
| E3 | Повторная доставка консьюмеру | redelivery | повторное чтение | пропуск дубликата через `processed_events` |
| E4 | Публикация проблемного сообщения | routing → очередь / DLX | запись в топик / DLT | время приёма брокером |
| E5 | Параллельные воркеры outbox | 3 воркера (`SKIP LOCKED`) | 3 воркера (`SKIP LOCKED`) | распределение пачки и отсутствие дублей |
| E6 | Упорядочивание сообщений | FIFO в рамках очереди | `key=aggregate_id` → 1 партиция | число задействованных партиций для одного заказа |
| E7 | Повторное чтение истории (replay) | только через перепубликацию | чтение с `earliest` без коммита | время чтения истории из лога |
| E8 | Переподключение сессии брокера | `connect_robust` reconnect | перезапуск продюсера | время переподключения |
| E9 | Пакетная публикация (burst) | цикл `OutboxPublishLoop.tick()` | цикл `OutboxPublishLoop.tick()` | пропускная способность (сообщений/сек) |

## Изоляция транспортов

Не допускается одновременный запуск `outbox-publisher` для RabbitMQ и Kafka на одних и тех же строках `outbox_events`. Каждая запись при создании получает свой `target_transport`, а сравнительные прогоны выполняются последовательно после сброса состояния.

## Результаты измерений

<!-- LAB_RESULTS_TABLE -->
| E | RabbitMQ (факты) | Kafka (факты) |
|---|------------------|---------------|
| E1 | publish_batch_latency_sec=0.04297087498707697, published_count=10, errors=0, p50_sec=0.04297087498707697 | publish_batch_latency_sec=0.027550959013751708, published_count=10, errors=0, p50_sec=0.027550959013751708 |
| E2 | duplicate_insert_blocked=True, rows_expected=1 | duplicate_insert_blocked=True, rows_expected=1 |
| E3 | duplicate_skips=1, business_handler_runs_expected=1 | duplicate_skips=1, business_handler_runs_expected=1 |
| E4 | publish_ok=True, publish_sec=0.0024308330030180514 | publish_ok=True, publish_sec=0.0026815000019269064 |
| E5 | published_total=16, elapsed_sec=0.031339583001681603, per_worker=[16, 0, 0] | published_total=16, elapsed_sec=0.03122625000833068, per_worker=[16, 0, 0] |
| E6 | note=single queue FIFO per consumer | distinct_partitions=1, partitions=[1] |
| E7 | note=Rabbit: redelivery + DLQ replay manual | messages_read=3, read_elapsed_sec=0.0020831250003539026 |
| E8 | reconnect_total_sec=0.028610667010070756, second_session_ok=True | reconnect_total_sec=0.007975624990649521, second_session_ok=True |
| E9 | elapsed_sec=0.08746620800229721, throughput_msg_per_sec=342.98960347305876 | elapsed_sec=0.04588833400339354, throughput_msg_per_sec=653.7609318695562 |
<!-- /LAB_RESULTS_TABLE -->
