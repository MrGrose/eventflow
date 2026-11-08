# ADR-002: Transactional Outbox для двух транспортов

**Статус:** Accepted

## Контекст

Необходима атомарность сохранения бизнес-состояния заказа и постановки события в очередь на отправку, а также безопасная повторная публикация при сбоях воркера или брокера.

## Схема `outbox_events`

| Поле | Назначение |
|------|------------|
| `id` | Первичный ключ строки |
| `event_id` | Уникальный UUID доменного события (`UNIQUE`) |
| `event_type` | Тип события (`order.created`, `order.confirmed` и др.) |
| `aggregate_type`, `aggregate_id` | Тип и идентификатор агрегата (`order`, `order_id`) |
| `payload` | JSONB-тело события |
| `target_transport` | Целевой транспорт (`rabbitmq` \| `kafka`), фиксируется при вставке |
| `status` | `pending` \| `publishing` \| `published` \| `failed` |
| `attempts` | Число попыток публикации |
| `last_error` | Текст последней ошибки публикации |
| `locked_at`, `lock_owner`, `lease_expires_at` | Поля владения и времени аренды (lease) при захвате строки |
| `next_attempt_at` | Время следующей попытки при экспоненциальном backoff |
| `broker_meta` | JSONB-метаданные подтверждения брокера (`exchange`/`routing_key` или `topic`/`partition`/`offset`) |
| `created_at`, `published_at` | Временные метки создания и подтверждённой публикации |

## Конкурентная выборка и Lease Recovery

- Захват готовых записей выполняется запросом `SELECT ... WHERE status = 'pending' AND target_transport = :transport AND (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED`.
- Захваченные строки переводятся в статус `publishing` с заполнением `lock_owner` и `lease_expires_at` (по умолчанию 30 секунд).
- Перед каждым циклом выборки метод `recover_stale_leases()` возвращает зависшие строки с истёкшим `lease_expires_at` обратно в статус `pending`.

## Критерий подтверждения публикации

| Транспорт | Условие перехода в `published` |
|-----------|--------------------------------|
| RabbitMQ | Получение `publisher confirm` от брокера |
| Kafka | Получение `RecordMetadata` от брокера (`acks=all`) |

## Защита от двойной публикации и пересечения транспортов

- Переход из `publishing` в `published` проверяет совпадение `lock_owner`.
- Воркер выбирает только строки со своим `target_transport`.
- При повторной публикации после истечения lease консьюмер отфильтрует дубликат по неизменному `event_id` в таблице `processed_events`.
