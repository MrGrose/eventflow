# ADR-002 Addendum: outbox failure matrix

| Сценарий | Поведение | Гарантия |
|----------|-----------|----------|
| A Успешная публикация | claim → broker ack → `published` | событие не теряется |
| B Crash после broker, до `published` | строка `publishing` до lease; recovery → retry | at-least-once, возможен дубль |
| C Lease истёк | `recover_stale_leases` → `pending` | повторная доставка |
| D Два publisher | `SKIP LOCKED` без пересечения claim | нет двойного in-flight claim |
| E Transport guard | claim только `target_transport == MESSAGING_TRANSPORT` | нет публикации в чужой transport |
| Broker down | `mark_publish_failed`, backoff `next_attempt_at` | событие остаётся в outbox |

`mark_published` / `mark_publish_failed` принимают только текущего `lock_owner`.
