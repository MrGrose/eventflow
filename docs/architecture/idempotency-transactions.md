# Транзакции и `run_idempotent`

## Production call sites

| Путь | Сессия | Commit |
|------|--------|--------|
| `workers/rabbitmq_worker.py` | новая `AsyncSession` на сообщение | внутри `run_idempotent` при чистой сессии |
| `workers/kafka_worker.py` | то же | то же |
| `application/consumers/dispatch.py` | переданная сессия | делегирует в `run_idempotent` |

Воркеры **не** выполняют SQL до `dispatch_consumer_event` → режим «чистая сессия».

## Контракт `run_idempotent`

**Чистая сессия** (нет `in_transaction()`, нет dirty/new/deleted):

- dedupe + handler в одной транзакции;
- успех → `commit`;
- дубликат → `rollback`;
- ошибка handler → `rollback`.

**Внешняя транзакция** (уже был `execute`/`flush` или ORM-изменения):

- при необходимости `flush()` внешних изменений;
- dedupe + handler в `begin_nested()` (SAVEPOINT);
- внешний `commit` — ответственность вызывающего;
- дубликат/ошибка откатывают только savepoint, не внешнюю транзакцию.

## Autobegin

После `SELECT` / любого `execute` сессия в транзакции → внешний режим. Без внешнего `commit` при закрытии сессии обработка не сохранится.
