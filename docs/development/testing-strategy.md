# Стратегия тестирования

## Структура каталога `tests/`

```text
tests/
  unit/          # быстрые модульные тесты домена, настроек, API health/metrics и слоёв архитектуры
  integration/   # интеграционные тесты с PostgreSQL и сквозные E2E-сценарии обоих брокеров
  rabbitmq/      # тесты топологии, publisher confirms, retry и DLQ в RabbitMQ (@pytest.mark.rabbitmq)
  kafka/         # тесты создания топиков, партиционирования, offset commit и DLT в Kafka (@pytest.mark.kafka)
  concurrency/   # конкурентные сценарии дедупликации и резервирования остатков (@pytest.mark.concurrency)
  failure/       # тесты откатов транзакций, дубликатов и обработки ошибок (@pytest.mark.failure)
  lab/           # тесты жизненного цикла подключений сравнительной лаборатории (@pytest.mark.lab)
```

## Команды запуска

```bash
# Быстрые модульные тесты (не требуют Docker)
make unit

# Полный набор тестов (требуются запущенные PostgreSQL, RabbitMQ и Kafka)
uv run pytest -q

# Запуск отдельных групп по маркерам
uv run pytest -m rabbitmq -q
uv run pytest -m kafka -q
uv run pytest -m concurrency -q
uv run pytest -m failure -q
```

## Принципы

- Интеграционные и сквозные тесты выполняются на реальных контейнерах PostgreSQL, RabbitMQ и Apache Kafka без подмены транзакций или брокерного протокола моками.
- Перед каждым интеграционным тестом фикстура `_isolate_db` очищает таблицы PostgreSQL (`TRUNCATE ... RESTART IDENTITY CASCADE`), а фикстура `_purge_rabbit_queues` очищает очереди RabbitMQ для полной изоляции прогонов.
