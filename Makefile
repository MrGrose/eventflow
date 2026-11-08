.PHONY: sync lint test unit compose-config compose-up compose-down lab-reset lab-rabbit lab-kafka

sync:
	uv sync --all-extras

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

unit:
	uv run pytest -q tests/unit

test: unit

compose-config:
	docker compose config --quiet

compose-up:
	docker compose up -d postgres rabbitmq kafka

compose-down:
	docker compose down

lab-reset:
	uv run python -m eventflow.lab.reset

lab-rabbit:
	MESSAGING_TRANSPORT=rabbitmq uv run python -m eventflow.lab.runner --transport rabbitmq

lab-kafka:
	MESSAGING_TRANSPORT=kafka uv run python -m eventflow.lab.runner --transport kafka

lab: lab-reset lab-rabbit lab-kafka
