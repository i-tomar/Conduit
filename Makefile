UV ?= uv
export PATH := $(HOME)/.local/bin:$(PATH)

.PHONY: install test lint format run bench compose ci

install:
	$(UV) sync --all-groups

test:
	$(UV) run pytest --cov=conduit --cov-report=term-missing

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

run:
	CONDUIT_REDIS_URL=memory:// \
	CONDUIT_DEV_API_KEY=cdt_live_dev_local_key \
	CONDUIT_ADMIN_TOKEN=dev-admin-token \
	CONDUIT_ALLOW_FAULT_INJECTION=true \
	$(UV) run uvicorn conduit.main:app --host 0.0.0.0 --port 8091 --reload

bench:
	$(UV) run python scripts/benchmark.py

compose:
	docker compose up --build

ci: lint test
