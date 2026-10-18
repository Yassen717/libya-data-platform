.PHONY: up up-all down logs ps test lint

up:            ## core stack
	docker compose up -d

up-all:        ## core stack + Airflow orchestration profile
	docker compose --profile orchestration up -d

down:
	docker compose --profile orchestration down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

test:          ## all unit tests (no docker required)
	cd services/producers && python -m pytest
	cd services/api && python -m pytest
	cd services/stream-processor && cargo test

lint:
	cd services/producers && ruff check .
	cd services/api && ruff check .
	cd services/stream-processor && cargo clippy -- -D warnings
