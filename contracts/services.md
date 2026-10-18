# Service contracts

Fixed integration surface every service builds against. Do not deviate
without updating this file, `docker-compose.yml` and `docs/architecture.md`.

## ly_ingest (services/producers)

Python package (`pip install .`), import namespace `ly_ingest`.

- `ly_ingest.gdelt` — polls GDELT DOC 2 API for Libya-tagged articles.
- `ly_ingest.worldbank` — pulls configured World Bank indicators for LBY.
- Each module exposes:
  - `run_once() -> int` — one collection pass; returns events emitted.
  - `main()` — loop forever every `POLL_INTERVAL_SECONDS`; `--once` flag
    runs a single pass then exits (used by Airflow DAGs and tests).
- Shared helpers live in `ly_ingest.common`:
  - HTTP session with retry/backoff and size caps (pattern ported from
    the Scrap-app project).
  - `KafkaPublisher` — `kafka-python-ng` producer, JSON values keyed by
    `event_id`, `acks=all`, idempotent send.
  - `RunReporter` — records each collection pass in Postgres
    `pipeline_runs` (see db/postgres/init/001_init.sql).

## stream-processor (services/stream-processor)

Rust binary `ly-processor`.

- Consumes `TOPIC_EVENTS` with consumer group `ly-processor`.
- Validates each message against the event contract (serde types).
- Batches valid rows and inserts into ClickHouse via HTTP:
  `POST {CLICKHOUSE_URL}/?query=INSERT INTO libya.events FORMAT JSONEachRow`.
- Invalid records are forwarded to `TOPIC_DLQ` with an added `error` field.
- Config via env: `KAFKA_BROKERS`, `TOPIC_EVENTS`, `TOPIC_DLQ`,
  `CONSUMER_GROUP`, `CLICKHOUSE_URL`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`,
  `CLICKHOUSE_PASSWORD`, `BATCH_MAX_MESSAGES` (500),
  `BATCH_FLUSH_MS` (2000), `RUST_LOG`.
- Graceful shutdown on SIGTERM: flush pending batch, commit offsets.

## api (services/api)

FastAPI service, uvicorn on container port 8000.

- `GET /health` — liveness.
- `GET /events?source=&category=&from=&to=&limit=` — recent events.
- `GET /stats/summary` — counts by source/category/day.
- `GET /indicators` — World Bank metric series (events with metrics).
- `GET /runs` — pipeline_runs lineage from Postgres.
- Reads ClickHouse over HTTP (`clickhouse-connect` or httpx) and Postgres
  via `psycopg`/`asyncpg`.

## airflow (infra/airflow + dags/)

`airflow standalone` container (profile `orchestration`) with the
`ly_ingest` package installed — DAGs call `run_once()` via PythonOperator.
DAG files live in `dags/` and are mounted read-only.
