# ly-api — Libya Data Platform serving API

FastAPI read layer over ClickHouse (`libya.events`, `libya.events_daily`)
and Postgres (`pipeline_runs`, `sources`). Uvicorn on port 8000.

## Endpoints

| Method | Path              | Description                                              |
| ------ | ----------------- | -------------------------------------------------------- |
| GET    | `/`               | Service name, version, link list                         |
| GET    | `/health`         | Liveness + ClickHouse `SELECT 1` ping (503 if CH is down)|
| GET    | `/events`         | Recent events. Params: `source`, `category`, `from`, `to`, `limit` (≤1000, default 100), `offset` |
| GET    | `/stats/summary`  | Per-source/category counts + avg tone, last 30 days. Reads `libya.events_daily`, falls back to `libya.events` when the MV is empty |
| GET    | `/indicators`     | Indicator series from events with `metrics_json != '{}'`. Params: `indicator`, `limit` |
| GET    | `/runs`           | Latest 50 `pipeline_runs` joined with `sources` (Postgres lineage) |
| GET    | `/metrics`        | Prometheus metrics (prometheus-fastapi-instrumentator)   |

Interactive OpenAPI docs at `/docs`.

## Configuration

| Env var              | Default                 | Purpose                |
| -------------------- | ----------------------- | ---------------------- |
| `CLICKHOUSE_URL`     | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `CLICKHOUSE_DB`      | `libya`                 | ClickHouse database    |
| `CLICKHOUSE_USER`    | `ly`                    | ClickHouse user        |
| `CLICKHOUSE_PASSWORD`| `devpass`               | ClickHouse password    |
| `POSTGRES_HOST`      | `localhost`             | Postgres host          |
| `POSTGRES_PORT`      | `5432`                  | Postgres port          |
| `POSTGRES_DB`        | `libya`                 | Postgres database      |
| `POSTGRES_USER`      | `ly`                    | Postgres user          |
| `POSTGRES_PASSWORD`  | `devpass`               | Postgres password      |

All user-supplied query values are bound via ClickHouse `{name:Type}`
parameters — nothing is interpolated into SQL.

## Develop

```bat
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"
.venv\Scripts\uvicorn app.main:app --reload
.venv\Scripts\python -m pytest -q
```

Or via docker compose from the repo root: `docker compose up -d api`.
