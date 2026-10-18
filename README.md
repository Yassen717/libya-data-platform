# Libya Data Platform

An open, real-time data platform for Libyan public data — news events,
economic indicators and primary sources — built on a streaming pipeline:

**Producers (Python) → Redpanda (Kafka) → Stream processor (Rust) →
ClickHouse → FastAPI / Grafana**, orchestrated by Airflow, observed by
Prometheus.

> Status: early build — the platform is under active development.
> See [`docs/roadmap.md`](docs/roadmap.md).

## Why this exists

Libyan public data is scattered across portals, PDFs and APIs. This
project ingests it into one canonical event stream
([`contracts/event.schema.json`](contracts/event.schema.json)), stores it
for analysis, and serves it — the data backbone behind the
[Libya Natural Resources](https://github.com/Yassen717/Libya-natural-resources)
dashboard.

## Quickstart

```bash
cp .env.example .env          # optional — defaults work out of the box
docker compose up -d          # core stack
docker compose --profile orchestration up -d   # + Airflow
```

Then open:

| Service | URL |
| --- | --- |
| API docs | <http://localhost:8000/docs> |
| Grafana (admin/admin) | <http://localhost:3100> |
| Redpanda Console | <http://localhost:8080> |
| Prometheus | <http://localhost:9090> |
| Airflow (admin/admin) | <http://localhost:8085> |

## Repository layout

```
contracts/    event schema, topic and service contracts — read first
services/     producers (Python), stream-processor (Rust), api (FastAPI)
db/           ClickHouse + Postgres init DDL
dags/         Airflow DAGs (orchestration profile)
infra/        Prometheus config, Grafana provisioning, Airflow image
docs/         architecture, ADRs, roadmap
```

## Documentation

- [Architecture & design decisions](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [ADRs](docs/adr/)

## License

[MIT](LICENSE)
