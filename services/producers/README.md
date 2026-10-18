# ly-ingest — ingestion producers

Python package `ly_ingest` that polls public upstream sources about Libya and
emits canonical events (see `contracts/event.schema.json`) to Kafka topic
`ly.events.raw`, keyed by `event_id`. Each collection pass is logged to
Postgres `pipeline_runs` for lineage.

## Producers

| Module                | Upstream                            | Default cadence |
| --------------------- | ----------------------------------- | --------------- |
| `ly_ingest.gdelt`     | GDELT DOC 2 API (`artlist`)         | 300 s           |
| `ly_ingest.worldbank` | World Bank indicators for LBY       | 86400 s         |

Both expose `run_once() -> int` (one pass, returns events emitted) and
`main()` (poll loop; `--once` exits after a single pass). The same module is
what the Airflow DAGs call.

## Environment

| Variable                 | Default                                              | Used by    |
| ------------------------ | ---------------------------------------------------- | ---------- |
| `KAFKA_BROKERS`          | `localhost:9092`                                     | both       |
| `TOPIC_EVENTS`           | `ly.events.raw`                                      | both       |
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | `localhost/5432/libya/ly/""`            | lineage    |
| `POLL_INTERVAL_SECONDS`  | `300` (gdelt) / `86400` (worldbank)                  | main loop  |
| `GDELT_QUERY`            | `libya`                                              | gdelt      |
| `GDELT_MAX_RECORDS`      | `250`                                                | gdelt      |
| `WORLDBANK_INDICATORS`   | `NY.GDP.PETR.RT.ZS,NY.GDP.MKTP.CD,SP.POP.TOTL`       | worldbank  |
| `LOG_LEVEL`              | `INFO`                                               | both       |

When Postgres is unreachable the reporter degrades to a no-op (warning only) —
producers still emit to Kafka.

## Development

```bat
python -m venv .venv
.venv\Scripts\pip install -e .[dev]
.venv\Scripts\python -m pytest -q
```

Single pass (requires reachable Kafka; Postgres optional):

```bat
.venv\Scripts\python -m ly_ingest.gdelt --once
```

In docker-compose this image backs `producer-gdelt` and
`producer-worldbank`; each overrides `command` to select the module.
