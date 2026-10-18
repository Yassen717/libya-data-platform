# stream-processor

`ly-processor` — Rust consumer that validates `ly.events.raw` against
`contracts/event.schema.json`, batches valid rows into ClickHouse
`libya.events` over HTTP (`JSONEachRow`), and routes invalid payloads to
`ly.events.dlq` with an added `error` field.

## Configuration (env)

| Variable              | Default                   | Notes                                   |
| --------------------- | ------------------------- | --------------------------------------- |
| `KAFKA_BROKERS`       | *(required)*              | e.g. `redpanda:9092` / `localhost:19092` |
| `TOPIC_EVENTS`        | `ly.events.raw`           | source topic                            |
| `TOPIC_DLQ`           | `ly.events.dlq`           | dead-letter topic                       |
| `CONSUMER_GROUP`      | `ly-processor`            | Kafka consumer group                    |
| `CLICKHOUSE_URL`      | `http://localhost:8123`   | HTTP interface base                     |
| `CLICKHOUSE_DB`       | `libya`                   | database holding `events`               |
| `CLICKHOUSE_USER`     | `default`                 | basic-auth user                         |
| `CLICKHOUSE_PASSWORD` | *(empty)*                 | basic-auth password                     |
| `BATCH_MAX_MESSAGES`  | `500`                     | flush size cap                          |
| `BATCH_FLUSH_MS`      | `2000`                    | flush deadline from first buffered row  |
| `RUST_LOG`            | `info`                    | `tracing` filter                        |

## Run

```sh
KAFKA_BROKERS=localhost:19092 CLICKHOUSE_URL=http://localhost:8123 \
  CLICKHOUSE_USER=ly CLICKHOUSE_PASSWORD=devpass cargo run
```

Or in the stack: `docker compose up -d processor` (env is wired in
`docker-compose.yml`).

## Batching / commit protocol

1. Each message deserializes into `PlatformEvent`; failures go to the DLQ
   (original payload + `error`) and the consumer never stalls on them.
2. Valid rows buffer until `BATCH_MAX_MESSAGES` rows or `BATCH_FLUSH_MS`
   since the first buffered row, then `POST .../INSERT INTO {db}.events
   FORMAT JSONEachRow` with basic auth; retries: 5 attempts, exponential
   backoff.
3. Kafka offsets are committed **only after a confirmed insert** (or a
   confirmed DLQ delivery), and never past an unflushed batch row —
   at-least-once; dedupe by `event_id` is a documented downstream concern.
4. SIGTERM/SIGINT stops consuming, flushes the partial batch, commits, and
   exits 0; if the final insert fails, those offsets stay uncommitted and
   replay on restart.

## Tests

`cargo test` — pure unit tests (schema mapping, rejection cases, DLQ
payload shape, batcher flush triggers, offset watermark, JSONEachRow
encoding, retry exhaustion). No Kafka/ClickHouse needed.
