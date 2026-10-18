# ADR-0002: ClickHouse for analytics

Status: Accepted
Date: 2026-10-18

## Context

`libya.events` is append-mostly, time-ordered data queried almost
exclusively by scans: counts per hour/source, average tone per day,
category breakdowns, indicator series. Postgres is already in the stack
— it would be the zero-new-dependency answer, and it is the right tool
for the `pipeline_runs` ledger, which is small, relational, and updated
row-by-row.

The question is whether event analytics belongs in the same store.

## Decision

Keep Postgres for metadata; store events in **ClickHouse**
(`MergeTree`, `PARTITION BY toYYYYMM(event_time)`,
`ORDER BY (source, event_time, event_id)`, 2-year TTL). Schema and
rollups live in `db/clickhouse/init/001_events.sql`, including a
`SummingMergeTree` materialized view (`events_daily`) that pre-aggregates
per source/category/day for dashboards and `/stats`.

Column choices matter more than the engine choice: `LowCardinality` for
`source`/`category`/`country`, nullable `tone`, and `metrics_json` /
`raw_json` string columns that keep the lossless payload without an
EFTed schema per source.

The processor inserts via HTTP `JSONEachRow` in batches (see ADR-0004),
which is the ingest pattern MergeTree wants: few large parts, not many
small ones.

## Consequences

- Aggregations that would seq-scan a row store (group-by over months of
  events) run interactively — this is what makes the Grafana dashboard
  and `/stats/summary` cheap to serve.
- ClickHouse's built-in Prometheus endpoint (port 8001, enabled by
  `db/clickhouse/config/prometheus.xml`) gives us storage-layer metrics
  with no exporter sidecar.
- Cost: a second database system to operate — its own upgrade cadence,
  backup story, and failure modes. Accepted deliberately; operating a
  columnar store against real event volume is part of the point of the
  project.
- ClickHouse is weak at updates and deletes (mutations are heavyweight
  rewrites). That fits: events are immutable, and dedupe is pushed to
  read-time `argMax` queries with `ReplacingMergeTree` as a roadmap item
  rather than fighting the engine.
- Materialized views are insert-time, eventually-consistent rollups —
  acceptable for dashboards, wrong for anything needing transactional
  freshness. The `pipeline_runs` ledger stays in Postgres for exactly
  that reason.
- Alternatives rejected: plain Postgres (aggregation cost grows
  linearly with retention; becomes the bottleneck it was meant to
  avoid), TimescaleDB (continuous aggregates are good, but hypertables
  still carry row-store overheads and add an extension dependency),
  DuckDB (embedded — no concurrent-writer service model), Elasticsearch
  (heavier cluster, inverted-index model tuned for search not analytics).
