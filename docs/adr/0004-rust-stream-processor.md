# ADR-0004: Rust for the stream processor

Status: Accepted
Date: 2026-10-18

## Context

The stream processor is the platform's hot path: consume
`ly.events.raw`, validate every record against the event contract
(ADR-0003), batch valid rows, insert into ClickHouse, and route
violations to the DLQ — forever, unattended, on shared hardware.

Python is the right language for the producers (messy upstream APIs,
fast iteration). For the always-on consumer it is the wrong default:
per-message throughput is mediocre, the process footprint is large for
what is mostly I/O shuttling, and backpressure semantics get fuzzy under
a GC'd runtime.

## Decision

The consumer is a **Rust** binary (`ly-processor`): `rdkafka` on
consumer group `ly-processor`, serde validation against the contract,
and batched inserts over HTTP —
`POST {CLICKHOUSE_URL}/?query=INSERT INTO libya.events FORMAT JSONEachRow`
— flushed at `BATCH_MAX_MESSAGES` (500) or `BATCH_FLUSH_MS` (2000),
whichever comes first. Offsets commit only after ClickHouse confirms
the insert; SIGTERM flushes the pending batch before exit.

## Consequences

- Predictable footprint: single-digit-MB RSS, no GC pauses, sized to sit
  next to four other services on a laptop without being the thing that
  eats memory.
- Batching is the real feature, not the language flex: MergeTree wants
  few large parts, and a 500-message/2 s flush turns a chatty event
  stream into chunky inserts ClickHouse digests efficiently.
- Failure semantics are honest: no offset commit until the insert lands,
  so a ClickHouse outage backs up the consumer (Kafka retains) rather
  than losing data — retry is bounded by `BATCH_FLUSH_MS`.
- HTTP inserts over the native protocol: the `clickhouse` Rust crate
  speaks HTTP anyway — one dependency, TLS-ready, fast enough. If it
  ever bottlenecks, the fix is a bigger batch, not a new protocol.
- Costs accepted: slower iteration than Python (deliberate — this
  service should be boring and rarely touched), a C toolchain dependency
  via `librdkafka` in builds, and a smaller hiring/pool familiarity.
  Rust here is also a stated portfolio differentiator: the point is
  demonstrating a real batched consumer, not a `for` loop over
  `consumer.poll`.
- Alternatives rejected: Python consumer (reuses `ly_ingest` patterns
  but weak batching/footprint story for the hot path), Go (a fair
  alternative — lost on explicit resource control and the
  differentiator argument), Faust/Bytewax (framework overhead for one
  consume-transform-insert loop).
