# ADR-0001: Redpanda over Kafka

Status: Accepted
Date: 2026-10-18

## Context

The platform needs a durable, replayable log between producers and the
stream processor (`ly.events.raw`, `ly.events.dlq`). The ecosystem skill
that matters here is the Kafka protocol — `kafka-python-ng` on the
producer side, `rdkafka` in the consumer — so any broker we pick must be
wire-compatible, not "Kafka-like".

Constraints: the dev stack runs on a laptop alongside Postgres,
ClickHouse, Grafana and (optionally) Airflow; a broker that wants a
controller quorum and several GB of heap is the wrong default.

## Decision

Run **Redpanda** (`v25.2.7`) in `--mode=dev-container`: a single
self-contained binary with `--smp=1 --memory=1G`, Kafka listeners split
internal (`redpanda:9092`) / external (`localhost:19092`), and no
ZooKeeper or KRaft ceremony for a one-node cluster.

PandaProxy (8082) and Schema Registry (8081) listeners stay enabled even
though nothing consumes them yet — they are the documented migration
path for ADR-0003, and the marginal cost is two bound ports.

Observability comes free: the admin port exposes `/public_metrics`
(9644), a bounded-cardinality Prometheus surface scraped by
`infra/prometheus/prometheus.yml`. Topic/message inspection is via
Redpanda Console rather than a third-party UI.

## Consequences

- One container, ~1 GB cap, starts in seconds — the compose stack stays
  laptop-friendly where a real Kafka cluster would dominate it.
- Producer/consumer code is unchanged Kafka client code; if we ever swap
  to managed Kafka (MSK, Confluent) nothing in `services/` changes.
- `dev-container` mode trades away production tuning (no persistence or
  replication guarantees worth reasoning about). Accepted for dev — the
  topics contract (`contracts/topics.md`) already states the production
  equivalent is explicit `rpk topic create`.
- Redpanda-specific surfaces (admin API, `/public_metrics`, console) are
  vendor features; anything we build on them is not portable to Apache
  Kafka. So far that list is: metrics scraping and the dev UI. Fine.
- Alternatives rejected: Apache Kafka/KRaft (JVM, heavier, more moving
  parts for zero dev benefit), RabbitMQ/NATS (wrong protocol — loses the
  Kafka ecosystem story that is half the point), managed Kafka (local
  dev must be offline-capable and free).
