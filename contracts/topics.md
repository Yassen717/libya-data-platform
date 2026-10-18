# Kafka topics

Redpanda auto-creates topics on first produce in this dev stack.
Production equivalent: create explicitly via `rpk topic create`.

| Topic            | Partitions | Retention | Purpose                                        |
| ---------------- | ---------- | --------- | ---------------------------------------------- |
| `ly.events.raw`  | 3          | 7 days    | Canonical platform events (see event.schema.json) |
| `ly.events.dlq`  | 1          | 30 days   | Malformed / schema-violating records for triage |

## Conventions

- **Message key**: `event_id` — gives per-source ordering and enables
  compaction later if we move to upsert semantics.
- **Value**: JSON matching `contracts/event.schema.json`.
- **Consumer group**: `ly-processor` for the Rust stream processor.
- The DLQ holds the *original bytes* plus an `error` field describing the
  validation/insert failure, so bad records can be replayed after a fix.
