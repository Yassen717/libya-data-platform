# ADR-0003: JSON contract enforced at the consumer, with a DLQ

Status: Accepted
Date: 2026-10-18

## Context

Every producer emits onto `ly.events.raw`. Without a contract, the
topic degrades into whatever shape the last producer found convenient.

The "correct" industry answer is Schema Registry plus Avro/Protobuf:
schemas are enforced on the wire, compatibility modes are checked by the
registry, and payloads are compact. The cost is a registry service in
the loop, binary payloads you cannot eyeball in a console, and codegen
or registry client plumbing in every producer.

## Decision

The contract is **a JSON Schema checked into the repo**
(`contracts/event.schema.json`), enforced by the stream processor at
consume time via serde types. Violating records are not dropped: they
are forwarded to `ly.events.dlq` with the original bytes plus an `error`
field, so they can be triaged and replayed after a fix. Message keys are
`event_id` for per-source ordering.

Schema evolution is by convention: additive fields with defaults only;
producers MUST NOT repurpose or remove fields without a contract PR.

## Consequences

- The contract is diffable in code review, readable in Redpanda Console,
  and requires zero extra services at runtime. The whole enforcement
  story is ~50 lines of serde in the consumer.
- Enforcement is one consumer deep, not wire-level: a rogue producer can
  still push garbage to `ly.events.raw`. The DLQ is the mitigation —
  poisoned records are quarantined, visible, and replayable rather than
  silently dropped or crashing the pipeline.
- JSON is verbose on the wire (no field-number compaction). At
  current volume — hundreds of events per poll, not per second — that
  is the right trade.
- No registry means no enforced compatibility modes; the convention
  (additive-with-defaults) is enforced by review, not tooling. If the
  producer count grows this becomes the weakest part of the design.
- Migration path is preserved, not foreclosed: the Redpanda container
  already listens on 8081 for Schema Registry (see ADR-0001), so
  adopting registry-enforced Avro later is a producer/consumer change,
  not an infrastructure one. Scheduled as a roadmap item (M4).
