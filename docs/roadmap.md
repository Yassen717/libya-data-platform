# Roadmap

Milestones from scaffold to a deployed, public data platform. Each
milestone leaves the repo runnable — no dead-end states.

## M0 — Repo scaffold & contracts ✅

- `docker-compose.yml` wiring the full stack: Redpanda + Console,
  ClickHouse, Postgres, producers, processor, API, Prometheus, Grafana.
- `contracts/` — canonical event schema, topic conventions, service
  contracts. Everything else builds against these.
- Init DDL for both databases (`db/clickhouse/init`, `db/postgres/init`).
- Docs skeleton: `docs/architecture.md`, ADRs (`docs/adr/`), this
  roadmap.

## M1 — Core pipeline, end-to-end local

- `producer-gdelt` and `producer-worldbank` emitting real events to
  `ly.events.raw`, each pass recorded in `pipeline_runs`.
- `ly-processor` validating, batching and inserting into
  `libya.events`; schema violations visible in `ly.events.dlq` and
  `libya.dead_events`.
- API serving `/events`, `/stats/summary`, `/indicators`, `/runs`
  against live data.
- Observability wired: Prometheus scraping redpanda/clickhouse/api,
  Grafana provisioned with both datasources and the Pipeline Health
  dashboard.
- Done means: `docker compose up -d` → events flow → dashboard shows
  non-empty panels within one poll interval.

## M2 — Batch orchestration & scraper producer

- Airflow `orchestration` profile running DAGs that call
  `ly_ingest.*.run_once()` on schedule (World Bank cadence, GDELT
  backfills) with retries and `pipeline_runs` lineage.
- `ly_ingest.scraper` producer — the HTTP engine from the author's
  [Scrap-app](https://github.com/Yassen717/Scrap-app) ported into
  `ly_ingest.common` (retry/backoff session, size caps, polite
  rate-limiting), plus at least one real target emitting
  `source: "scraper"` events.
- Processor unit tests covering contract edge cases and DLQ routing.

## M3 — Public dashboard

- Rewire the
  [Libya-natural-resources](https://github.com/Yassen717/Libya-natural-resources)
  frontend onto the platform API (`/events`, `/stats/summary`,
  `/indicators`) instead of static data.
- Category and source facets, tone-over-time view, freshness indicator
  fed from `/runs`.
- Read-only public surface — no mutation endpoints exposed.

## M4 — Deploy to a VPS

- Terraform for the host (VPS + firewall + DNS), one-command
  environment bring-up.
- CI/CD: GitHub Actions — lint, tests, image builds, deploy on merge to
  `main`.
- HTTPS via reverse proxy (Caddy or nginx + Let's Encrypt); Grafana and
  API behind it.
- Schema registry: migrate event serialization from consumer-side JSON
  validation to the Redpanda registry already listening on 8081
  (see ADR-0003) — Avro or Protobuf with compatibility enforced.
- Secrets out of `.env` into a real store; ClickHouse + Postgres backup
  and restore runbook.

## M5 — Kubernetes & scale-out

- K8s manifests (or Helm chart) for the stack; namespace-per-env layout.
- Scale notes: processor replicas keyed on consumer lag (KEDA or HPA on
  a lag exporter), ClickHouse volume sizing, Redpanda multi-broker
  topology.
- Evaluate operators vs raw manifests (Strimzi for Kafka, Altinity
  ClickHouse operator) — documented as an ADR whichever way it lands.

---

### Contributing upstream

This platform stands on open-source projects — Apache Airflow most of
all. Where the work surfaces a real upstream bug, doc gap or papercut
(not a local workaround), the fix goes back as a PR to the project —
small, scoped, and linked from the commit that needed it. Contributions
to date live in the PR history; this note is the standing policy.
