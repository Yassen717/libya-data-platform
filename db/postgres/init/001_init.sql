-- Metadata store: ingestion lineage + Airflow metastore.
-- Mounted to /docker-entrypoint-initdb.d and run once at first boot.

-- Airflow metastore database (same credentials as the app user).
CREATE DATABASE airflow;

-- Every producer collection pass is recorded here. This is the pipeline's
-- lineage ledger: what ran, when, how many events it emitted, and failures.
CREATE TABLE IF NOT EXISTS pipeline_runs
(
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed')),
    events_emitted  INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS pipeline_runs_source_started
    ON pipeline_runs (source, started_at DESC);

-- Registered upstream sources; producers upsert on first run.
CREATE TABLE IF NOT EXISTS sources
(
    source      TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,           -- api | scraper | dataset
    base_url    TEXT,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMPTZ
);
