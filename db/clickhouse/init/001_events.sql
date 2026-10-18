-- Analytics store for canonical platform events.
-- Mounted to /docker-entrypoint-initdb.d and run once at first boot.

CREATE DATABASE IF NOT EXISTS libya;

CREATE TABLE IF NOT EXISTS libya.events
(
    event_id    String,
    source      LowCardinality(String),
    source_id   String DEFAULT '',
    event_time  DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    title       String,
    url         String DEFAULT '',
    country     LowCardinality(String) DEFAULT 'LY',
    category    LowCardinality(String) DEFAULT 'other',
    actors      Array(String) DEFAULT [],
    lat         Nullable(Float64),
    lon         Nullable(Float64),
    tone        Nullable(Float32),
    metrics_json String DEFAULT '{}',
    raw_json    String DEFAULT '{}'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (source, event_time, event_id)
TTL event_time + INTERVAL 2 YEAR;

-- Mirror of the Kafka DLQ for inspecting poisoned records.
CREATE TABLE IF NOT EXISTS libya.dead_events
(
    received_at  DateTime64(3, 'UTC'),
    error        String,
    payload      String
)
ENGINE = MergeTree
ORDER BY received_at
TTL received_at + INTERVAL 30 DAY;

-- Per-source/category daily rollup used by dashboards and /stats.
CREATE MATERIALIZED VIEW IF NOT EXISTS libya.events_daily
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (source, category, day)
AS SELECT
    toDate(event_time) AS day,
    source,
    category,
    count()           AS events,
    avgOrNull(tone)   AS avg_tone
FROM libya.events
GROUP BY day, source, category;
