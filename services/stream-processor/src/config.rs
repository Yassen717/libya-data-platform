//! Runtime configuration, built entirely from environment variables.

use anyhow::{Context, Result};
use std::time::Duration;

pub struct Config {
    /// e.g. `redpanda:9092` (required, no default).
    pub kafka_brokers: String,
    /// Source topic, default `ly.events.raw`.
    pub topic_events: String,
    /// Dead-letter topic, default `ly.events.dlq`.
    pub topic_dlq: String,
    /// Consumer group, default `ly-processor`.
    pub consumer_group: String,
    /// ClickHouse HTTP base URL, default `http://localhost:8123`.
    pub clickhouse_url: String,
    /// ClickHouse database holding `events`, default `libya`.
    pub clickhouse_db: String,
    /// Basic-auth user, default `default` (ClickHouse built-in).
    pub clickhouse_user: String,
    /// Basic-auth password, default empty.
    pub clickhouse_password: String,
    /// Rows per insert, default 500.
    pub batch_max_messages: usize,
    /// Max time a buffered row may wait before a flush, default 2000ms.
    pub batch_flush: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// Injectable lookup so defaults and parsing can be unit-tested without
    /// touching process env.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let or = |key: &str, default: &str| {
            get(key)
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default.to_string())
        };

        let kafka_brokers = get("KAFKA_BROKERS")
            .filter(|v| !v.is_empty())
            .context("KAFKA_BROKERS is required (e.g. KAFKA_BROKERS=localhost:19092)")?;

        let batch_max_messages = get("BATCH_MAX_MESSAGES")
            .map(|v| {
                v.parse::<usize>()
                    .context("BATCH_MAX_MESSAGES must be a positive integer")
            })
            .transpose()?
            .filter(|&n| n > 0)
            .unwrap_or(500);

        let batch_flush_ms = get("BATCH_FLUSH_MS")
            .map(|v| {
                v.parse::<u64>()
                    .context("BATCH_FLUSH_MS must be milliseconds (integer)")
            })
            .transpose()?
            .unwrap_or(2000);

        Ok(Self {
            kafka_brokers,
            topic_events: or("TOPIC_EVENTS", "ly.events.raw"),
            topic_dlq: or("TOPIC_DLQ", "ly.events.dlq"),
            consumer_group: or("CONSUMER_GROUP", "ly-processor"),
            clickhouse_url: or("CLICKHOUSE_URL", "http://localhost:8123"),
            clickhouse_db: or("CLICKHOUSE_DB", "libya"),
            clickhouse_user: or("CLICKHOUSE_USER", "default"),
            clickhouse_password: get("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            batch_max_messages,
            batch_flush: Duration::from_millis(batch_flush_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn defaults_match_contract() {
        let cfg = Config::from_lookup(lookup(&[("KAFKA_BROKERS", "redpanda:9092")])).unwrap();
        assert_eq!(cfg.topic_events, "ly.events.raw");
        assert_eq!(cfg.topic_dlq, "ly.events.dlq");
        assert_eq!(cfg.consumer_group, "ly-processor");
        assert_eq!(cfg.clickhouse_db, "libya");
        assert_eq!(cfg.batch_max_messages, 500);
        assert_eq!(cfg.batch_flush, Duration::from_millis(2000));
    }

    #[test]
    fn brokers_required() {
        assert!(Config::from_lookup(lookup(&[])).is_err());
    }

    #[test]
    fn parses_batch_overrides() {
        let cfg = Config::from_lookup(lookup(&[
            ("KAFKA_BROKERS", "b:1"),
            ("BATCH_MAX_MESSAGES", "50"),
            ("BATCH_FLUSH_MS", "250"),
        ]))
        .unwrap();
        assert_eq!(cfg.batch_max_messages, 50);
        assert_eq!(cfg.batch_flush, Duration::from_millis(250));
    }

    #[test]
    fn rejects_bad_batch_values() {
        assert!(Config::from_lookup(lookup(&[
            ("KAFKA_BROKERS", "b:1"),
            ("BATCH_MAX_MESSAGES", "lots"),
        ]))
        .is_err());
    }
}
