//! Kafka wiring: the `ly.events.raw` consumer, the DLQ producer, and
//! manual offset commits.

use crate::config::Config;
use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::time::Duration;
use tracing::debug;

/// Source-topic consumer. Auto-commit and auto-offset-store are both off:
/// offsets are committed explicitly only after a confirmed ClickHouse
/// insert (or DLQ delivery), giving at-least-once semantics.
pub fn consumer(cfg: &Config) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", &cfg.consumer_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .create()
        .context("failed to create Kafka consumer")?;
    consumer
        .subscribe(&[cfg.topic_events.as_str()])
        .with_context(|| format!("failed to subscribe to {}", cfg.topic_events))?;
    Ok(consumer)
}

/// Producer used exclusively for dead-lettering invalid records.
pub fn producer(cfg: &Config) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "15000")
        .create()
        .context("failed to create Kafka producer")
}

/// Deliver one record to the DLQ and wait for the broker ack.
/// `payload` must already carry the `error` field (see `schema::dlq_payload`).
/// `Duration::ZERO` fails fast if the producer queue is full; delivery
/// itself is bounded by `message.timeout.ms` and the caller's timeout.
pub async fn send_to_dlq(
    producer: &FutureProducer,
    topic: &str,
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
) -> Result<()> {
    let mut record = FutureRecord::to(topic).payload(&payload);
    if let Some(k) = &key {
        record = record.key(k.as_slice());
    }
    let delivery = producer
        .send(record, Duration::ZERO)
        .await
        .map_err(|(err, _)| err)
        .context("DLQ produce failed")?;
    debug!(
        partition = delivery.partition,
        offset = delivery.offset,
        "DLQ delivery confirmed"
    );
    Ok(())
}

/// Commit `(partition, next_offset)` pairs. Errors are returned to the
/// caller; a failed commit is safe — the records are simply replayed.
pub fn commit_offsets(
    consumer: &StreamConsumer,
    topic: &str,
    committable: &[(i32, i64)],
    mode: CommitMode,
) -> Result<()> {
    if committable.is_empty() {
        return Ok(());
    }
    let map: HashMap<(String, i32), Offset> = committable
        .iter()
        .map(|&(partition, next)| ((topic.to_string(), partition), Offset::Offset(next)))
        .collect();
    let tpl = TopicPartitionList::from_topic_map(&map).context("failed to build offset list")?;
    consumer
        .commit(&tpl, mode)
        .with_context(|| format!("offset commit failed for {topic}"))?;
    debug!(offsets = ?committable, "committed offsets");
    Ok(())
}
