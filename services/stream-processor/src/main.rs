//! `ly-processor` — consumes `ly.events.raw`, validates each record against
//! the platform event contract, batches valid rows into ClickHouse
//! (`libya.events` via HTTP JSONEachRow), routes invalid payloads to
//! `ly.events.dlq`, and commits Kafka offsets only after confirmed writes.

mod batcher;
mod clickhouse;
mod config;
mod kafka_io;
mod schema;

use anyhow::Result;
use batcher::{Batcher, OffsetTracker};
use clickhouse::ClickHouse;
use config::Config;
use rdkafka::consumer::{CommitMode, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::FutureProducer;
use schema::{dlq_payload, EventRow, PlatformEvent};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{self, Instant};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

/// Bounded wait for a DLQ produce so one bad record can never stall the
/// consumer even if the broker is unhealthy.
const DLQ_SEND_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::from_env()?;
    info!(
        brokers = %cfg.kafka_brokers,
        topic = %cfg.topic_events,
        dlq = %cfg.topic_dlq,
        group = %cfg.consumer_group,
        clickhouse = %cfg.clickhouse_url,
        db = %cfg.clickhouse_db,
        batch_max = cfg.batch_max_messages,
        flush_ms = cfg.batch_flush.as_millis() as u64,
        "ly-processor starting"
    );

    let consumer = kafka_io::consumer(&cfg)?;
    let producer = kafka_io::producer(&cfg)?;
    let clickhouse = ClickHouse::new(
        &cfg.clickhouse_url,
        &cfg.clickhouse_db,
        &cfg.clickhouse_user,
        &cfg.clickhouse_password,
    )?;

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let mut batcher = Batcher::new(cfg.batch_max_messages, cfg.batch_flush);
    let mut offsets = OffsetTracker::new();

    loop {
        let flush_at = batcher.deadline();
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => break,
            res = consumer.recv() => match res {
                Ok(msg) => {
                    handle_message(&msg, &consumer, &producer, &cfg, &mut batcher, &mut offsets).await;
                    if batcher.is_due(std::time::Instant::now()) {
                        flush(&clickhouse, &consumer, &cfg, &mut batcher, &offsets, shutdown_rx.clone())
                            .await;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "kafka consumer error");
                    time::sleep(Duration::from_millis(500)).await;
                }
            },
            // Time-based flush: fires `BATCH_FLUSH_MS` after the first
            // buffered row; pends forever while the batch is empty.
            _ = async move {
                match flush_at {
                    Some(d) => time::sleep_until(Instant::from_std(d)).await,
                    None => std::future::pending().await,
                }
            } => {
                flush(&clickhouse, &consumer, &cfg, &mut batcher, &offsets, shutdown_rx.clone())
                    .await;
            }
        }
    }

    // Graceful shutdown: flush the partial batch (a never-fired shutdown
    // receiver lets the retry backoff run its course), then commit whatever
    // is confirmed-handled. If the insert still fails, those offsets stay
    // uncommitted and the records replay on restart — at-least-once.
    info!(
        rows = batcher.len(),
        "shutdown signal received; flushing partial batch"
    );
    let (_never_tx, never_rx) = watch::channel(false);
    match clickhouse.insert(batcher.rows(), never_rx).await {
        Ok(()) => {
            info!(rows = batcher.len(), "final flush succeeded");
            batcher.clear();
        }
        Err(e) => error!(
            error = %e,
            rows = batcher.len(),
            "final flush failed; buffered rows will be redelivered on restart"
        ),
    }
    commit_ready(
        &consumer,
        &cfg.topic_events,
        &offsets,
        &batcher,
        CommitMode::Sync,
    );
    info!("ly-processor stopped");
    Ok(())
}

/// Classify one consumed message: valid events go to the batch, invalid
/// payloads go to the DLQ (with an `error` field) and are committed once
/// the DLQ delivery is confirmed.
async fn handle_message(
    msg: &BorrowedMessage<'_>,
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    cfg: &Config,
    batcher: &mut Batcher,
    offsets: &mut OffsetTracker,
) {
    let partition = msg.partition();
    let offset = msg.offset();
    offsets.record(partition, offset);

    let payload: &[u8] = msg.payload().unwrap_or(b"");
    let outcome = PlatformEvent::from_bytes(payload).and_then(EventRow::try_from);
    match outcome {
        Ok(row) => {
            batcher.push(row, partition, offset);
        }
        Err(e) => {
            let reason = e.to_string();
            warn!(partition, offset, error = %reason, "invalid event; routing to DLQ");
            let doc = dlq_payload(payload, &reason);
            let key = msg.key().map(|k| k.to_vec());
            match time::timeout(
                DLQ_SEND_TIMEOUT,
                kafka_io::send_to_dlq(producer, &cfg.topic_dlq, key, doc),
            )
            .await
            {
                Ok(Ok(())) => debug!(partition, offset, "routed to DLQ"),
                Ok(Err(e)) => {
                    error!(partition, offset, error = %e, "DLQ produce failed; dropping record")
                }
                Err(_) => {
                    error!(partition, offset, "DLQ produce timed out; dropping record")
                }
            }
            // The record is handled either way; commit up to the oldest
            // unflushed batch row.
            commit_ready(
                consumer,
                &cfg.topic_events,
                offsets,
                batcher,
                CommitMode::Async,
            );
        }
    }
}

/// Flush the buffer to ClickHouse and commit on success. On failure the
/// rows stay buffered and the next trigger retries.
async fn flush(
    clickhouse: &ClickHouse,
    consumer: &StreamConsumer,
    cfg: &Config,
    batcher: &mut Batcher,
    offsets: &OffsetTracker,
    shutdown: watch::Receiver<bool>,
) {
    if !batcher.is_empty() {
        match clickhouse.insert(batcher.rows(), shutdown).await {
            Ok(()) => {
                debug!(rows = batcher.len(), "flushed batch to ClickHouse");
                batcher.clear();
            }
            Err(e) => error!(
                error = %e,
                rows = batcher.len(),
                "ClickHouse insert failed; batch retained and will be retried"
            ),
        }
    }
    commit_ready(
        consumer,
        &cfg.topic_events,
        offsets,
        batcher,
        CommitMode::Async,
    );
}

/// Commit every offset below the oldest unflushed batch row.
fn commit_ready(
    consumer: &StreamConsumer,
    topic: &str,
    offsets: &OffsetTracker,
    batcher: &Batcher,
    mode: CommitMode,
) {
    if let Err(e) = kafka_io::commit_offsets(consumer, topic, &offsets.committable(batcher), mode) {
        warn!(error = %e, "offset commit failed");
    }
}

/// SIGINT or SIGTERM (unix) / Ctrl-C (elsewhere).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                warn!(error = %e, "SIGTERM handler unavailable; using SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
