//! Batch buffering and the offset-commit watermark.
//!
//! A Kafka offset is only safe to commit once the record is fully handled —
//! either inserted into ClickHouse or confirmed on the DLQ. Because valid
//! rows linger in the buffer while later invalid records are confirmed on
//! the DLQ, "handled" is not simply the highest seen offset: it is the
//! highest offset *below the oldest unflushed batch row* (or the last
//! consumed offset when the buffer holds nothing for that partition).

use crate::schema::EventRow;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub struct Batcher {
    max_len: usize,
    flush_after: Duration,
    rows: Vec<EventRow>,
    /// Lowest buffered (not yet flushed) offset per partition.
    min_offsets: BTreeMap<i32, i64>,
    /// When the first row of the current batch was buffered.
    first_at: Option<Instant>,
}

impl Batcher {
    pub fn new(max_len: usize, flush_after: Duration) -> Self {
        Self {
            max_len,
            flush_after,
            rows: Vec::new(),
            min_offsets: BTreeMap::new(),
            first_at: None,
        }
    }

    /// Buffer a row. Kafka delivers offsets in increasing order per
    /// partition, so `or_insert` keeps the lowest one.
    pub fn push(&mut self, row: EventRow, partition: i32, offset: i64) {
        if self.rows.is_empty() {
            self.first_at = Some(Instant::now());
        }
        self.min_offsets.entry(partition).or_insert(offset);
        self.rows.push(row);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn rows(&self) -> &[EventRow] {
        &self.rows
    }

    /// Oldest unflushed offset for a partition, if any.
    pub fn min_offset(&self, partition: i32) -> Option<i64> {
        self.min_offsets.get(&partition).copied()
    }

    /// Time by which the current batch must be flushed.
    pub fn deadline(&self) -> Option<Instant> {
        self.first_at.map(|t| t + self.flush_after)
    }

    /// Flush trigger: size cap reached, or `flush_after` elapsed since the
    /// first buffered row.
    pub fn is_due(&self, now: Instant) -> bool {
        !self.rows.is_empty()
            && (self.rows.len() >= self.max_len || self.deadline().is_some_and(|d| now >= d))
    }

    /// Drop the batch after a confirmed ClickHouse insert.
    pub fn clear(&mut self) {
        self.rows.clear();
        self.min_offsets.clear();
        self.first_at = None;
    }
}

/// Highest consumed offset per partition, regardless of how the record was
/// handled. Combined with the batcher's buffered minimums this yields the
/// offsets that are safe to commit.
#[derive(Default)]
pub struct OffsetTracker {
    consumed: BTreeMap<i32, i64>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a consumed offset (any outcome: batched or DLQ-routed).
    pub fn record(&mut self, partition: i32, offset: i64) {
        let entry = self.consumed.entry(partition).or_insert(offset);
        *entry = (*entry).max(offset);
    }

    /// `(partition, next_offset)` pairs safe to commit: every offset below
    /// `next_offset` is fully handled. Offsets at or above the oldest
    /// unflushed batch row are never committed.
    pub fn committable(&self, batcher: &Batcher) -> Vec<(i32, i64)> {
        self.consumed
            .iter()
            .filter_map(|(&partition, &last)| {
                let ready = batcher
                    .min_offset(partition)
                    .map(|min| min - 1)
                    .unwrap_or(last);
                (ready >= 0).then_some((partition, ready + 1))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_trigger() {
        let mut b = Batcher::new(2, Duration::from_secs(60));
        b.push(EventRow::dummy(), 0, 0);
        assert!(!b.is_due(Instant::now()));
        b.push(EventRow::dummy(), 0, 1);
        assert!(b.is_due(Instant::now()));
    }

    #[test]
    fn time_trigger() {
        let mut b = Batcher::new(500, Duration::from_secs(2));
        assert!(!b.is_due(Instant::now() + Duration::from_secs(10))); // empty never due
        b.push(EventRow::dummy(), 0, 0);
        assert!(!b.is_due(Instant::now()));
        assert!(b.is_due(Instant::now() + Duration::from_secs(3)));
    }

    #[test]
    fn clear_resets() {
        let mut b = Batcher::new(2, Duration::from_secs(1));
        b.push(EventRow::dummy(), 0, 5);
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.deadline(), None);
        assert_eq!(b.min_offset(0), None);
    }

    #[test]
    fn dlq_offsets_never_pass_unflushed_batch() {
        let mut t = OffsetTracker::new();
        let mut b = Batcher::new(10, Duration::from_secs(60));
        // partition 0: offset 5 valid -> buffered; offset 6 invalid -> DLQ'd.
        t.record(0, 5);
        b.push(EventRow::dummy(), 0, 5);
        t.record(0, 6);
        // Committing past 5 would lose the buffered row, so only "next = 5".
        assert_eq!(t.committable(&b), vec![(0, 5)]);
        // After the flush confirms, both 5 and 6 are handled -> next = 7.
        b.clear();
        assert_eq!(t.committable(&b), vec![(0, 7)]);
    }

    #[test]
    fn committable_is_per_partition() {
        let mut t = OffsetTracker::new();
        let mut b = Batcher::new(10, Duration::from_secs(60));
        t.record(0, 3);
        b.push(EventRow::dummy(), 0, 3); // p0 blocked at 3
        t.record(1, 9); // p1 fully handled (e.g. all DLQ'd)
        assert_eq!(t.committable(&b), vec![(0, 3), (1, 10)]);
    }
}
