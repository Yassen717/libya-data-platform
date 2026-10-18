//! ClickHouse HTTP inserts: `POST {url}/?query=INSERT INTO {db}.events
//! FORMAT JSONEachRow` with basic auth and exponential-backoff retries.

use crate::schema::EventRow;
use anyhow::{bail, Context, Result};
use reqwest::Client;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::warn;

const MAX_ATTEMPTS: u32 = 5;
const BASE_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(4);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ClickHouse {
    client: Client,
    /// Base URL without trailing slash, e.g. `http://clickhouse:8123`.
    base: String,
    db: String,
    user: String,
    password: String,
}

impl ClickHouse {
    pub fn new(base_url: &str, db: &str, user: &str, password: &str) -> Result<Self> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').to_string(),
            db: db.to_string(),
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    /// Serialize rows as newline-delimited JSON (JSONEachRow body).
    fn encode(rows: &[EventRow]) -> String {
        let mut body = String::with_capacity(rows.len() * 160);
        for row in rows {
            // EventRow is plain data; serialization is effectively infallible,
            // but skip defensively rather than abort the whole batch.
            if let Ok(line) = serde_json::to_string(row) {
                body.push_str(&line);
                body.push('\n');
            }
        }
        body
    }

    /// Insert a batch, retrying with exponential backoff (max `MAX_ATTEMPTS`
    /// tries). Backoff aborts early when `shutdown` fires so SIGTERM stays
    /// responsive. On final failure the caller keeps the rows buffered —
    /// offsets for them are never committed.
    pub async fn insert(
        &self,
        rows: &[EventRow],
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let body = Self::encode(rows);
        let mut backoff = BASE_BACKOFF;
        for attempt in 1..MAX_ATTEMPTS {
            match self.try_insert(&body).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!(
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "ClickHouse insert failed; retrying"
                    );
                    tokio::select! {
                        _ = sleep(backoff) => {}
                        _ = shutdown.changed() => bail!("ClickHouse insert aborted by shutdown"),
                    }
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
        self.try_insert(&body)
            .await
            .with_context(|| format!("ClickHouse insert failed after {MAX_ATTEMPTS} attempts"))
    }

    async fn try_insert(&self, body: &str) -> Result<()> {
        let resp = self
            .client
            .post(format!("{}/", self.base))
            .query(&[(
                "query",
                format!("INSERT INTO {}.events FORMAT JSONEachRow", self.db),
            )])
            .basic_auth(&self.user, Some(&self.password))
            .body(body.to_string())
            .send()
            .await
            .context("ClickHouse request failed")?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let detail = resp
            .text()
            .await
            .unwrap_or_else(|_| "<no body>".to_string());
        let detail: String = detail.trim().chars().take(300).collect();
        bail!("ClickHouse HTTP {status}: {detail}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PlatformEvent;

    #[test]
    fn encodes_json_each_row() {
        let ev = PlatformEvent::from_bytes(
            br#"{"event_id":"gdelt:1","source":"gdelt",
                "event_time":"2024-05-01T00:00:00Z","ingested_at":"2024-05-01T00:00:01Z",
                "title":"t","country":"LY","actors":["a","b"],"tone":-1.5,
                "metrics":{"v":1}}"#,
        )
        .unwrap();
        let row = EventRow::try_from(ev).unwrap();
        let body = ClickHouse::encode(&[row]);
        assert!(body.ends_with('\n'));
        let v: serde_json::Value = serde_json::from_str(body.trim_end()).unwrap();
        assert_eq!(v["event_id"], "gdelt:1");
        assert_eq!(v["source"], "gdelt");
        assert_eq!(v["actors"], serde_json::json!(["a", "b"]));
        assert_eq!(v["tone"], -1.5f32);
        assert_eq!(v["metrics_json"], serde_json::json!("{\"v\":1}"));
        assert_eq!(v["raw_json"], "{}");
    }

    #[tokio::test]
    async fn empty_insert_is_noop() {
        let (_tx, rx) = watch::channel(false);
        let ch = ClickHouse::new("http://127.0.0.1:1", "libya", "u", "p").unwrap();
        ch.insert(&[], rx).await.unwrap();
    }

    #[tokio::test]
    async fn insert_retries_then_errors() {
        // Nothing listens on port 1; connect fails fast every attempt.
        let (_tx, rx) = watch::channel(false);
        let ch = ClickHouse::new("http://127.0.0.1:1", "libya", "u", "p").unwrap();
        let rows = [EventRow::dummy()];
        let err = ch.insert(&rows, rx).await.unwrap_err();
        assert!(err.to_string().contains("after 5 attempts"));
    }
}
