//! Typed mirror of `contracts/event.schema.json` and the `libya.events`
//! ClickHouse row shape. Extra/unknown fields are tolerated (the JSON Schema
//! does not forbid them); violations of declared types are rejected and the
//! record is routed to the DLQ by the caller.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// Canonical event envelope, one per Kafka message on `ly.events.raw`.
#[derive(Debug, Clone, Deserialize)]
pub struct PlatformEvent {
    pub event_id: String,
    pub source: Source,
    #[serde(default)]
    pub source_id: Option<String>,
    pub event_time: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub title: String,
    #[serde(default)]
    pub url: Option<String>,
    pub country: String,
    #[serde(default)]
    pub category: Category,
    #[serde(default)]
    pub actors: Vec<String>,
    #[serde(default)]
    pub lat: Option<f64>,
    #[serde(default)]
    pub lon: Option<f64>,
    #[serde(default)]
    pub tone: Option<f64>,
    #[serde(default)]
    pub metrics: Map<String, Value>,
    #[serde(default)]
    pub raw: Map<String, Value>,
}

impl PlatformEvent {
    /// Deserialize a message payload; the error message doubles as the DLQ
    /// `error` field describing the violation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// `source` enum from the schema: gdelt | worldbank | scraper | manual.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Gdelt,
    Worldbank,
    Scraper,
    Manual,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gdelt => "gdelt",
            Self::Worldbank => "worldbank",
            Self::Scraper => "scraper",
            Self::Manual => "manual",
        }
    }
}

/// `category` enum from the schema; default `other`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    Politics,
    Economy,
    Energy,
    Resources,
    Society,
    Conflict,
    Environment,
    Other,
}

impl Default for Category {
    fn default() -> Self {
        Self::Other
    }
}

impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Politics => "politics",
            Self::Economy => "economy",
            Self::Energy => "energy",
            Self::Resources => "resources",
            Self::Society => "society",
            Self::Conflict => "conflict",
            Self::Environment => "environment",
            Self::Other => "other",
        }
    }
}

/// One row of `libya.events` (see db/clickhouse/init/001_events.sql),
/// serialized as a single JSONEachRow line.
#[derive(Debug, Serialize)]
pub struct EventRow {
    pub event_id: String,
    pub source: String,
    pub source_id: String,
    /// `DateTime64(3)` rendered as `YYYY-MM-DD HH:MM:SS.mmm` in UTC.
    pub event_time: String,
    pub ingested_at: String,
    pub title: String,
    pub url: String,
    pub country: String,
    pub category: String,
    pub actors: Vec<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub tone: Option<f32>,
    pub metrics_json: String,
    pub raw_json: String,
}

impl TryFrom<PlatformEvent> for EventRow {
    type Error = anyhow::Error;

    fn try_from(ev: PlatformEvent) -> Result<Self> {
        // Schema: metrics values may only be number | string | null.
        if !ev
            .metrics
            .values()
            .all(|v| matches!(v, Value::Null | Value::Number(_) | Value::String(_)))
        {
            bail!("metrics values must be number, string or null");
        }
        Ok(Self {
            event_id: ev.event_id,
            source: ev.source.as_str().to_string(),
            source_id: ev.source_id.unwrap_or_default(),
            event_time: format_datetime64(&ev.event_time),
            ingested_at: format_datetime64(&ev.ingested_at),
            title: ev.title,
            url: ev.url.unwrap_or_default(),
            country: ev.country,
            category: ev.category.as_str().to_string(),
            actors: ev.actors,
            lat: ev.lat,
            lon: ev.lon,
            tone: ev.tone.map(|t| t as f32),
            metrics_json: to_json_string(&ev.metrics),
            raw_json: to_json_string(&ev.raw),
        })
    }
}

/// ClickHouse `DateTime64(3, 'UTC')` text form: `2024-05-01 12:34:56.789`.
pub fn format_datetime64(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn to_json_string(map: &Map<String, Value>) -> String {
    serde_json::to_string(map).unwrap_or_else(|_| "{}".to_string())
}

/// DLQ payload: the original bytes with a top-level `error` field added.
/// If the payload is not a JSON object it cannot carry a field, so it is
/// wrapped as `{"error": ..., "payload": ...}` instead.
pub fn dlq_payload(original: &[u8], error: &str) -> Vec<u8> {
    let doc = match serde_json::from_slice::<Value>(original) {
        Ok(Value::Object(mut map)) => {
            map.insert("error".to_string(), Value::String(error.to_string()));
            Value::Object(map)
        }
        Ok(other) => json!({ "error": error, "payload": other }),
        Err(_) => json!({ "error": error, "payload": String::from_utf8_lossy(original) }),
    };
    serde_json::to_vec(&doc).unwrap_or_default()
}

#[cfg(test)]
impl EventRow {
    /// Minimal valid row for batcher/clickhouse unit tests.
    pub fn dummy() -> Self {
        Self {
            event_id: "test:1".into(),
            source: "gdelt".into(),
            source_id: String::new(),
            event_time: "2024-05-01 00:00:00.000".into(),
            ingested_at: "2024-05-01 00:00:01.000".into(),
            title: "t".into(),
            url: String::new(),
            country: "LY".into(),
            category: "other".into(),
            actors: vec![],
            lat: None,
            lon: None,
            tone: None,
            metrics_json: "{}".into(),
            raw_json: "{}".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sample matching contracts/event.schema.json exactly.
    const VALID: &str = r#"{
        "event_id": "gdelt:1234567890",
        "source": "gdelt",
        "source_id": "art-998877",
        "event_time": "2024-05-01T12:34:56.789Z",
        "ingested_at": "2024-05-01T12:35:02Z",
        "title": "Clashes reported in Tripoli",
        "url": "https://example.com/news/1",
        "country": "LY",
        "category": "conflict",
        "actors": ["GNA", "LNA"],
        "lat": 32.8872,
        "lon": 13.1913,
        "tone": -4.25,
        "metrics": {"value": 3.7, "indicator": "NY.GDP.PETR.RT.ZS", "note": null},
        "raw": {"upstream": {"id": 1, "seen": true}}
    }"#;

    #[test]
    fn valid_event_maps_to_row() {
        let ev = PlatformEvent::from_bytes(VALID.as_bytes()).unwrap();
        let row = EventRow::try_from(ev).unwrap();
        assert_eq!(row.event_id, "gdelt:1234567890");
        assert_eq!(row.source, "gdelt");
        assert_eq!(row.source_id, "art-998877");
        assert_eq!(row.event_time, "2024-05-01 12:34:56.789");
        assert_eq!(row.ingested_at, "2024-05-01 12:35:02.000");
        assert_eq!(row.category, "conflict");
        assert_eq!(row.actors, vec!["GNA", "LNA"]);
        assert_eq!(row.lat, Some(32.8872));
        assert_eq!(row.lon, Some(13.1913));
        assert_eq!(row.tone, Some(-4.25f32));
        let metrics: Value = serde_json::from_str(&row.metrics_json).unwrap();
        assert_eq!(metrics["value"], json!(3.7));
        assert_eq!(metrics["indicator"], json!("NY.GDP.PETR.RT.ZS"));
        let raw: Value = serde_json::from_str(&row.raw_json).unwrap();
        assert_eq!(raw["upstream"]["id"], json!(1));
    }

    #[test]
    fn optional_fields_default() {
        let minimal = r#"{
            "event_id": "worldbank:NY.GDP:2023",
            "source": "worldbank",
            "event_time": "2024-01-01T00:00:00Z",
            "ingested_at": "2024-01-02T00:00:00Z",
            "title": "GDP (current US$)",
            "country": "LY"
        }"#;
        let row =
            EventRow::try_from(PlatformEvent::from_bytes(minimal.as_bytes()).unwrap()).unwrap();
        assert_eq!(row.category, "other");
        assert!(row.actors.is_empty());
        assert_eq!(row.source_id, "");
        assert_eq!(row.url, "");
        assert_eq!(row.metrics_json, "{}");
        assert_eq!(row.raw_json, "{}");
        assert!(row.lat.is_none() && row.lon.is_none() && row.tone.is_none());
    }

    #[test]
    fn malformed_payloads_rejected() {
        // missing required field
        assert!(PlatformEvent::from_bytes(
            br#"{"source":"gdelt","event_time":"2024-01-01T00:00:00Z",
                "ingested_at":"2024-01-01T00:00:00Z","title":"t","country":"LY"}"#
        )
        .is_err());
        // source not in enum
        assert!(PlatformEvent::from_bytes(
            br#"{"event_id":"x:1","source":"reuters","event_time":"2024-01-01T00:00:00Z",
                "ingested_at":"2024-01-01T00:00:00Z","title":"t","country":"LY"}"#
        )
        .is_err());
        // unparseable timestamp
        assert!(PlatformEvent::from_bytes(
            br#"{"event_id":"x:1","source":"gdelt","event_time":"yesterday",
                "ingested_at":"2024-01-01T00:00:00Z","title":"t","country":"LY"}"#
        )
        .is_err());
        // not JSON at all
        assert!(PlatformEvent::from_bytes(b"not json").is_err());
        // metrics must be an object
        assert!(PlatformEvent::from_bytes(
            br#"{"event_id":"x:1","source":"gdelt","event_time":"2024-01-01T00:00:00Z",
                "ingested_at":"2024-01-01T00:00:00Z","title":"t","country":"LY","metrics":[1]}"#
        )
        .is_err());
    }

    #[test]
    fn metrics_rejects_nested_objects() {
        let ev = PlatformEvent::from_bytes(
            br#"{"event_id":"x:1","source":"gdelt","event_time":"2024-01-01T00:00:00Z",
                "ingested_at":"2024-01-01T00:00:00Z","title":"t","country":"LY",
                "metrics":{"nested":{"a":1}}}"#,
        )
        .unwrap();
        assert!(EventRow::try_from(ev).is_err());
    }

    #[test]
    fn datetime64_format() {
        let dt: DateTime<Utc> = "2024-05-01T12:34:56.7891Z".parse().unwrap();
        assert_eq!(format_datetime64(&dt), "2024-05-01 12:34:56.789");
        // offsets normalize to UTC
        let dt: DateTime<Utc> = "2024-05-01T14:34:56+02:00".parse().unwrap();
        assert_eq!(format_datetime64(&dt), "2024-05-01 12:34:56.000");
    }

    #[test]
    fn dlq_payload_adds_error_field() {
        let out = dlq_payload(VALID.as_bytes(), "bad record");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["error"], json!("bad record"));
        assert_eq!(v["event_id"], json!("gdelt:1234567890"));
    }

    #[test]
    fn dlq_payload_wraps_non_objects() {
        let v: Value = serde_json::from_slice(&dlq_payload(b"[1,2]", "nope")).unwrap();
        assert_eq!(v["payload"], json!([1, 2]));
        // non-JSON bytes are preserved lossily as a string
        let v: Value = serde_json::from_slice(&dlq_payload(b"\xff\xfe junk", "nope")).unwrap();
        assert!(v["payload"].is_string());
    }
}
