"""Shared plumbing for ly_ingest producers: HTTP, Kafka, Postgres lineage."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("ly_ingest")

MAX_RESPONSE_BYTES = 20 * 1024 * 1024
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
STATUS_FORCELIST = (429, 500, 502, 503, 504)


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name))
    except ValueError:
        return default


def setup_logging() -> None:
    logging.basicConfig(
        level=env("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def make_session() -> requests.Session:
    """Session with retry/backoff and a desktop User-Agent (ported from Scrap-app)."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=STATUS_FORCELIST,
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 30,
    max_bytes: int = MAX_RESPONSE_BYTES,
) -> Any:
    """GET a JSON document, streaming the body and capping it at max_bytes."""
    with session.get(url, params=params, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        chunks: list[bytes] = []
        size = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            size += len(chunk)
            if size > max_bytes:
                raise ValueError(f"response from {url} exceeds {max_bytes} bytes")
            chunks.append(chunk)
    return json.loads(b"".join(chunks))


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(
    *,
    event_id: str,
    source: str,
    source_id: str,
    event_time: str,
    title: str,
    country: str = "LY",
    category: str = "other",
    url: str | None = None,
    actors: list[str] | None = None,
    lat: float | None = None,
    lon: float | None = None,
    tone: float | None = None,
    metrics: dict[str, Any] | None = None,
    raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the canonical event envelope (contracts/event.schema.json)."""
    event: dict[str, Any] = {
        "event_id": event_id,
        "source": source,
        "source_id": source_id,
        "event_time": event_time,
        "ingested_at": utcnow_iso(),
        "title": title,
        "country": country,
        "category": category,
        "actors": actors or [],
        "lat": lat,
        "lon": lon,
        "tone": tone,
        "metrics": metrics or {},
    }
    if url:
        event["url"] = url
    if raw is not None:
        event["raw"] = raw
    return event


class KafkaPublisher:
    """kafka-python-ng producer: canonical JSON events keyed by event_id."""

    def __init__(self, topic: str | None = None) -> None:
        self.topic = topic or env("TOPIC_EVENTS", "ly.events.raw")
        self._producer = KafkaProducer(
            bootstrap_servers=[
                s.strip() for s in env("KAFKA_BROKERS", "localhost:9092").split(",") if s.strip()
            ],
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            acks="all",
            enable_idempotence=True,
            linger_ms=500,
        )
        self._pending: list[Any] = []

    def send(self, event: dict) -> None:
        """Queue one event for delivery; confirmed on close()."""
        self._pending.append(
            self._producer.send(self.topic, key=event["event_id"], value=event)
        )

    def close(self) -> None:
        self._producer.flush(timeout=30)
        for future in self._pending:
            future.get(timeout=30)
        self._pending.clear()
        self._producer.close(timeout=10)


class RunReporter:
    """Records producer passes in Postgres pipeline_runs; no-ops when Postgres is down."""

    def __init__(self) -> None:
        self._conninfo = {
            "host": env("POSTGRES_HOST", "localhost"),
            "port": env_int("POSTGRES_PORT", 5432),
            "dbname": env("POSTGRES_DB", "libya"),
            "user": env("POSTGRES_USER", "ly"),
            "password": env("POSTGRES_PASSWORD", ""),
            "connect_timeout": 5,
        }

    def _query(self, sql: str, params: tuple[Any, ...]) -> Any:
        try:
            with psycopg.connect(**self._conninfo) as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()[0] if cur.description else None
        except psycopg.Error as exc:
            log.warning("postgres unreachable, continuing without lineage: %s", exc)
            return None

    def start(self, source: str) -> str | None:
        """Open a pipeline_runs row and return its run_id (None when Postgres is down)."""
        row = self._query(
            "INSERT INTO pipeline_runs (source) VALUES (%s) RETURNING run_id", (source,)
        )
        return str(row) if row else None

    def finish(self, run_id: str | None, events_emitted: int, error: str | None = None) -> None:
        """Close out a run row with its final status."""
        if run_id is None:
            return
        self._query(
            "UPDATE pipeline_runs SET finished_at = now(), status = %s, "
            "events_emitted = %s, error = %s WHERE run_id = %s::uuid",
            ("failed" if error else "success", events_emitted, error, run_id),
        )

    def touch_source(self, source: str, kind: str, base_url: str) -> None:
        """Upsert the sources registry row and stamp last_run_at."""
        self._query(
            "INSERT INTO sources (source, kind, base_url, last_run_at) "
            "VALUES (%s, %s, %s, now()) ON CONFLICT (source) DO UPDATE "
            "SET last_run_at = now(), kind = EXCLUDED.kind, base_url = EXCLUDED.base_url",
            (source, kind, base_url),
        )
