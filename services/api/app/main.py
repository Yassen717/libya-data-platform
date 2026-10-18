import json
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

from app import __version__, ch, pg

app = FastAPI(title="Libya Data Platform API", version=__version__)
Instrumentator().instrument(app).expose(app)


class Event(BaseModel):
    event_id: str
    source: str
    source_id: str
    event_time: datetime
    ingested_at: datetime
    title: str
    url: str
    country: str
    category: str
    actors: list[str]
    lat: float | None
    lon: float | None
    tone: float | None
    metrics: dict[str, Any]


class EventsPage(BaseModel):
    items: list[Event]
    limit: int
    offset: int


class StatRow(BaseModel):
    source: str
    category: str
    events: int
    avg_tone: float | None


class Totals(BaseModel):
    events: int
    sources: int
    categories: int
    avg_tone: float | None


class StatsSummary(BaseModel):
    window_days: int
    table: str
    by_source_category: list[StatRow]
    totals: Totals


class IndicatorPoint(BaseModel):
    event_id: str
    source: str
    title: str
    event_time: datetime
    indicator: str
    value: float | None
    year: int


class IndicatorSeries(BaseModel):
    items: list[IndicatorPoint]


class Run(BaseModel):
    run_id: UUID
    source: str
    kind: str | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    events_emitted: int
    error: str | None


class RunsPage(BaseModel):
    items: list[Run]


EVENT_COLUMNS = (
    "event_id, source, source_id, event_time, ingested_at, title, url, "
    "country, category, actors, lat, lon, tone, metrics_json"
)

SUMMARY_DAILY_SQL = """
SELECT source, category,
       sum(events) AS events,
       sum(avg_tone * events) / nullIf(sum(events), 0) AS avg_tone
FROM libya.events_daily
WHERE day >= today() - INTERVAL 30 DAY
GROUP BY source, category
ORDER BY source, category
"""

SUMMARY_FALLBACK_SQL = """
SELECT source, category, count() AS events, avgOrNull(tone) AS avg_tone
FROM libya.events
WHERE event_time >= now() - INTERVAL 30 DAY
GROUP BY source, category
ORDER BY source, category
"""

INDICATORS_SQL = """
SELECT event_id, source, title, event_time,
       JSONExtractString(metrics_json, 'indicator') AS indicator,
       JSONExtractFloat(metrics_json, 'value') AS value,
       toYear(event_time) AS year
FROM libya.events
WHERE metrics_json != '{}'
"""

RUNS_SQL = """
SELECT r.run_id, r.source, s.kind, r.started_at, r.finished_at,
       r.status, r.events_emitted, r.error
FROM pipeline_runs r
LEFT JOIN sources s ON s.source = r.source
ORDER BY r.started_at DESC
LIMIT 50
"""


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "ly-api",
        "title": "Libya Data Platform API",
        "version": __version__,
        "links": [
            {"rel": "docs", "href": "/docs"},
            {"rel": "openapi", "href": "/openapi.json"},
            {"rel": "health", "href": "/health"},
            {"rel": "events", "href": "/events"},
            {"rel": "stats", "href": "/stats/summary"},
            {"rel": "indicators", "href": "/indicators"},
            {"rel": "runs", "href": "/runs"},
            {"rel": "metrics", "href": "/metrics"},
        ],
    }


@app.get("/health")
def health() -> dict[str, str]:
    try:
        ch.query("SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"clickhouse unreachable: {exc}")
    return {"status": "ok", "clickhouse": "ok"}


@app.get("/events", response_model=EventsPage)
def list_events(
    source: str | None = None,
    category: str | None = None,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> EventsPage:
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if source is not None:
        clauses.append("source = {src:String}")
        params["src"] = source
    if category is not None:
        clauses.append("category = {cat:String}")
        params["cat"] = category
    if from_ is not None:
        clauses.append("event_time >= {ts_from:DateTime64(3)}")
        params["ts_from"] = from_
    if to is not None:
        clauses.append("event_time <= {ts_to:DateTime64(3)}")
        params["ts_to"] = to
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    sql = (
        f"SELECT {EVENT_COLUMNS} FROM libya.events{where} "
        "ORDER BY event_time DESC LIMIT {limit:UInt32} OFFSET {offset:UInt32}"
    )
    rows = ch.query(sql, params)
    for row in rows:
        row["metrics"] = json.loads(row.pop("metrics_json") or "{}")
    return EventsPage(items=rows, limit=limit, offset=offset)


@app.get("/stats/summary", response_model=StatsSummary)
def stats_summary() -> StatsSummary:
    table = "events_daily"
    rows = ch.query(SUMMARY_DAILY_SQL)
    if not rows:
        table = "events"
        rows = ch.query(SUMMARY_FALLBACK_SQL)
    stats = [StatRow.model_validate(row) for row in rows]
    tone_rows = [r for r in stats if r.avg_tone is not None]
    tone_weight = sum((r.avg_tone or 0) * r.events for r in tone_rows)
    totals = Totals(
        events=sum(r.events for r in stats),
        sources=len({r.source for r in stats}),
        categories=len({r.category for r in stats}),
        avg_tone=tone_weight / sum(r.events for r in tone_rows) if tone_rows else None,
    )
    return StatsSummary(
        window_days=30, table=table, by_source_category=stats, totals=totals
    )


@app.get("/indicators", response_model=IndicatorSeries)
def indicators(
    indicator: str | None = None,
    limit: int = Query(default=1000, ge=1, le=10000),
) -> IndicatorSeries:
    sql = INDICATORS_SQL
    params: dict[str, Any] = {"limit": limit}
    if indicator is not None:
        sql += " AND JSONExtractString(metrics_json, 'indicator') = {indicator:String}"
        params["indicator"] = indicator
    sql += " ORDER BY indicator, year LIMIT {limit:UInt32}"
    return IndicatorSeries(items=ch.query(sql, params))


@app.get("/runs", response_model=RunsPage)
def list_runs() -> RunsPage:
    return RunsPage(items=pg.query(RUNS_SQL))
