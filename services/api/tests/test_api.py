from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import ch, pg
from app.main import app

EVENT_ROW: dict[str, Any] = {
    "event_id": "gdelt:abc123",
    "source": "gdelt",
    "source_id": "abc123",
    "event_time": datetime(2025, 1, 2, 12, 0, tzinfo=UTC),
    "ingested_at": datetime(2025, 1, 2, 12, 5, tzinfo=UTC),
    "title": "Oil output rises in eastern Libya",
    "url": "https://example.com/a",
    "country": "LY",
    "category": "energy",
    "actors": ["NOC"],
    "lat": 30.0,
    "lon": 18.5,
    "tone": 1.5,
    "metrics_json": '{"indicator": "NY.GDP.PETR.RT.ZS", "value": 58.2}',
}

DAILY_ROWS = [
    {"source": "gdelt", "category": "energy", "events": 10, "avg_tone": 1.0},
    {"source": "gdelt", "category": "politics", "events": 30, "avg_tone": -2.0},
    {"source": "worldbank", "category": "economy", "events": 5, "avg_tone": None},
]


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_root(client: TestClient) -> None:
    body = client.get("/").json()
    assert body["service"] == "ly-api"
    assert body["title"] == "Libya Data Platform API"
    assert {link["rel"] for link in body["links"]} >= {"docs", "events", "metrics"}


def test_health_ok(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ch, "query", lambda sql, params=None: [{"1": 1}])
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "clickhouse": "ok"}


def test_health_clickhouse_down(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(sql: str, params: dict | None = None) -> list[dict]:
        raise ConnectionError("refused")

    monkeypatch.setattr(ch, "query", boom)
    r = client.get("/health")
    assert r.status_code == 503
    assert "clickhouse" in r.json()["detail"]


def test_events_shape_and_metrics_parsing(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ch, "query", lambda sql, params=None: [dict(EVENT_ROW)])
    r = client.get("/events")
    assert r.status_code == 200
    body = r.json()
    assert body["limit"] == 100 and body["offset"] == 0
    item = body["items"][0]
    assert item["event_id"] == "gdelt:abc123"
    assert item["metrics"] == {"indicator": "NY.GDP.PETR.RT.ZS", "value": 58.2}
    assert "metrics_json" not in item


def test_events_limit_capped(client: TestClient) -> None:
    assert client.get("/events", params={"limit": 1001}).status_code == 422
    assert client.get("/events", params={"limit": 0}).status_code == 422


def test_events_filters_passed_as_params(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, dict]] = []

    def fake(sql: str, params: dict | None = None) -> list[dict]:
        calls.append((sql, params or {}))
        return []

    monkeypatch.setattr(ch, "query", fake)
    r = client.get(
        "/events",
        params={
            "source": "gdelt",
            "category": "energy",
            "from": "2025-01-01T00:00:00Z",
            "to": "2025-02-01T00:00:00Z",
            "limit": 5,
            "offset": 10,
        },
    )
    assert r.status_code == 200
    sql, params = calls[0]
    assert "source = {src:String}" in sql
    assert "category = {cat:String}" in sql
    assert "event_time >= {ts_from:DateTime64(3)}" in sql
    assert "event_time <= {ts_to:DateTime64(3)}" in sql
    assert "gdelt" not in sql and "energy" not in sql
    assert params["src"] == "gdelt"
    assert params["cat"] == "energy"
    assert params["ts_from"] == datetime(2025, 1, 1, tzinfo=UTC)
    assert params["ts_to"] == datetime(2025, 2, 1, tzinfo=UTC)
    assert params["limit"] == 5 and params["offset"] == 10


def test_stats_summary_from_daily_mv(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ch, "query", lambda sql, params=None: [dict(r) for r in DAILY_ROWS])
    r = client.get("/stats/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["table"] == "events_daily"
    assert body["window_days"] == 30
    assert len(body["by_source_category"]) == 3
    assert body["totals"]["events"] == 45
    assert body["totals"]["sources"] == 2
    assert body["totals"]["categories"] == 3
    # weighted over tone-bearing rows only: (1*10 + -2*30) / 40 = -1.25
    assert body["totals"]["avg_tone"] == pytest.approx(-1.25)


def test_stats_summary_falls_back_to_events(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    queries: list[str] = []

    def fake(sql: str, params: dict | None = None) -> list[dict]:
        queries.append(sql)
        if "events_daily" in sql:
            return []
        return [dict(r) for r in DAILY_ROWS]

    monkeypatch.setattr(ch, "query", fake)
    r = client.get("/stats/summary")
    assert r.status_code == 200
    assert r.json()["table"] == "events"
    assert len(queries) == 2


def test_runs_empty(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pg, "query", lambda sql, params=None: [])
    r = client.get("/runs")
    assert r.status_code == 200
    assert r.json() == {"items": []}


def test_runs_row(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    row = {
        "run_id": "12345678-1234-5678-1234-567812345678",
        "source": "gdelt",
        "kind": "api",
        "status": "success",
        "started_at": datetime(2025, 1, 2, 12, 0, tzinfo=UTC),
        "finished_at": datetime(2025, 1, 2, 12, 1, tzinfo=UTC),
        "events_emitted": 42,
        "error": None,
    }
    monkeypatch.setattr(pg, "query", lambda sql, params=None: [row])
    body = client.get("/runs").json()
    assert body["items"][0]["events_emitted"] == 42
    assert body["items"][0]["kind"] == "api"


def test_indicators_filter(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, dict]] = []

    def fake(sql: str, params: dict | None = None) -> list[dict]:
        calls.append((sql, params or {}))
        return []

    monkeypatch.setattr(ch, "query", fake)
    r = client.get("/indicators", params={"indicator": "NY.GDP.PETR.RT.ZS"})
    assert r.status_code == 200
    assert r.json() == {"items": []}
    sql, params = calls[0]
    assert "metrics_json != '{}'" in sql
    assert "{indicator:String}" in sql
    assert "NY.GDP.PETR.RT.ZS" not in sql
    assert params["indicator"] == "NY.GDP.PETR.RT.ZS"


def test_metrics_endpoint_exposed(client: TestClient) -> None:
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "http_requests" in r.text or "python_info" in r.text
