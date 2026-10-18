import json

import pytest
import responses

from ly_ingest import common


def test_make_session_has_retry_and_ua():
    session = common.make_session()
    assert "Mozilla" in session.headers["User-Agent"]
    retries = session.get_adapter("https://example.com").max_retries
    assert retries.total == 3
    assert 429 in retries.status_forcelist
    assert 503 in retries.status_forcelist


@responses.activate
def test_fetch_json_parses_body():
    responses.add(responses.GET, "https://example.com/data", json={"ok": True})
    assert common.fetch_json(common.make_session(), "https://example.com/data") == {
        "ok": True
    }


@responses.activate
def test_fetch_json_enforces_size_cap():
    responses.add(
        responses.GET, "https://example.com/big", body=b"x" * 1024, status=200
    )
    with pytest.raises(ValueError, match="exceeds"):
        common.fetch_json(common.make_session(), "https://example.com/big", max_bytes=16)


def test_kafka_publisher_serializes_and_sends(monkeypatch):
    sent = []

    class FakeProducer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def send(self, topic, key=None, value=None):
            sent.append((topic, key, value))
            return FakeFuture()

        def flush(self, timeout=None):
            pass

        def close(self, timeout=None):
            pass

    class FakeFuture:
        def get(self, timeout=None):
            return None

    monkeypatch.setenv("KAFKA_BROKERS", "a:9092, b:9092")
    monkeypatch.setenv("TOPIC_EVENTS", "ly.events.raw")
    monkeypatch.setattr(common, "KafkaProducer", FakeProducer)

    pub = common.KafkaPublisher()
    producer = pub._producer
    assert producer.kwargs["bootstrap_servers"] == ["a:9092", "b:9092"]
    assert producer.kwargs["acks"] == "all"
    assert producer.kwargs["enable_idempotence"] is True
    assert producer.kwargs["linger_ms"] == 500

    event = {"event_id": "gdelt:abc", "title": "t"}
    pub.send(event)
    assert sent == [("ly.events.raw", "gdelt:abc", event)]
    assert producer.kwargs["key_serializer"]("gdelt:abc") == b"gdelt:abc"
    assert json.loads(producer.kwargs["value_serializer"](event)) == event
    pub.close()


def test_run_reporter_degrades_when_postgres_down(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "1")
    reporter = common.RunReporter()
    assert reporter.start("gdelt") is None
    reporter.finish(None, 0)  # no-op
    reporter.finish("some-id", 3)  # logs warning, does not raise
    reporter.touch_source("gdelt", "api", "https://api.gdeltproject.org")
