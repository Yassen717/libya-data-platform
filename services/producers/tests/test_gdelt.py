import hashlib

import jsonschema
import pytest
import responses

from ly_ingest import common, gdelt

ARTICLE = {
    "url": "https://example.com/libya-oil-output",
    "url_mobile": "https://example.com/libya-oil-output?m=1",
    "title": "Libya oil output rises as ceasefire holds",
    "seendate": "20250102T093000Z",
    "socialimage": "https://example.com/img.jpg",
    "domain": "example.com",
    "language": "English",
    "sourcecountry": "United States",
}


def test_map_article_validates_against_contract(event_schema):
    event = gdelt.map_article(ARTICLE)
    jsonschema.validate(event, event_schema)
    assert event["source"] == "gdelt"
    assert event["country"] == "LY"
    assert event["actors"] == []
    assert event["lat"] is None and event["lon"] is None and event["tone"] is None
    assert event["metrics"] == {}
    assert event["raw"] == ARTICLE


def test_map_article_stable_id_from_url():
    event = gdelt.map_article(ARTICLE)
    digest = hashlib.sha256(ARTICLE["url"].encode("utf-8")).hexdigest()[:16]
    assert event["event_id"] == f"gdelt:{digest}"
    assert event["source_id"] == digest
    assert gdelt.map_article(dict(ARTICLE))["event_id"] == event["event_id"]


def test_map_article_parses_seendate_to_utc_iso():
    event = gdelt.map_article(ARTICLE)
    assert event["event_time"] == "2025-01-02T09:30:00+00:00"


def test_map_article_without_url_is_skipped():
    assert gdelt.map_article({"title": "no url"}) is None


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Libya oil exports resume", "energy"),
        ("Gas field deal signed", "energy"),
        ("Dinar slips on parallel market", "economy"),
        ("Militia clash near Tripoli", "conflict"),
        ("Drought hits coastal farms", "environment"),
        ("New government sworn in", "politics"),
        ("Festival opens in Benghazi", "other"),
    ],
)
def test_categorize_keywords(title, expected):
    assert gdelt.categorize(title) == expected


@responses.activate
def test_run_once_emits_and_dedupes(publisher, reporter):
    responses.add(
        responses.GET,
        gdelt.BASE_URL,
        json={"articles": [ARTICLE]},
        status=200,
    )
    seen: set[str] = set()
    assert gdelt.run_once(
        session=common.make_session(), publisher=publisher, reporter=reporter, seen=seen
    ) == 1
    assert len(publisher.events) == 1
    # Same pass again: the already-seen event_id is not re-emitted.
    assert gdelt.run_once(
        session=common.make_session(), publisher=publisher, reporter=reporter, seen=seen
    ) == 0
    assert len(publisher.events) == 1
    request = responses.calls[0].request
    assert "mode=artlist" in request.url and "format=json" in request.url


@responses.activate
def test_run_once_reports_failure_and_raises(publisher, reporter):
    responses.add(responses.GET, gdelt.BASE_URL, body="upstream down", status=503)
    with pytest.raises(Exception):
        gdelt.run_once(
            session=common.make_session(),
            publisher=publisher,
            reporter=reporter,
            seen=set(),
        )
    _, emitted, error = reporter.finished[-1]
    assert emitted == 0
    assert error
