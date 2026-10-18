"""GDELT DOC 2 producer: Libya-tagged articles -> ly.events.raw."""

from __future__ import annotations

import argparse
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from .common import (
    KafkaPublisher,
    RunReporter,
    env,
    env_int,
    fetch_json,
    make_event,
    make_session,
    setup_logging,
    utcnow_iso,
)

log = logging.getLogger("ly_ingest.gdelt")

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
SOURCE = "gdelt"
SOURCE_KIND = "api"

CATEGORY_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("energy", ("oil", "gas", "energy")),
    ("economy", ("economy", "market")),
    ("conflict", ("conflict", "militia", "ceasefire")),
    ("environment", ("water", "drought", "climate")),
    ("politics", ("election", "government")),
)

# Per-process dedupe on event_id. Ids are stable (sha256 of the URL), so
# ClickHouse can dedupe again downstream if a process restarts.
_seen: set[str] = set()


def categorize(title: str) -> str:
    """Keyword heuristic mapping a headline to a contract category."""
    text = title.lower()
    for category, keywords in CATEGORY_KEYWORDS:
        if any(keyword in text for keyword in keywords):
            return category
    return "other"


def parse_seendate(value: str | None) -> str | None:
    """GDELT seendate (YYYYMMDDTHHMMSSZ) -> UTC ISO 8601."""
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return parsed.isoformat()


def map_article(article: dict[str, Any]) -> dict[str, Any] | None:
    """Map one GDELT artlist article to a canonical event; None when it has no URL."""
    url = article.get("url")
    if not url:
        return None
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    title = article.get("title") or ""
    return make_event(
        event_id=f"gdelt:{digest}",
        source=SOURCE,
        source_id=digest,
        event_time=parse_seendate(article.get("seendate")) or utcnow_iso(),
        title=title,
        url=url,
        category=categorize(title),
        raw=article,
    )


def poll(session: requests.Session, publisher: KafkaPublisher, seen: set[str]) -> int:
    data = fetch_json(
        session,
        BASE_URL,
        params={
            "query": env("GDELT_QUERY", "libya"),
            "mode": "artlist",
            "maxrecords": env("GDELT_MAX_RECORDS", "250"),
            "format": "json",
            "sort": "datedesc",
        },
    )
    articles = data.get("articles") if isinstance(data, dict) else None
    emitted = 0
    for article in articles or []:
        event = map_article(article)
        if event is None or event["event_id"] in seen:
            continue
        publisher.send(event)
        seen.add(event["event_id"])
        emitted += 1
    log.info("gdelt: %d articles fetched, %d new events", len(articles or []), emitted)
    return emitted


def run_once(
    *,
    session: requests.Session | None = None,
    publisher: KafkaPublisher | None = None,
    reporter: RunReporter | None = None,
    seen: set[str] | None = None,
) -> int:
    """Run one GDELT collection pass; returns events emitted."""
    own_session = session is None
    own_publisher = publisher is None
    session = session or make_session()
    publisher = publisher or KafkaPublisher()
    reporter = reporter or RunReporter()
    seen = _seen if seen is None else seen

    run_id = reporter.start(SOURCE)
    emitted, error = 0, None
    try:
        emitted = poll(session, publisher, seen)
        if own_publisher:
            publisher.close()
    except Exception as exc:
        error = exc
        log.exception("gdelt pass failed")
    finally:
        reporter.finish(run_id, emitted, str(error) if error else None)
        reporter.touch_source(SOURCE, SOURCE_KIND, BASE_URL)
        if own_session:
            session.close()
    if error is not None:
        raise error
    return emitted


def main() -> None:
    """Loop every POLL_INTERVAL_SECONDS (default 300); --once runs a single pass."""
    setup_logging()
    parser = argparse.ArgumentParser(description="Poll GDELT for Libya articles.")
    parser.add_argument("--once", action="store_true", help="run a single pass then exit")
    args = parser.parse_args()

    if args.once:
        run_once()
        return

    interval = env_int("POLL_INTERVAL_SECONDS", 300)
    log.info("gdelt producer loop: one pass every %ds", interval)
    while True:
        try:
            run_once()
        except Exception:
            log.exception("gdelt pass failed; continuing")
        time.sleep(interval)


if __name__ == "__main__":
    main()
