"""World Bank producer: configured indicators for LBY -> ly.events.raw."""

from __future__ import annotations

import argparse
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
)

log = logging.getLogger("ly_ingest.worldbank")

BASE_URL = "https://api.worldbank.org/v2/country/LBY/indicator"
SOURCE = "worldbank"
SOURCE_KIND = "api"
DEFAULT_INDICATORS = "NY.GDP.PETR.RT.ZS,NY.GDP.MKTP.CD,SP.POP.TOTL"
ENERGY_KEYWORDS = ("oil", "gas", "petr", "fuel", "energy")

# Per-process dedupe on event_id; stable ids let ClickHouse dedupe downstream.
_seen: set[str] = set()


def indicators() -> list[str]:
    raw = env("WORLDBANK_INDICATORS", DEFAULT_INDICATORS)
    return [code.strip() for code in raw.split(",") if code.strip()]


def categorize(code: str, name: str) -> str:
    """Oil/gas indicators are energy; everything else is economy."""
    text = f"{code} {name}".lower()
    return "energy" if any(keyword in text for keyword in ENERGY_KEYWORDS) else "economy"


def map_row(code: str, row: dict[str, Any]) -> dict[str, Any] | None:
    """Map one indicator/year row to a canonical event; None for null values."""
    if row.get("value") is None:
        return None
    try:
        year = int(row["date"])
    except (KeyError, TypeError, ValueError):
        return None
    name = (row.get("indicator") or {}).get("value") or code
    return make_event(
        event_id=f"worldbank:{code}:{year}",
        source=SOURCE,
        source_id=code,
        event_time=datetime(year, 1, 1, tzinfo=timezone.utc).isoformat(),
        title=f"{name} {year}",
        category=categorize(code, name),
        metrics={"indicator": code, "value": float(row["value"]), "unit": name},
        raw=row,
    )


def poll(session: requests.Session, publisher: KafkaPublisher, seen: set[str]) -> int:
    emitted = 0
    for code in indicators():
        data = fetch_json(
            session, f"{BASE_URL}/{code}", params={"format": "json", "per_page": "100"}
        )
        rows = data[1] if isinstance(data, list) and len(data) > 1 else []
        for row in rows or []:
            event = map_row(code, row)
            if event is None or event["event_id"] in seen:
                continue
            publisher.send(event)
            seen.add(event["event_id"])
            emitted += 1
    log.info("worldbank: %d new events", emitted)
    return emitted


def run_once(
    *,
    session: requests.Session | None = None,
    publisher: KafkaPublisher | None = None,
    reporter: RunReporter | None = None,
    seen: set[str] | None = None,
) -> int:
    """Run one World Bank collection pass; returns events emitted."""
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
        log.exception("worldbank pass failed")
    finally:
        reporter.finish(run_id, emitted, str(error) if error else None)
        reporter.touch_source(SOURCE, SOURCE_KIND, BASE_URL)
        if own_session:
            session.close()
    if error is not None:
        raise error
    return emitted


def main() -> None:
    """Loop every POLL_INTERVAL_SECONDS (default 86400); --once runs a single pass."""
    setup_logging()
    parser = argparse.ArgumentParser(description="Pull World Bank indicators for Libya.")
    parser.add_argument("--once", action="store_true", help="run a single pass then exit")
    args = parser.parse_args()

    if args.once:
        run_once()
        return

    interval = env_int("POLL_INTERVAL_SECONDS", 86400)
    log.info("worldbank producer loop: one pass every %ds", interval)
    while True:
        try:
            run_once()
        except Exception:
            log.exception("worldbank pass failed; continuing")
        time.sleep(interval)


if __name__ == "__main__":
    main()
