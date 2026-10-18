import json
from pathlib import Path

import pytest

SCHEMA_PATH = (
    Path(__file__).resolve().parents[3] / "contracts" / "event.schema.json"
)


@pytest.fixture(scope="session")
def event_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.closed = False

    def send(self, event: dict) -> None:
        self.events.append(event)

    def close(self) -> None:
        self.closed = True


class FakeReporter:
    def __init__(self) -> None:
        self.finished: list[tuple] = []
        self.sources: list[tuple] = []

    def start(self, source: str) -> str:
        return "test-run-id"

    def finish(self, run_id, events_emitted: int, error=None) -> None:
        self.finished.append((run_id, events_emitted, error))

    def touch_source(self, source: str, kind: str, base_url: str) -> None:
        self.sources.append((source, kind, base_url))


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


@pytest.fixture
def reporter() -> FakeReporter:
    return FakeReporter()
