import jsonschema
import responses

from ly_ingest import common, worldbank

GDP_ROW = {
    "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP (current US$)"},
    "country": {"id": "LY", "value": "Libya"},
    "countryiso3code": "LBY",
    "date": "2023",
    "value": 45678901234.5,
    "unit": "",
    "obs_status": "",
    "decimal": 1,
}

OIL_ROW = {
    "indicator": {"id": "NY.GDP.PETR.RT.ZS", "value": "Oil rents (% of GDP)"},
    "country": {"id": "LY", "value": "Libya"},
    "countryiso3code": "LBY",
    "date": "2022",
    "value": 42.7,
    "unit": "",
    "obs_status": "",
    "decimal": 1,
}

NULL_ROW = dict(GDP_ROW, date="2024", value=None)


def _wb_payload(*rows):
    return [{"page": 1, "pages": 1, "per_page": "100", "total": len(rows)}, list(rows)]


def test_map_row_validates_against_contract(event_schema):
    event = worldbank.map_row("NY.GDP.MKTP.CD", GDP_ROW)
    jsonschema.validate(event, event_schema)
    assert event["source"] == "worldbank"
    assert event["country"] == "LY"
    assert event["raw"] == GDP_ROW


def test_map_row_fields():
    event = worldbank.map_row("NY.GDP.MKTP.CD", GDP_ROW)
    assert event["event_id"] == "worldbank:NY.GDP.MKTP.CD:2023"
    assert event["source_id"] == "NY.GDP.MKTP.CD"
    assert event["event_time"] == "2023-01-01T00:00:00+00:00"
    assert event["title"] == "GDP (current US$) 2023"
    assert event["metrics"] == {
        "indicator": "NY.GDP.MKTP.CD",
        "value": 45678901234.5,
        "unit": "GDP (current US$)",
    }


def test_map_row_categories():
    assert worldbank.map_row("NY.GDP.PETR.RT.ZS", OIL_ROW)["category"] == "energy"
    assert worldbank.map_row("NY.GDP.MKTP.CD", GDP_ROW)["category"] == "economy"


def test_map_row_skips_null_values():
    assert worldbank.map_row("NY.GDP.MKTP.CD", NULL_ROW) is None


@responses.activate
def test_run_once_emits_per_indicator_year(publisher, reporter, monkeypatch):
    monkeypatch.setenv("WORLDBANK_INDICATORS", "NY.GDP.MKTP.CD,NY.GDP.PETR.RT.ZS")
    responses.add(
        responses.GET,
        f"{worldbank.BASE_URL}/NY.GDP.MKTP.CD",
        json=_wb_payload(GDP_ROW, NULL_ROW),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{worldbank.BASE_URL}/NY.GDP.PETR.RT.ZS",
        json=_wb_payload(OIL_ROW),
        status=200,
    )
    seen: set[str] = set()
    emitted = worldbank.run_once(
        session=common.make_session(), publisher=publisher, reporter=reporter, seen=seen
    )
    assert emitted == 2  # the null-valued row is skipped
    ids = {e["event_id"] for e in publisher.events}
    assert ids == {"worldbank:NY.GDP.MKTP.CD:2023", "worldbank:NY.GDP.PETR.RT.ZS:2022"}
    assert reporter.finished[-1][1] == 2
    # A repeat pass emits nothing: event_ids are already seen.
    assert worldbank.run_once(
        session=common.make_session(), publisher=publisher, reporter=reporter, seen=seen
    ) == 0
