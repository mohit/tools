from __future__ import annotations

import json
from pathlib import Path

from location_pipeline.sources.google_takeout import _e7_to_float, _parse_ts_millis, load_google_takeout


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_e7_conversion() -> None:
    assert _e7_to_float(377699999) == 37.7699999


def test_parse_timestamp_ms() -> None:
    dt = _parse_ts_millis("1700000000000")
    assert dt is not None
    assert dt.year == 2023


def test_event_ids_stable_across_different_input_roots(tmp_path: Path) -> None:
    nested_takeout = tmp_path / "nested" / "Takeout"
    _write_json(
        nested_takeout / "Location History (Timeline)" / "Records.json",
        {
            "locations": [
                {
                    "timestampMs": "1700000000000",
                    "latitudeE7": 377700000,
                    "longitudeE7": -1224200000,
                }
            ]
        },
    )
    _write_json(
        nested_takeout
        / "Location History (Timeline)"
        / "Semantic Location History"
        / "2024"
        / "2024_JANUARY.json",
        {
            "timelineObjects": [
                {
                    "placeVisit": {
                        "location": {
                            "name": "Coffee Shop",
                            "latitudeE7": 377710000,
                            "longitudeE7": -1224300000,
                        },
                        "duration": {
                            "startTimestamp": "2024-01-10T10:00:00.000Z",
                            "endTimestamp": "2024-01-10T11:00:00.000Z",
                        },
                    }
                }
            ]
        },
    )

    raw_takeout, visits_takeout = load_google_takeout(str(nested_takeout))
    raw_parent, visits_parent = load_google_takeout(str(tmp_path / "nested"))

    assert {event.event_id for event in raw_takeout} == {event.event_id for event in raw_parent}
    assert {visit.visit_id for visit in visits_takeout} == {visit.visit_id for visit in visits_parent}
