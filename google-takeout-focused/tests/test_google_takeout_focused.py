from __future__ import annotations

import json
from pathlib import Path

from google_takeout_focused import (
    _extract_location_rows,
    _extract_music_rows,
    _extract_search_rows,
    _load_json_documents,
    _print_guide,
    _process_one_takeout,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_extracts_location_search_and_music_rows(tmp_path: Path) -> None:
    takeout_root = tmp_path / "Takeout"

    _write_json(
        takeout_root / "Location History (Timeline)" / "Records.json",
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
        takeout_root
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
                            "placeId": "abc123",
                            "latitudeE7": 377710000,
                            "longitudeE7": -1224300000,
                        },
                        "duration": {
                            "startTimestamp": "2024-01-10T10:00:00.000Z",
                            "endTimestamp": "2024-01-10T11:00:00.000Z",
                        },
                        "visitConfidence": 90,
                    }
                },
                {
                    "activitySegment": {
                        "activityType": "WALKING",
                        "distance": 1200,
                        "duration": {
                            "startTimestamp": "2024-01-10T09:40:00.000Z",
                            "endTimestamp": "2024-01-10T10:00:00.000Z",
                        },
                        "startLocation": {"latitudeE7": 377700000, "longitudeE7": -1224200000},
                        "endLocation": {"latitudeE7": 377710000, "longitudeE7": -1224300000},
                    }
                },
            ]
        },
    )

    _write_json(
        takeout_root / "My Activity" / "Search" / "MyActivity.json",
        [
            {
                "title": "Searched for hiking near me",
                "time": "2024-01-02T01:02:03.000Z",
                "products": ["Search"],
            }
        ],
    )

    _write_json(
        takeout_root / "My Activity" / "YouTube and YouTube Music" / "MyActivity.json",
        [
            {
                "header": "YouTube Music",
                "title": "Listened to Song A",
                "titleUrl": "https://music.youtube.com/watch?v=1",
                "subtitles": [{"name": "Artist A"}],
                "time": "2024-01-03T04:05:06.000Z",
            },
            {
                "header": "YouTube",
                "title": "Watched random video",
                "titleUrl": "https://www.youtube.com/watch?v=2",
                "time": "2024-01-03T05:05:06.000Z",
            },
        ],
    )

    docs = _load_json_documents(takeout_root)
    location_visits, location_routes = _extract_location_rows(docs)
    searches = _extract_search_rows(docs)
    music = _extract_music_rows(docs)

    assert len(location_visits) == 2
    assert len(location_routes) == 1
    assert len(searches) == 1
    assert searches[0]["query"] == "hiking near me"
    assert len(music) == 1
    assert music[0]["subtitle"] == "Artist A"


def test_process_writes_parquet_outputs(tmp_path: Path) -> None:
    takeout_root = tmp_path / "Takeout"
    curated_root = tmp_path / "curated"

    _write_json(
        takeout_root / "My Activity" / "Search" / "MyActivity.json",
        [
            {
                "title": "Searched for coffee beans",
                "time": "2024-02-01T01:02:03.000Z",
                "products": ["Search"],
            }
        ],
    )

    _write_json(
        takeout_root / "My Activity" / "YouTube and YouTube Music" / "MyActivity.json",
        [
            {
                "header": "YouTube Music",
                "title": "Listened to Song B",
                "titleUrl": "https://music.youtube.com/watch?v=3",
                "time": "2024-02-01T02:02:03.000Z",
            }
        ],
    )

    result = _process_one_takeout(takeout_root, curated_root)

    assert result["search_history"] == 1
    assert result["youtube_music_history"] == 1
    assert result["location_visits"] == 0

    search_files = list((curated_root / "google_takeout" / "search_history").rglob("*.parquet"))
    music_files = list((curated_root / "google_takeout" / "youtube_music_history").rglob("*.parquet"))

    assert len(search_files) == 1
    assert len(music_files) == 1


def test_event_ids_are_stable_across_different_input_roots(tmp_path: Path) -> None:
    inner_takeout = tmp_path / "nested" / "Takeout"

    _write_json(
        inner_takeout / "Location History (Timeline)" / "Records.json",
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
        inner_takeout
        / "Location History (Timeline)"
        / "Semantic Location History"
        / "2024"
        / "2024_JANUARY.json",
        {
            "timelineObjects": [
                {
                    "placeVisit": {
                        "location": {"name": "Coffee Shop", "latitudeE7": 377710000, "longitudeE7": -1224300000},
                        "duration": {
                            "startTimestamp": "2024-01-10T10:00:00.000Z",
                            "endTimestamp": "2024-01-10T11:00:00.000Z",
                        },
                    }
                }
            ]
        },
    )

    _write_json(
        inner_takeout / "My Activity" / "Search" / "MyActivity.json",
        [
            {
                "title": "Searched for pour over coffee",
                "time": "2024-01-04T04:05:06.000Z",
                "products": ["Search"],
            }
        ],
    )

    _write_json(
        inner_takeout / "My Activity" / "YouTube and YouTube Music" / "MyActivity.json",
        [
            {
                "header": "YouTube Music",
                "title": "Listened to Song C",
                "titleUrl": "https://music.youtube.com/watch?v=42",
                "time": "2024-01-03T04:05:06.000Z",
            }
        ],
    )

    docs_from_takeout = _load_json_documents(inner_takeout)
    visits_takeout, _ = _extract_location_rows(docs_from_takeout)
    searches_takeout = _extract_search_rows(docs_from_takeout)
    music_takeout = _extract_music_rows(docs_from_takeout)

    docs_from_parent = _load_json_documents(tmp_path / "nested")
    visits_parent, _ = _extract_location_rows(docs_from_parent)
    searches_parent = _extract_search_rows(docs_from_parent)
    music_parent = _extract_music_rows(docs_from_parent)

    assert {row["event_id"] for row in visits_takeout} == {row["event_id"] for row in visits_parent}
    assert {row["search_id"] for row in searches_takeout} == {row["search_id"] for row in searches_parent}
    assert {row["event_id"] for row in music_takeout} == {row["event_id"] for row in music_parent}
    assert {row["source_file"] for row in visits_takeout} == {row["source_file"] for row in visits_parent}


def test_ignores_non_scope_exports(tmp_path: Path) -> None:
    takeout_root = tmp_path / "Takeout"

    _write_json(
        takeout_root / "Mail" / "All mail Including Spam and Trash.mbox.json",
        [{"subject": "hello from gmail"}],
    )
    _write_json(
        takeout_root / "Chrome" / "BrowserHistory.json",
        [{"title": "Some visited website"}],
    )
    _write_json(
        takeout_root / "Drive" / "metadata.json",
        [{"name": "document"}],
    )

    docs = _load_json_documents(takeout_root)
    location_visits, location_routes = _extract_location_rows(docs)
    searches = _extract_search_rows(docs)
    music = _extract_music_rows(docs)

    assert len(location_visits) == 0
    assert len(location_routes) == 0
    assert len(searches) == 0
    assert len(music) == 0


def test_guide_mentions_scope_and_exclusions(capsys) -> None:
    _print_guide()
    output = capsys.readouterr().out

    assert "Location History (Timeline)" in output
    assert "Search" in output
    assert "YouTube and YouTube Music" in output
    assert "Workspace products disabled" in output
    assert "Chrome data disabled" in output
