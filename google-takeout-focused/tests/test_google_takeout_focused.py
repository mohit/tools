from __future__ import annotations

import json
from pathlib import Path

from google_takeout_focused import (
    _extract_location_rows,
    _extract_music_rows,
    _extract_search_rows,
    _load_json_documents,
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
