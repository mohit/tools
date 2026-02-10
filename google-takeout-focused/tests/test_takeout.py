from __future__ import annotations

import json
from pathlib import Path

import duckdb

from google_takeout_focused.takeout import (
    LOCATION_COLUMNS,
    SEARCH_COLUMNS,
    build_analysis_report,
    merge_to_curated,
    parse_takeout,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_takeout(root: Path) -> None:
    _write_json(
        root / "Takeout/Location History (Timeline)/Records.json",
        {
            "locations": [
                {
                    "timestampMs": "1704067200000",
                    "latitudeE7": 377749000,
                    "longitudeE7": -1224194000,
                }
            ]
        },
    )

    _write_json(
        root / "Takeout/Location History/Semantic Location History/2024/2024_JANUARY.json",
        {
            "timelineObjects": [
                {
                    "placeVisit": {
                        "duration": {
                            "startTimestamp": "2024-01-01T10:00:00Z",
                            "endTimestamp": "2024-01-01T11:00:00Z",
                        },
                        "location": {
                            "name": "Office",
                            "placeId": "place-1",
                            "latitudeE7": 377800000,
                            "longitudeE7": -1224100000,
                        },
                    }
                }
            ]
        },
    )

    _write_json(
        root / "Takeout/My Activity/Search/MyActivity.json",
        [
            {
                "title": "Searched for duckdb parquet partition",
                "titleUrl": "https://www.google.com/search?q=duckdb+parquet+partition",
                "time": "2024-01-02T10:00:00Z",
            }
        ],
    )

    _write_json(
        root / "Takeout/YouTube and YouTube Music/history/watch-history.json",
        [
            {
                "header": "YouTube Music",
                "title": "Listened to Nils Frahm - Says",
                "titleUrl": "https://music.youtube.com/watch?v=abc",
                "time": "2024-01-03T12:00:00Z",
                "subtitles": [{"name": "Nils Frahm"}],
            }
        ],
    )


def test_parse_takeout_three_sources(tmp_path: Path) -> None:
    _seed_takeout(tmp_path)

    parsed = parse_takeout(tmp_path)

    assert len(parsed.location_events) == 2
    assert len(parsed.search_events) == 1
    assert len(parsed.music_events) == 1

    location_types = {row["event_type"] for row in parsed.location_events}
    assert "raw_point" in location_types
    assert "place_visit" in location_types

    assert parsed.search_events[0]["query"] == "duckdb parquet partition"
    assert parsed.music_events[0]["artist"] == "Nils Frahm"


def test_analysis_report_includes_ranges(tmp_path: Path) -> None:
    _seed_takeout(tmp_path)
    parsed = parse_takeout(tmp_path)

    report = build_analysis_report(parsed)

    assert report["location_timeline"]["events"] == 2
    assert report["search_history"]["events"] == 1
    assert report["youtube_music_history"]["events"] == 1
    assert report["location_timeline"]["first_event_utc"] is not None


def test_merge_to_curated_dedupes_on_event_id(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    records = [
        {
            "event_id": "search-1",
            "event_ts": "2024-01-02T10:00:00Z",
            "query": "duckdb parquet",
            "title": "Searched for duckdb parquet",
            "title_url": "https://www.google.com/search?q=duckdb+parquet",
            "source_file": "Takeout/My Activity/Search/MyActivity.json",
            "payload_json": "{}",
        }
    ]

    count1 = merge_to_curated(
        records=records,
        dataset_name="search_history",
        columns=SEARCH_COLUMNS,
        ts_column="event_ts",
        curated_root=curated_root,
    )
    assert count1 == 1

    records_second = [
        {
            "event_id": "search-1",
            "event_ts": "2024-01-02T10:00:00Z",
            "query": "duckdb parquet partitioning",
            "title": "Searched for duckdb parquet partitioning",
            "title_url": "https://www.google.com/search?q=duckdb+parquet+partitioning",
            "source_file": "Takeout/My Activity/Search/MyActivity.json",
            "payload_json": "{}",
        }
    ]

    count2 = merge_to_curated(
        records=records_second,
        dataset_name="search_history",
        columns=SEARCH_COLUMNS,
        ts_column="event_ts",
        curated_root=curated_root,
    )
    assert count2 == 1

    con = duckdb.connect()
    rows = con.execute(
        "select query from read_parquet(?)",
        [str(curated_root / "google/search_history/year=*/month=*/*.parquet")],
    ).fetchall()
    assert rows == [("duckdb parquet partitioning",)]


def test_location_columns_stable() -> None:
    assert LOCATION_COLUMNS[0] == "event_id"
    assert "event_ts" in LOCATION_COLUMNS
