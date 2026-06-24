import json
from pathlib import Path

import pandas as pd
from main import determine_from_uts, load_last_uts_from_raw, merge_raw_monthly_jsonl


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def test_load_last_uts_from_raw_reads_max_timestamp(tmp_path):
    raw_dir = tmp_path / "lastfm"
    _write_jsonl(
        raw_dir / "scrobbles_2024-01.jsonl",
        [
            {"uts": 100},
            {"date": {"uts": "150"}},
        ],
    )
    with (raw_dir / "bad.jsonl").open("w") as f:
        f.write("{not-json}\n")
        f.write(json.dumps({"uts": 149}) + "\n")

    assert load_last_uts_from_raw(raw_dir) == 150


def test_determine_from_uts_prefers_raw_over_state(tmp_path, monkeypatch):
    raw_dir = tmp_path / "lastfm"
    _write_jsonl(raw_dir / "scrobbles_2024-01.jsonl", [{"uts": 200}])

    monkeypatch.setattr("main.load_last_uts_from_state", lambda: 150)
    assert determine_from_uts(raw_dir) == 201


def test_determine_from_uts_state_fallback_no_double_offset(tmp_path, monkeypatch):
    """Regression test for the double-+1 bug.

    lastfm_ingest.py writes (max_uts_seen + 1) to the state file, so
    determine_from_uts() must NOT add another +1 when falling back to state;
    otherwise it requests from T+2 and permanently skips the scrobble at T+1.
    """
    # Empty raw dir so state fallback is exercised.
    raw_dir = tmp_path / "lastfm"
    raw_dir.mkdir()

    # State file contains T+1 as written by lastfm_ingest.py.
    stored_value = 1_700_000_001  # i.e. max_uts_seen=1_700_000_000, +1 already stored
    monkeypatch.setattr("main.load_last_uts_from_state", lambda: stored_value)

    result = determine_from_uts(raw_dir)
    assert result == stored_value, (
        f"determine_from_uts() returned {result}, expected {stored_value}. "
        "State already holds next-from_uts; adding +1 again causes a skipped scrobble."
    )


def test_merge_raw_monthly_jsonl_upserts_and_splits_months(tmp_path):
    raw_dir = tmp_path / "lastfm"
    jan_file = raw_dir / "scrobbles_2024-01.jsonl"
    _write_jsonl(
        jan_file,
        [
            {
                "uts": 1704067200,
                "artist": "A",
                "track": "Song",
                "album": None,
                "mbid_track": "old",
                "source": "lastfm",
            },
            {
                "uts": 1704153600,
                "artist": "A",
                "track": "Other",
                "album": None,
                "mbid_track": "keep",
                "source": "lastfm",
            },
        ],
    )

    merge_raw_monthly_jsonl(
        [
            {
                "uts": 1704067200,
                "played_at_utc": pd.to_datetime(1704067200, unit="s", utc=True),
                "artist": "A",
                "track": "Song",
                "album": None,
                "mbid_track": "new",
                "source": "lastfm",
            },
            {
                "uts": 1706745600,
                "played_at_utc": pd.to_datetime(1706745600, unit="s", utc=True),
                "artist": "B",
                "track": "Feb Song",
                "album": "Album",
                "mbid_track": None,
                "source": "lastfm",
            },
        ],
        raw_dir=raw_dir,
    )

    jan_rows = _read_jsonl(jan_file)
    assert len(jan_rows) == 2
    assert jan_rows[0]["uts"] == 1704067200
    assert jan_rows[0]["mbid_track"] == "new"
    assert jan_rows[1]["uts"] == 1704153600

    feb_rows = _read_jsonl(raw_dir / "scrobbles_2024-02.jsonl")
    assert len(feb_rows) == 1
    assert feb_rows[0]["track"] == "Feb Song"


def test_merge_raw_monthly_jsonl_handles_legacy_api_shaped_rows(tmp_path):
    raw_dir = tmp_path / "lastfm"
    jan_file = raw_dir / "scrobbles_2024-01.jsonl"
    _write_jsonl(
        jan_file,
        [
            {
                "date": {"uts": "1704067200"},
                "artist": {"#text": "A", "mbid": ""},
                "name": "Song",
                "album": {"#text": "", "mbid": ""},
            }
        ],
    )

    merge_raw_monthly_jsonl(
        [
            {
                "uts": 1704067200,
                "played_at_utc": pd.to_datetime(1704067200, unit="s", utc=True),
                "artist": "A",
                "track": "Song",
                "album": None,
                "mbid_track": "new",
                "source": "lastfm",
            },
        ],
        raw_dir=raw_dir,
    )

    jan_rows = _read_jsonl(jan_file)
    assert len(jan_rows) == 1
    assert jan_rows[0]["uts"] == 1704067200
    assert jan_rows[0]["track"] == "Song"
    assert jan_rows[0]["mbid_track"] == "new"


def test_merge_raw_monthly_jsonl_normalizes_dict_fields_in_existing_rows(tmp_path):
    raw_dir = tmp_path / "lastfm"
    jan_file = raw_dir / "scrobbles_2024-01.jsonl"
    _write_jsonl(
        jan_file,
        [
            {
                "uts": 1704067200,
                "artist": {"#text": "A", "mbid": ""},
                "track": {"#text": "Song"},
                "album": {"#text": ""},
            }
        ],
    )

    merge_raw_monthly_jsonl(
        [
            {
                "uts": 1704067200,
                "played_at_utc": pd.to_datetime(1704067200, unit="s", utc=True),
                "artist": "A",
                "track": "Song",
                "album": None,
                "mbid_track": "new",
                "source": "lastfm",
            },
        ],
        raw_dir=raw_dir,
    )

    jan_rows = _read_jsonl(jan_file)
    assert len(jan_rows) == 1
    assert jan_rows[0]["uts"] == 1704067200
    assert jan_rows[0]["track"] == "Song"
    assert jan_rows[0]["mbid_track"] == "new"
