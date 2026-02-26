from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "lastfm_ingest.py"
SPEC = importlib.util.spec_from_file_location("lastfm_ingest", MODULE_PATH)
assert SPEC and SPEC.loader
lastfm_ingest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lastfm_ingest)


def _row(uts: int, artist: str, track: str, album: str | None) -> dict[str, object]:
    return {
        "uts": uts,
        "artist": artist,
        "track": track,
        "album": album,
    }


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def _read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_load_checkpoint_returns_none_for_corrupt_json(tmp_path: Path) -> None:
    checkpoint_file = tmp_path / "lastfm_ingest_checkpoint.json"
    checkpoint_file.write_text("{bad json", encoding="utf-8")

    assert lastfm_ingest.load_checkpoint(checkpoint_file=checkpoint_file) is None


def test_dedupe_rows_removes_duplicates_across_pages() -> None:
    seen_keys: set[tuple[object, object, object, object]] = set()

    page_1 = [
        _row(100, "A", "T1", "AL1"),
        _row(101, "B", "T2", "AL2"),
    ]
    page_2 = [
        _row(101, "B", "T2", "AL2"),  # duplicate from previous page
        _row(102, "C", "T3", "AL3"),
    ]

    unique_1 = lastfm_ingest.dedupe_rows(page_1, seen_keys)
    unique_2 = lastfm_ingest.dedupe_rows(page_2, seen_keys)

    assert len(unique_1) == 2
    assert len(unique_2) == 1
    assert unique_2[0]["uts"] == 102


def test_load_seen_keys_for_run_handles_null_album_for_resume(tmp_path: Path) -> None:
    curated_root = tmp_path / "scrobbles"
    parquet_dir = curated_root / "year=2026" / "month=02"
    parquet_dir.mkdir(parents=True)
    parquet_file = parquet_dir / "scrobbles_12345_p0001.parquet"

    table = pa.Table.from_pylist([
        _row(200, "Artist", "Track", None),
    ])
    pq.write_table(table, parquet_file)

    seen_keys = lastfm_ingest.load_seen_keys_for_run(curated_root, run_id=12345)

    incoming = [
        _row(200, "Artist", "Track", None),  # duplicate already written pre-resume
        _row(201, "Artist", "Track 2", None),
    ]

    unique = lastfm_ingest.dedupe_rows(incoming, seen_keys)

    assert len(unique) == 1
    assert unique[0]["uts"] == 201


def test_dedupe_rows_before_append_dedupes_across_pages(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 555
    seen_keys: set[tuple[object, object, object, object]] = set()

    page_1 = [
        {
            "uts": 300,
            "played_at_utc": lastfm_ingest.pd.to_datetime(300, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 301,
            "played_at_utc": lastfm_ingest.pd.to_datetime(301, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
    ]
    page_2 = [
        {
            "uts": 301,
            "played_at_utc": lastfm_ingest.pd.to_datetime(301, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 302,
            "played_at_utc": lastfm_ingest.pd.to_datetime(302, unit="s", utc=True),
            "artist": "C",
            "track": "T3",
            "album": "AL3",
            "mbid_track": None,
            "source": "lastfm",
        },
    ]

    written_1 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=1,
        rows=page_1,
        seen_keys=seen_keys,
    )
    written_2 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=2,
        rows=page_2,
        seen_keys=seen_keys,
    )

    assert written_1 == 2
    assert written_2 == 1

    files = sorted(curated_root.rglob(f"scrobbles_{run_id}_p*.parquet"))
    assert len(files) == 2

    total_rows = sum(pq.read_table(path).num_rows for path in files)
    assert total_rows == 3


def test_append_parquet_partitions_dedupes_across_pages_with_shared_seen_keys(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 556
    seen_keys: set[tuple[object, object, object, object]] = set()

    page_1 = [
        {
            "uts": 310,
            "played_at_utc": lastfm_ingest.pd.to_datetime(310, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
    ]
    page_2 = [
        {
            "uts": 310,
            "played_at_utc": lastfm_ingest.pd.to_datetime(310, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 311,
            "played_at_utc": lastfm_ingest.pd.to_datetime(311, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
    ]

    written_1 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=1,
        rows=page_1,
        seen_keys=seen_keys,
    )
    written_2 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=2,
        rows=page_2,
        seen_keys=seen_keys,
    )

    assert written_1 == 1
    assert written_2 == 1

    files = sorted(curated_root.rglob(f"scrobbles_{run_id}_p*.parquet"))
    assert len(files) == 2
    total_rows = sum(pq.read_table(path).num_rows for path in files)
    assert total_rows == 2


def test_dedupe_rows_dedupes_within_page(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 777
    seen_keys: set[tuple[object, object, object, object]] = set()

    rows = [
        {
            "uts": 400,
            "played_at_utc": lastfm_ingest.pd.to_datetime(400, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 401,
            "played_at_utc": lastfm_ingest.pd.to_datetime(401, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 401,
            "played_at_utc": lastfm_ingest.pd.to_datetime(401, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
    ]

    written = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=1,
        rows=rows,
        seen_keys=seen_keys,
    )

    assert written == 2

    files = sorted(curated_root.rglob(f"scrobbles_{run_id}_p*.parquet"))
    total_rows = sum(pq.read_table(path).num_rows for path in files)
    assert total_rows == 2


def test_resume_loads_seen_keys_from_run_files(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 888

    first_page_rows = [
        {
            "uts": 500,
            "played_at_utc": lastfm_ingest.pd.to_datetime(500, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
    ]
    resumed_page_rows = [
        {
            "uts": 500,
            "played_at_utc": lastfm_ingest.pd.to_datetime(500, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 501,
            "played_at_utc": lastfm_ingest.pd.to_datetime(501, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
    ]

    seen_keys = lastfm_ingest.load_seen_keys_for_run(curated_root, run_id=run_id)
    written_1 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=1,
        rows=first_page_rows,
        seen_keys=seen_keys,
    )
    assert written_1 == 1

    reloaded_seen_keys = lastfm_ingest.load_seen_keys_for_run(curated_root, run_id=run_id)

    written_2 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=2,
        rows=resumed_page_rows,
        seen_keys=reloaded_seen_keys,
    )
    assert written_2 == 1

    files = sorted(curated_root.rglob(f"scrobbles_{run_id}_p*.parquet"))
    total_rows = sum(pq.read_table(path).num_rows for path in files)
    assert total_rows == 2


def test_dedupe_rows_when_seen_keys_instance_changes(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 889

    page_1 = [
        {
            "uts": 600,
            "played_at_utc": lastfm_ingest.pd.to_datetime(600, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
    ]
    page_2 = [
        {
            "uts": 600,
            "played_at_utc": lastfm_ingest.pd.to_datetime(600, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 601,
            "played_at_utc": lastfm_ingest.pd.to_datetime(601, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
    ]

    seen_keys_1: set[tuple[object, object, object, object]] = set()
    written_1 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=1,
        rows=page_1,
        seen_keys=seen_keys_1,
    )
    seen_keys_2 = lastfm_ingest.load_seen_keys_for_run(curated_root, run_id=run_id)
    written_2 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=2,
        rows=page_2,
        seen_keys=seen_keys_2,
    )

    assert written_1 == 1
    assert written_2 == 1

    files = sorted(curated_root.rglob(f"scrobbles_{run_id}_p*.parquet"))
    total_rows = sum(pq.read_table(path).num_rows for path in files)
    assert total_rows == 2


def test_append_parquet_partitions_dedupes_across_pages_without_shared_seen_keys(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 990

    page_1 = [
        {
            "uts": 700,
            "played_at_utc": lastfm_ingest.pd.to_datetime(700, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
    ]
    page_2 = [
        {
            "uts": 700,
            "played_at_utc": lastfm_ingest.pd.to_datetime(700, unit="s", utc=True),
            "artist": "A",
            "track": "T1",
            "album": "AL1",
            "mbid_track": None,
            "source": "lastfm",
        },
        {
            "uts": 701,
            "played_at_utc": lastfm_ingest.pd.to_datetime(701, unit="s", utc=True),
            "artist": "B",
            "track": "T2",
            "album": None,
            "mbid_track": None,
            "source": "lastfm",
        },
    ]

    written_1 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=1,
        rows=page_1,
    )
    written_2 = lastfm_ingest.append_parquet_partitions(
        curated_root,
        run_id=run_id,
        page=2,
        rows=page_2,
    )

    assert written_1 == 1
    assert written_2 == 1

    files = sorted(curated_root.rglob(f"scrobbles_{run_id}_p*.parquet"))
    total_rows = sum(pq.read_table(path).num_rows for path in files)
    assert total_rows == 2


def test_determine_from_uts_prefers_state_over_raw(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_2026-01.jsonl", [{"uts": 1000}])
    _write_jsonl(raw_root / "scrobbles_2026-02.jsonl", [{"date": {"uts": "1005"}}])
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("1003")

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 1004


def test_determine_from_uts_falls_back_to_state_when_raw_missing(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("2000")

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 2001


def test_determine_from_uts_returns_none_when_state_missing_and_raw_exists(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_2026-02.jsonl", [{"uts": 2010}])
    state_file = tmp_path / "lastfm_last_uts.txt"

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) is None


def test_merge_raw_monthly_jsonl_only_updates_impacted_month(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    jan_file = raw_root / "scrobbles_2026-01.jsonl"
    feb_file = raw_root / "scrobbles_2026-02.jsonl"
    _write_jsonl(jan_file, [{"uts": 1735776000, "artist": "A", "track": "Jan", "album": None}])
    _write_jsonl(feb_file, [{"uts": 1738454400, "artist": "B", "track": "Feb", "album": None}])

    jan_before = jan_file.read_text(encoding="utf-8")
    updated = lastfm_ingest.merge_raw_monthly_jsonl(
        rows=[
            {
                "uts": 1738454401,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454401, unit="s", utc=True),
                "artist": "C",
                "track": "Feb New",
                "album": None,
                "mbid_track": None,
                "source": "lastfm",
            }
        ],
        raw_root=raw_root,
    )

    assert updated == 1
    assert jan_file.read_text(encoding="utf-8") == jan_before
    feb_rows = _read_jsonl(feb_file)
    assert len(feb_rows) == 2


def test_merge_raw_monthly_jsonl_is_append_only(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    feb_file = raw_root / "scrobbles_2026-02.jsonl"
    _write_jsonl(
        feb_file,
        [
            {"uts": 1738454400, "artist": "B", "track": "Feb", "album": None, "mbid_track": "old"},
        ],
    )
    before = feb_file.read_text(encoding="utf-8")

    updated = lastfm_ingest.merge_raw_monthly_jsonl(
        rows=[
            {
                "uts": 1738454400,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454400, unit="s", utc=True),
                "artist": "B",
                "track": "Feb",
                "album": None,
                "mbid_track": "new",
                "source": "lastfm",
            }
        ],
        raw_root=raw_root,
    )

    assert updated == 1
    after = feb_file.read_text(encoding="utf-8")
    assert after.startswith(before)
    feb_rows = _read_jsonl(feb_file)
    assert len(feb_rows) == 2
    assert feb_rows[0]["mbid_track"] == "old"
    assert feb_rows[1]["mbid_track"] == "new"


def test_resolve_user_prefers_explicit_then_env_then_default(monkeypatch) -> None:
    monkeypatch.delenv("LASTFM_USER", raising=False)
    assert lastfm_ingest.resolve_user(explicit_user="explicit", default_user="fallback") == "explicit"

    monkeypatch.setenv("LASTFM_USER", "from-env")
    assert lastfm_ingest.resolve_user(explicit_user=None, default_user="fallback") == "from-env"

    monkeypatch.delenv("LASTFM_USER", raising=False)
    assert lastfm_ingest.resolve_user(explicit_user=None, default_user="fallback") == "fallback"


def test_resolve_user_errors_when_no_source(monkeypatch) -> None:
    monkeypatch.delenv("LASTFM_USER", raising=False)
    try:
        lastfm_ingest.resolve_user(explicit_user=None, default_user=None)
        assert False, "Expected SystemExit when user is unavailable"
    except SystemExit as exc:
        assert "Missing Last.fm user" in str(exc)
