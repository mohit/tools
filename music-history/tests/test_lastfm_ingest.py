from __future__ import annotations

import importlib.util
import json
import os
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


def test_append_raw_page_jsonl_keeps_existing_raw_dump_immutable(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"

    first_rows = [{"uts": 1001, "artist": "A", "track": "T1", "album": None}]
    second_rows = [{"uts": 1002, "artist": "B", "track": "T2", "album": None}]

    written_1 = lastfm_ingest.append_raw_page_jsonl(
        rows=first_rows,
        raw_root=raw_root,
        run_id=1234,
        from_uts=1000,
        page=1,
    )
    written_2 = lastfm_ingest.append_raw_page_jsonl(
        rows=second_rows,
        raw_root=raw_root,
        run_id=1234,
        from_uts=1000,
        page=1,
    )

    assert written_1 == 1
    assert written_2 == 1
    assert _read_jsonl(raw_root / "scrobbles_run-1234_from-1000_p0001.jsonl") == first_rows
    assert _read_jsonl(raw_root / "scrobbles_run-1234_from-1000_p0001_a0001.jsonl") == second_rows


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


def test_determine_from_uts_prefers_older_unmarked_backfill_over_marked_newer_restart(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("9000")

    _write_jsonl(raw_root / "scrobbles_run-8001_from-5000_started.json", [{"run_id": 8001, "from_uts": 5000}])
    _write_jsonl(raw_root / "scrobbles_run-8001_from-5000_p0001.jsonl", [{"uts": 5001}])
    _write_jsonl(raw_root / "scrobbles_run-8002_from-full_p0001.jsonl", [{"uts": 1}])

    # The safest restart floor is full history (None), not marked from_uts=5000
    # and not state+1.
    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) is None


def test_infer_resume_from_raw_uses_first_missing_page_and_preserves_run(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-1001_from-1200_started.json", [{"run_id": 1001, "from_uts": 1200}])
    _write_jsonl(raw_root / "scrobbles_run-1001_from-1200_p0001.jsonl", [{"uts": 1201}])
    _write_jsonl(raw_root / "scrobbles_run-1001_from-1200_p0003.jsonl", [{"uts": 1203}])

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1200
    assert next_page == 2
    assert run_id == 1001
    assert max_uts_seen == 1203


def test_resolve_start_prefers_inferred_raw_resume_when_checkpoint_missing(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-1002_from-1100_started.json", [{"run_id": 1002, "from_uts": 1100}])
    _write_jsonl(raw_root / "scrobbles_run-1002_from-1100_p0001.jsonl", [{"uts": 1101}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=None,
        fallback_from_uts=9000,
        raw_root=raw_root,
    )

    assert from_uts == 1100
    assert page == 2
    assert run_id == 1002
    assert max_uts_seen == 1101


def test_resolve_start_prefers_unmarked_raw_resume_over_state_fallback(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-2001_from-1200_p0001.jsonl", [{"uts": 1201}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=None,
        fallback_from_uts=9001,
        raw_root=raw_root,
    )

    assert from_uts == 1200
    assert page == 2
    assert run_id == 2001
    assert max_uts_seen == 1201


def test_resolve_start_checkpoint_next_page_is_clamped_to_first_missing_raw_page(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-7001_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-7001_from-1000_p0003.jsonl", [{"uts": 1003}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )
    checkpoint = {
        "from_uts": 1000,
        "next_page": 999,  # stale/corrupt high watermark must not skip missing page 2
        "run_id": 7001,
        "max_uts_seen": 1003,
    }

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=9001,
        raw_root=raw_root,
    )

    assert from_uts == 1000
    assert page == 2
    assert run_id == 7001
    assert max_uts_seen == 1003


def test_resolve_start_checkpoint_bad_run_id_uses_latest_run_for_range(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-7101_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-7102_from-1000_p0002.jsonl", [{"uts": 1002}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )
    checkpoint = {
        "from_uts": 1000,
        "next_page": 3,
        "run_id": 9999,  # not present in raw files
        "max_uts_seen": None,
    }

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=9001,
        raw_root=raw_root,
    )

    assert from_uts == 1000
    assert page == 3
    assert run_id == 7102
    assert max_uts_seen == 1002


def test_resolve_start_ignores_checkpoint_without_matching_raw_pages(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-7201_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-7201_from-1000_p0003.jsonl", [{"uts": 1003}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )
    checkpoint = {
        "from_uts": 9000,  # stale/corrupt range with no matching raw page dumps
        "next_page": 999,
        "run_id": 9999,
        "max_uts_seen": None,
    }

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=9500,
        raw_root=raw_root,
    )

    assert from_uts == 1000
    assert page == 2
    assert run_id == 7201
    assert max_uts_seen == 1003


def test_resolve_start_prefers_older_raw_backfill_over_stale_checkpoint_range(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    # Unfinished full-history run (older/safer restart target).
    _write_jsonl(raw_root / "scrobbles_run-7301_from-full_p0001.jsonl", [{"uts": 1}])
    # Existing incremental range that happens to match a stale checkpoint.
    _write_jsonl(raw_root / "scrobbles_run-7302_from-2000_p0001.jsonl", [{"uts": 2001}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )
    checkpoint = {
        "from_uts": 2000,
        "next_page": 2,
        "run_id": 7302,
        "max_uts_seen": 2001,
    }

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=9999,
        raw_root=raw_root,
    )

    assert from_uts is None
    assert page == 2
    assert run_id == 7301
    assert max_uts_seen == 1


def test_resolve_start_drops_unsupported_checkpoint_when_no_raw_resume_exists(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )
    checkpoint = {
        "from_uts": 9000,  # stale/corrupt range with no matching raw page dumps
        "next_page": 999,
        "run_id": 7777,
        "max_uts_seen": 9999999,
    }

    from_uts, page, _run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=1234,
        raw_root=raw_root,
    )

    assert from_uts == 1234
    assert page == 1
    assert max_uts_seen is None


def test_infer_resume_from_raw_prefers_oldest_unmarked_run_to_avoid_backfill_skip(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-3001_from-9000_p0001.jsonl", [{"uts": 9001}])
    _write_jsonl(raw_root / "scrobbles_run-3002_from-1000_p0001.jsonl", [{"uts": 1001}])

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1000
    assert next_page == 2
    assert run_id == 3002
    assert max_uts_seen == 1001


def test_infer_resume_from_raw_uses_union_of_pages_for_same_from_uts(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"

    # First interrupted run wrote pages 1 and 3.
    _write_jsonl(raw_root / "scrobbles_run-5001_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-5001_from-1000_p0003.jsonl", [{"uts": 1003}])

    # Later restart without checkpoint started a new run_id and wrote page 4.
    _write_jsonl(raw_root / "scrobbles_run-5002_from-1000_p0004.jsonl", [{"uts": 1004}])

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1000
    assert next_page == 2
    assert run_id == 5002
    assert max_uts_seen == 1004


def test_infer_resume_from_raw_full_history_uses_first_missing_page(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"

    # Interrupted full-history run wrote pages 1 and 3.
    _write_jsonl(raw_root / "scrobbles_run-6001_from-full_p0001.jsonl", [{"uts": 1}])
    _write_jsonl(raw_root / "scrobbles_run-6001_from-full_p0003.jsonl", [{"uts": 3}])

    # Later restart wrote page 4 with a different run id.
    _write_jsonl(raw_root / "scrobbles_run-6002_from-full_p0004.jsonl", [{"uts": 4}])

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts is None
    assert next_page == 2
    assert run_id == 6002
    assert max_uts_seen == 4


def test_infer_resume_from_raw_does_not_resume_into_completed_run(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"

    # Incomplete run for this range (missing page 2).
    _write_jsonl(raw_root / "scrobbles_run-6101_from-1000_started.json", [{"run_id": 6101, "from_uts": 1000}])
    _write_jsonl(raw_root / "scrobbles_run-6101_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-6101_from-1000_p0003.jsonl", [{"uts": 1003}])

    # A newer completed run with page files should not become the resume run id.
    _write_jsonl(raw_root / "scrobbles_run-6102_from-1000_started.json", [{"run_id": 6102, "from_uts": 1000}])
    _write_jsonl(raw_root / "scrobbles_run-6102_from-1000_completed.json", [{"run_id": 6102, "from_uts": 1000}])
    completed_page = raw_root / "scrobbles_run-6102_from-1000_p0005.jsonl"
    _write_jsonl(completed_page, [{"uts": 1005}])

    # Ensure the completed run page looks newest by mtime.
    newest_time = completed_page.stat().st_mtime + 20
    os.utime(completed_page, (newest_time, newest_time))

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1000
    assert next_page == 2
    assert run_id == 6101
    assert max_uts_seen == 1005


def test_infer_resume_from_raw_prefers_latest_run_with_pages_not_marker_only(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-5101_from-1000_started.json", [{"run_id": 5101, "from_uts": 1000}])
    _write_jsonl(raw_root / "scrobbles_run-5101_from-1000_p0001.jsonl", [{"uts": 1001}])

    # Newer started marker without any page dump should not become resume run_id.
    _write_jsonl(raw_root / "scrobbles_run-5102_from-1000_started.json", [{"run_id": 5102, "from_uts": 1000}])

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1000
    assert next_page == 2
    assert run_id == 5101
    assert max_uts_seen == 1001


def test_infer_resume_from_raw_uses_latest_page_writer_not_max_run_id(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    older_run_page = raw_root / "scrobbles_run-9999_from-1000_p0001.jsonl"
    newer_run_page = raw_root / "scrobbles_run-1000_from-1000_p0002.jsonl"
    _write_jsonl(older_run_page, [{"uts": 1001}])
    _write_jsonl(newer_run_page, [{"uts": 1002}])

    older_mtime = older_run_page.stat().st_mtime
    os.utime(newer_run_page, (older_mtime + 10, older_mtime + 10))

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1000
    assert next_page == 3
    assert run_id == 1000
    assert max_uts_seen == 1002


def test_infer_resume_from_raw_does_not_treat_empty_or_invalid_page_files_as_complete(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-6001_from-1500_started.json", [{"run_id": 6001, "from_uts": 1500}])
    _write_jsonl(raw_root / "scrobbles_run-6001_from-1500_p0001.jsonl", [{"uts": 1501}])

    # Simulate crash-corrupted page file: path exists but has no valid JSONL rows.
    bad_page = raw_root / "scrobbles_run-6001_from-1500_p0002.jsonl"
    bad_page.parent.mkdir(parents=True, exist_ok=True)
    bad_page.write_text("{not-json}\n", encoding="utf-8")

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts == 1500
    assert next_page == 2
    assert run_id == 6001
    assert max_uts_seen == 1501


def test_infer_resume_from_raw_includes_legacy_unmarked_runs_with_marked_incomplete(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-4001_from-5000_started.json", [{"run_id": 4001, "from_uts": 5000}])
    _write_jsonl(raw_root / "scrobbles_run-4001_from-5000_p0001.jsonl", [{"uts": 5001}])
    _write_jsonl(raw_root / "scrobbles_run-4002_from-full_p0001.jsonl", [{"uts": 1}])

    inferred = lastfm_ingest.infer_resume_from_raw(raw_root=raw_root)

    assert inferred is not None
    from_uts, next_page, run_id, max_uts_seen = inferred
    assert from_uts is None
    assert next_page == 2
    assert run_id == 4002
    assert max_uts_seen == 1


def test_append_raw_page_jsonl_writes_immutable_page_file(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    updated = lastfm_ingest.append_raw_page_jsonl(
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
        run_id=12345,
        from_uts=1735689600,
        page=1,
    )

    assert updated == 1
    files = sorted(raw_root.glob("scrobbles_run-*_from-*_p*.jsonl"))
    assert len(files) == 1
    rows = _read_jsonl(files[0])
    assert len(rows) == 1


def test_append_raw_page_jsonl_preserves_page_row_order(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    updated = lastfm_ingest.append_raw_page_jsonl(
        rows=[
            {
                "uts": 1738454402,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454402, unit="s", utc=True),
                "artist": "C",
                "track": "Newest",
                "album": None,
                "mbid_track": None,
                "source": "lastfm",
            },
            {
                "uts": 1738454400,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454400, unit="s", utc=True),
                "artist": "A",
                "track": "Oldest",
                "album": None,
                "mbid_track": None,
                "source": "lastfm",
            },
            {
                "uts": 1738454401,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454401, unit="s", utc=True),
                "artist": "B",
                "track": "Middle",
                "album": None,
                "mbid_track": None,
                "source": "lastfm",
            },
        ],
        raw_root=raw_root,
        run_id=12345,
        from_uts=1735689600,
        page=5,
    )

    assert updated == 1
    files = sorted(raw_root.glob("scrobbles_run-*_from-*_p*.jsonl"))
    assert len(files) == 1
    rows = _read_jsonl(files[0])
    assert [row["uts"] for row in rows] == [1738454402, 1738454400, 1738454401]


def test_append_raw_page_jsonl_is_append_only_for_same_page_file(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    run_id = 12345
    from_uts = 1735689600
    page = 2

    updated_1 = lastfm_ingest.append_raw_page_jsonl(
        rows=[
            {
                "uts": 1738454400,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454400, unit="s", utc=True),
                "artist": "B",
                "track": "Feb",
                "album": None,
                "mbid_track": "old",
                "source": "lastfm",
            }
        ],
        raw_root=raw_root,
        run_id=run_id,
        from_uts=from_uts,
        page=page,
    )
    raw_file = raw_root / "scrobbles_run-12345_from-1735689600_p0002.jsonl"
    before = raw_file.read_text(encoding="utf-8")
    updated_2 = lastfm_ingest.append_raw_page_jsonl(
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
        run_id=run_id,
        from_uts=from_uts,
        page=page,
    )

    assert updated_1 == 1
    assert updated_2 == 1
    after = raw_file.read_text(encoding="utf-8")
    assert after == before

    files = sorted(raw_root.glob("scrobbles_run-12345_from-1735689600_p0002*.jsonl"))
    assert len(files) == 2

    first_rows = _read_jsonl(files[0])
    second_rows = _read_jsonl(files[1])
    assert len(first_rows) == 1
    assert len(second_rows) == 1
    assert first_rows[0]["mbid_track"] == "old"
    assert second_rows[0]["mbid_track"] == "new"

    tmp_files = list(raw_root.glob("*.tmp-*"))
    assert tmp_files == []


def test_append_raw_page_jsonl_does_not_rewrite_legacy_month_dump(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    legacy_month_file = raw_root / "scrobbles_2026-02.jsonl"
    _write_jsonl(legacy_month_file, [{"uts": 1738454400, "track": "legacy"}])
    legacy_before = legacy_month_file.read_text(encoding="utf-8")

    updated = lastfm_ingest.append_raw_page_jsonl(
        rows=[
            {
                "uts": 1738454401,
                "played_at_utc": lastfm_ingest.pd.to_datetime(1738454401, unit="s", utc=True),
                "artist": "B",
                "track": "new-page-row",
                "album": None,
                "mbid_track": "new",
                "source": "lastfm",
            }
        ],
        raw_root=raw_root,
        run_id=12345,
        from_uts=1735689600,
        page=1,
    )

    assert updated == 1
    assert legacy_month_file.read_text(encoding="utf-8") == legacy_before
    page_files = sorted(raw_root.glob("scrobbles_run-12345_from-1735689600_p0001*.jsonl"))
    assert len(page_files) == 1


def test_append_parquet_partitions_is_append_only_for_same_month_file(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    run_id = 3333
    page = 4
    row_old = {
        "uts": 1738454400,
        "played_at_utc": lastfm_ingest.pd.to_datetime(1738454400, unit="s", utc=True),
        "artist": "A",
        "track": "Song",
        "album": None,
        "mbid_track": "old",
        "source": "lastfm",
    }
    row_new = {
        "uts": 1738454401,
        "played_at_utc": lastfm_ingest.pd.to_datetime(1738454401, unit="s", utc=True),
        "artist": "A",
        "track": "Song 2",
        "album": None,
        "mbid_track": "new",
        "source": "lastfm",
    }

    written_1 = lastfm_ingest.append_parquet_partitions(
        curated_root=curated_root,
        run_id=run_id,
        page=page,
        rows=[row_old],
    )
    month_dir = curated_root / "year=2025" / "month=02"
    first_file = month_dir / "scrobbles_3333_p0004.parquet"
    before_rows = pq.read_table(first_file).to_pylist()

    written_2 = lastfm_ingest.append_parquet_partitions(
        curated_root=curated_root,
        run_id=run_id,
        page=page,
        rows=[row_new],
    )
    after_rows = pq.read_table(first_file).to_pylist()

    assert written_1 == 1
    assert written_2 == 1
    assert after_rows == before_rows

    files = sorted(month_dir.glob("scrobbles_3333_p0004*.parquet"))
    assert len(files) == 2
    assert files[1].name == "scrobbles_3333_p0004_a0001.parquet"
    tmp_files = list(month_dir.glob(".scrobbles_tmp_*.parquet"))
    assert tmp_files == []


def test_determine_from_uts_prefers_latest_raw_run_start_when_raw_newer_than_state(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("2000")

    state_mtime = state_file.stat().st_mtime
    raw_file = raw_root / "scrobbles_run-555_from-1000_p0001.jsonl"
    _write_jsonl(raw_file, [{"uts": 1000}])
    os.utime(raw_file, (state_mtime + 10, state_mtime + 10))

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 1000


def test_determine_from_uts_uses_safest_lower_bound_across_newer_runs(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("2500")
    state_mtime = state_file.stat().st_mtime

    higher_start = raw_root / "scrobbles_run-777_from-2200_p0001.jsonl"
    lower_start = raw_root / "scrobbles_run-778_from-1000_p0001.jsonl"
    _write_jsonl(higher_start, [{"uts": 2200}])
    _write_jsonl(lower_start, [{"uts": 1000}])
    os.utime(higher_start, (state_mtime + 5, state_mtime + 5))
    os.utime(lower_start, (state_mtime + 10, state_mtime + 10))

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 1000


def test_determine_from_uts_uses_unfinished_raw_backfill_even_when_state_is_newer(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("5000")

    raw_file = raw_root / "scrobbles_run-9901_from-1000_p0001.jsonl"
    _write_jsonl(raw_file, [{"uts": 1001}])

    # Simulate a crash-before-checkpoint scenario where raw page files can appear
    # older than state metadata (clock skew, restore, copied files, etc.).
    raw_mtime = raw_file.stat().st_mtime
    os.utime(state_file, (raw_mtime + 20, raw_mtime + 20))

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 1000


def test_resolve_start_no_checkpoint_uses_first_missing_page_not_max_plus_one(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-8201_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-8202_from-1000_p0003.jsonl", [{"uts": 1003}])
    _write_jsonl(raw_root / "scrobbles_run-8202_from-1000_p0004.jsonl", [{"uts": 1004}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=None,
        fallback_from_uts=9001,
        raw_root=raw_root,
    )

    assert from_uts == 1000
    assert page == 2
    assert run_id == 8202
    assert max_uts_seen == 1004


def test_resolve_start_corrupt_checkpoint_still_uses_first_missing_page_from_raw(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-8301_from-1000_p0001.jsonl", [{"uts": 1001}])
    _write_jsonl(raw_root / "scrobbles_run-8302_from-1000_p0003.jsonl", [{"uts": 1003}])
    _write_jsonl(raw_root / "scrobbles_run-8302_from-1000_p0004.jsonl", [{"uts": 1004}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )
    checkpoint = {
        "from_uts": "not-an-int",
        "next_page": "also-bad",
        "run_id": "bad",
        "max_uts_seen": None,
    }

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=9999,
        raw_root=raw_root,
    )

    assert from_uts == 1000
    assert page == 2
    assert run_id == 8302
    assert max_uts_seen == 1004


def test_resolve_start_missing_checkpoint_resumes_from_earliest_missing_page(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-8401_from-1000_p0002.jsonl", [{"uts": 1002}])
    _write_jsonl(raw_root / "scrobbles_run-8402_from-1000_p0003.jsonl", [{"uts": 1003}])

    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        full_refetch=False,
        no_resume=False,
    )

    from_uts, page, run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=None,
        fallback_from_uts=9999,
        raw_root=raw_root,
    )

    assert from_uts == 1000
    assert page == 1
    assert run_id == 8402
    assert max_uts_seen == 1003


def test_determine_from_uts_avoids_skipping_interrupted_full_run_without_state(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    _write_jsonl(raw_root / "scrobbles_run-100_from-5000_p0001.jsonl", [{"uts": 5000}])
    _write_jsonl(raw_root / "scrobbles_run-101_from-full_p0001.jsonl", [{"uts": 1}])
    state_file = tmp_path / "lastfm_last_uts.txt"

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) is None


def test_determine_from_uts_prefers_incomplete_marked_runs_over_state(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("5000")

    started = raw_root / "scrobbles_run-999_from-1200_started.json"
    _write_jsonl(started, [{"run_id": 999, "from_uts": 1200}])

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 1200


def test_determine_from_uts_ignores_completed_marked_runs(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("5000")

    started = raw_root / "scrobbles_run-1000_from-1200_started.json"
    completed = raw_root / "scrobbles_run-1000_from-1200_completed.json"
    _write_jsonl(started, [{"run_id": 1000, "from_uts": 1200}])
    _write_jsonl(completed, [{"run_id": 1000, "from_uts": 1200}])

    assert lastfm_ingest.determine_from_uts(raw_root=raw_root, state_file=state_file) == 5001


def test_write_run_marker_is_immutable(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"

    lastfm_ingest.write_run_marker(
        raw_root=raw_root,
        run_id=123,
        from_uts=1000,
        kind="started",
        payload={"value": "first"},
    )
    marker = raw_root / "scrobbles_run-123_from-1000_started.json"
    before = marker.read_text(encoding="utf-8")

    lastfm_ingest.write_run_marker(
        raw_root=raw_root,
        run_id=123,
        from_uts=1000,
        kind="started",
        payload={"value": "second"},
    )
    after = marker.read_text(encoding="utf-8")

    assert before == after
    assert "first" in after


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
