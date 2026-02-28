from __future__ import annotations

import importlib.util
import json
import ast
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


def test_load_last_uts_from_raw_prefers_max_uts_and_supports_nested_uts(tmp_path: Path) -> None:
    raw_root = tmp_path / "lastfm"
    raw_root.mkdir(parents=True)

    file_a = raw_root / "recent_a.jsonl"
    file_b = raw_root / "legacy.jsonl"
    file_a.write_text(
        "\n".join(
            [
                json.dumps({"uts": 100}),
                json.dumps({"date": {"uts": "220"}}),
                "{bad json}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    file_b.write_text(json.dumps({"uts": 180}) + "\n", encoding="utf-8")

    assert lastfm_ingest.load_last_uts_from_raw(raw_root) == 220


def test_load_lastfm_user_returns_trimmed_value(monkeypatch) -> None:
    monkeypatch.setenv("LASTFM_USER", "env_user")
    assert lastfm_ingest.load_lastfm_user() == "env_user"


def test_load_lastfm_user_requires_value(monkeypatch) -> None:
    monkeypatch.delenv("LASTFM_USER", raising=False)
    expected = (
        "Missing required env var: LASTFM_USER. "
        f"{lastfm_ingest.LASTFM_USER_REQUIRED_HINT}"
    )
    with pytest.raises(
        SystemExit,
        match=expected,
    ):
        lastfm_ingest.load_lastfm_user()


def test_load_lastfm_user_rejects_whitespace_only_value(monkeypatch) -> None:
    monkeypatch.setenv("LASTFM_USER", "   ")
    expected = (
        "Missing required env var: LASTFM_USER. "
        f"{lastfm_ingest.LASTFM_USER_REQUIRED_HINT}"
    )
    with pytest.raises(
        SystemExit,
        match=expected,
    ):
        lastfm_ingest.load_lastfm_user()


def test_script_does_not_embed_hardcoded_lastfm_user_fallback() -> None:
    source = Path(lastfm_ingest.__file__).read_text(encoding="utf-8")
    assert "clakesnapster" not in source


def test_script_does_not_use_default_for_lastfm_user_env_lookup() -> None:
    source = Path(lastfm_ingest.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue

        is_os_getenv = (
            node.func.attr == "getenv"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
        )
        is_os_environ_get = (
            node.func.attr == "get"
            and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "environ"
            and isinstance(node.func.value.value, ast.Name)
            and node.func.value.value.id == "os"
        )
        if not is_os_getenv and not is_os_environ_get:
            continue
        if len(node.args) < 2:
            continue

        first_arg = node.args[0]
        if isinstance(first_arg, ast.Constant) and first_arg.value == "LASTFM_USER":
            raise AssertionError(
                "LASTFM_USER must not be read with a default value; require explicit env var."
            )


def test_main_requires_lastfm_user_and_fails_before_api_call(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("LASTFM_USER", raising=False)
    monkeypatch.setenv("LASTFM_API_KEY", "api_key")
    monkeypatch.setenv("DATALAKE_RAW_ROOT", str(tmp_path / "raw"))
    monkeypatch.setenv("DATALAKE_CURATED_ROOT", str(tmp_path / "curated"))

    monkeypatch.setattr(
        lastfm_ingest,
        "parse_args",
        lambda: lastfm_ingest.argparse.Namespace(
            from_uts=None,
            since=None,
            no_resume=False,
            max_pages=None,
            full_refresh=False,
        ),
    )

    call_counter = {"count": 0}

    def fake_request_recent_tracks(*args, **kwargs):
        call_counter["count"] += 1
        return {}

    monkeypatch.setattr(lastfm_ingest, "request_recent_tracks", fake_request_recent_tracks)

    expected = (
        "Missing required env var: LASTFM_USER. "
        f"{lastfm_ingest.LASTFM_USER_REQUIRED_HINT}"
    )
    with pytest.raises(
        SystemExit,
        match=expected,
    ):
        lastfm_ingest.main()
    assert call_counter["count"] == 0


def test_main_uses_lastfm_user_from_env_for_api_requests(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LASTFM_USER", "expected_user")
    monkeypatch.setenv("LASTFM_API_KEY", "api_key")
    monkeypatch.setenv("DATALAKE_RAW_ROOT", str(tmp_path / "raw"))
    monkeypatch.setenv("DATALAKE_CURATED_ROOT", str(tmp_path / "curated"))

    monkeypatch.setattr(
        lastfm_ingest,
        "parse_args",
        lambda: lastfm_ingest.argparse.Namespace(
            from_uts=None,
            since=None,
            no_resume=False,
            max_pages=None,
            full_refresh=False,
        ),
    )

    captured = {"user": None}

    def fake_request_recent_tracks(*, user, api_key, from_uts, page):
        captured["user"] = user
        return {"recenttracks": {"track": [], "@attr": {"totalPages": "1"}}}

    monkeypatch.setattr(lastfm_ingest, "request_recent_tracks", fake_request_recent_tracks)

    lastfm_ingest.main()
    assert captured["user"] == "expected_user"


def test_resolve_start_full_refresh_overrides_incremental_defaults() -> None:
    args = lastfm_ingest.argparse.Namespace(
        from_uts=None,
        since=None,
        no_resume=False,
        max_pages=None,
        full_refresh=True,
    )
    from_uts, page, _run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint={"from_uts": 123, "next_page": 2, "run_id": 99, "max_uts_seen": 123},
        fallback_from_uts=456,
    )
    assert from_uts is None
    assert page == 1
    assert max_uts_seen is None
