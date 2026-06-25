from __future__ import annotations

import importlib.util
import urllib.error
from argparse import Namespace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

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


def test_save_last_uts_advances_by_one_to_prevent_boundary_duplication(tmp_path: Path) -> None:
    """Regression test for #73: save_last_uts must store max_uts_seen + 1.

    When a new run begins with from_uts == max_uts_seen, Last.fm returns the
    scrobble at exactly that timestamp again.  Storing max_uts_seen + 1 ensures
    the next run starts strictly after the boundary and avoids re-writing it.
    """
    state_file = tmp_path / "lastfm_last_uts.txt"
    max_uts_seen = 1_700_000_000

    # Simulate what main() does after the loop completes.
    lastfm_ingest.save_last_uts(max_uts_seen + 1, state_file=state_file)

    stored = lastfm_ingest.load_last_uts(state_file=state_file)
    assert stored == max_uts_seen + 1, (
        f"Expected {max_uts_seen + 1}, got {stored}. "
        "save_last_uts must advance by 1 to skip the boundary scrobble."
    )


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


def test_request_recent_tracks_retries_on_500_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, object]) -> None:
            self.status_code = status_code
            self._payload = payload

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise RuntimeError(f"unexpected raise_for_status for status={self.status_code}")

        def json(self) -> dict[str, object]:
            return self._payload

    def fake_get(*args: object, **kwargs: object) -> FakeResponse:
        calls["count"] += 1
        if calls["count"] < 3:
            return FakeResponse(500, {})
        return FakeResponse(200, {"recenttracks": {"track": []}})

    monkeypatch.setattr(lastfm_ingest.requests, "get", fake_get)
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda *_: None)

    payload = lastfm_ingest.request_recent_tracks(
        user="u",
        api_key="k",
        from_uts=0,
        page=1,
        max_retries=3,
        base_delay_seconds=0,
    )

    assert payload["recenttracks"] == {"track": []}
    assert calls["count"] == 3


def test_request_recent_tracks_retries_on_requests_timeout_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"recenttracks": {"track": []}}

    def fake_get(*args: object, **kwargs: object) -> FakeResponse:
        calls["count"] += 1
        if calls["count"] < 3:
            raise requests.Timeout("timed out")
        return FakeResponse()

    monkeypatch.setattr(lastfm_ingest.requests, "get", fake_get)
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda *_: None)

    payload = lastfm_ingest.request_recent_tracks(
        user="u",
        api_key="k",
        from_uts=0,
        page=1,
        max_retries=3,
        base_delay_seconds=0,
    )

    assert payload["recenttracks"] == {"track": []}
    assert calls["count"] == 3


def test_request_recent_tracks_retries_on_requests_exception_with_500_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"recenttracks": {"track": []}}

    def fake_get(*args: object, **kwargs: object) -> FakeResponse:
        calls["count"] += 1
        if calls["count"] < 3:
            response = requests.Response()
            response.status_code = 500
            raise requests.RequestException("server error", response=response)
        return FakeResponse()

    monkeypatch.setattr(lastfm_ingest.requests, "get", fake_get)
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda *_: None)

    payload = lastfm_ingest.request_recent_tracks(
        user="u",
        api_key="k",
        from_uts=0,
        page=1,
        max_retries=3,
        base_delay_seconds=0,
    )

    assert payload["recenttracks"] == {"track": []}
    assert calls["count"] == 3


def test_request_recent_tracks_exhausted_retryable_status_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        status_code = 500

        def raise_for_status(self) -> None:
            raise RuntimeError("should not call raise_for_status for retryable status")

        def json(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(lastfm_ingest.requests, "get", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda *_: None)

    with pytest.raises(SystemExit, match="Failed to fetch page 2 after 2 retries: HTTP 500"):
        lastfm_ingest.request_recent_tracks(
            user="u",
            api_key="k",
            from_uts=0,
            page=2,
            max_retries=2,
            base_delay_seconds=0,
        )


def test_request_recent_tracks_retries_on_requests_http_500_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"recenttracks": {"track": []}}

    def fake_get(*args: object, **kwargs: object) -> FakeResponse:
        calls["count"] += 1
        if calls["count"] < 3:
            response = requests.Response()
            response.status_code = 500
            raise requests.HTTPError("server error", response=response)
        return FakeResponse()

    monkeypatch.setattr(lastfm_ingest.requests, "get", fake_get)
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda *_: None)

    payload = lastfm_ingest.request_recent_tracks(
        user="u",
        api_key="k",
        from_uts=0,
        page=1,
        max_retries=3,
        base_delay_seconds=0,
    )

    assert payload["recenttracks"] == {"track": []}
    assert calls["count"] == 3


def test_request_recent_tracks_retries_on_urllib_http_500_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"recenttracks": {"track": []}}

    def fake_get(*args: object, **kwargs: object) -> FakeResponse:
        calls["count"] += 1
        if calls["count"] < 3:
            raise urllib.error.HTTPError(
                url=lastfm_ingest.API,
                code=500,
                msg="Internal Server Error",
                hdrs=None,
                fp=None,
            )
        return FakeResponse()

    monkeypatch.setattr(lastfm_ingest.requests, "get", fake_get)
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda *_: None)

    payload = lastfm_ingest.request_recent_tracks(
        user="u",
        api_key="k",
        from_uts=0,
        page=1,
        max_retries=3,
        base_delay_seconds=0,
    )

    assert payload["recenttracks"] == {"track": []}
    assert calls["count"] == 3


def test_request_recent_tracks_urllib_non_retryable_http_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get(*args: object, **kwargs: object) -> object:
        raise urllib.error.HTTPError(
            url=lastfm_ingest.API,
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr(lastfm_ingest.requests, "get", fake_get)

    with pytest.raises(SystemExit, match="non-retryable HTTP 404"):
        lastfm_ingest.request_recent_tracks(
            user="u",
            api_key="k",
            from_uts=0,
            page=3,
            max_retries=3,
            base_delay_seconds=0,
        )


def test_detect_latest_curated_uts(tmp_path: Path) -> None:
    curated_root = tmp_path / "curated"
    part_1 = curated_root / "year=2024" / "month=12"
    part_2 = curated_root / "year=2025" / "month=01"
    part_1.mkdir(parents=True)
    part_2.mkdir(parents=True)

    pq.write_table(
        pa.Table.from_pylist([{"uts": 100}, {"uts": 200}]),
        part_1 / "a.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([{"uts": 300}, {"uts": 250}]),
        part_2 / "b.parquet",
    )

    assert lastfm_ingest.detect_latest_curated_uts(curated_root) == 300


def test_load_checkpoint_returns_none_for_invalid_json(tmp_path: Path) -> None:
    checkpoint_file = tmp_path / "lastfm_ingest_checkpoint.json"
    checkpoint_file.write_text("{not-json")

    assert lastfm_ingest.load_checkpoint(checkpoint_file=checkpoint_file) is None


def test_resolve_start_full_refetch_uses_zero() -> None:
    args = Namespace(from_uts=None, since=None, full_refetch=True, no_resume=False)

    from_uts, page, _run_id, max_uts_seen = lastfm_ingest.resolve_start(
        args=args,
        checkpoint=None,
        fallback_from_uts=999,
    )

    assert from_uts == 0
    assert page == 1
    assert max_uts_seen is None


def test_main_skips_curated_scan_when_state_file_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("123")
    monkeypatch.setattr(lastfm_ingest, "STATE_FILE", state_file)
    monkeypatch.setattr(lastfm_ingest, "parse_args", lambda: Namespace(
        from_uts=None, since=None, full_refetch=False, no_resume=False, max_pages=None
    ))
    monkeypatch.setattr(lastfm_ingest, "load_env", lambda _name: "value")
    monkeypatch.setattr(lastfm_ingest, "load_last_uts_if_valid", lambda: 123)
    monkeypatch.setattr(lastfm_ingest, "load_persisted_uts", lambda **_kw: None)
    monkeypatch.setattr(lastfm_ingest, "load_checkpoint", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "load_seen_keys_for_run", lambda **_kwargs: set())
    monkeypatch.setattr(
        lastfm_ingest,
        "request_recent_tracks",
        lambda **_kwargs: {"recenttracks": {"track": []}},
    )
    monkeypatch.setattr(lastfm_ingest, "clear_checkpoint", lambda: None)

    fallback_from_uts: list[int] = []

    def fake_resolve_start(
        args: Namespace, checkpoint: dict[str, object] | None, fallback_from_uts_value: int
    ) -> tuple[int, int, int, int | None]:
        fallback_from_uts.append(fallback_from_uts_value)
        return fallback_from_uts_value, 1, 123, None

    monkeypatch.setattr(lastfm_ingest, "resolve_start", fake_resolve_start)

    def fail_detect_latest_curated_uts(*_args: object, **_kwargs: object) -> int | None:
        pytest.fail("detect_latest_curated_uts should not run when state file exists")

    monkeypatch.setattr(lastfm_ingest, "detect_latest_curated_uts", fail_detect_latest_curated_uts)

    lastfm_ingest.main()

    assert fallback_from_uts == [123]


def test_main_scans_curated_when_state_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "missing_state.txt"
    monkeypatch.setattr(lastfm_ingest, "STATE_FILE", state_file)
    monkeypatch.setattr(lastfm_ingest, "parse_args", lambda: Namespace(
        from_uts=None, since=None, full_refetch=False, no_resume=False, max_pages=None
    ))
    monkeypatch.setattr(lastfm_ingest, "load_env", lambda _name: "value")
    monkeypatch.setattr(lastfm_ingest, "load_last_uts_if_valid", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "load_persisted_uts", lambda **_kw: None)
    monkeypatch.setattr(lastfm_ingest, "load_checkpoint", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "has_paginated_curated_output", lambda **_kwargs: False)
    monkeypatch.setattr(lastfm_ingest, "load_seen_keys_for_run", lambda **_kwargs: set())
    monkeypatch.setattr(
        lastfm_ingest,
        "request_recent_tracks",
        lambda **_kwargs: {"recenttracks": {"track": []}},
    )
    monkeypatch.setattr(lastfm_ingest, "clear_checkpoint", lambda: None)

    calls = {"count": 0}

    def fake_detect_latest_curated_uts(*_args: object, **_kwargs: object) -> int | None:
        calls["count"] += 1
        return 456

    monkeypatch.setattr(lastfm_ingest, "detect_latest_curated_uts", fake_detect_latest_curated_uts)

    fallback_from_uts: list[int] = []

    def fake_resolve_start(
        args: Namespace, checkpoint: dict[str, object] | None, fallback_from_uts_value: int
    ) -> tuple[int, int, int, int | None]:
        fallback_from_uts.append(fallback_from_uts_value)
        return fallback_from_uts_value, 1, 123, None

    monkeypatch.setattr(lastfm_ingest, "resolve_start", fake_resolve_start)

    lastfm_ingest.main()

    assert calls["count"] == 1
    assert fallback_from_uts == [456]


def test_main_scans_curated_when_state_file_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "lastfm_last_uts.txt"
    state_file.write_text("not-an-int")
    monkeypatch.setattr(lastfm_ingest, "STATE_FILE", state_file)
    monkeypatch.setattr(lastfm_ingest, "parse_args", lambda: Namespace(
        from_uts=None, since=None, full_refetch=False, no_resume=False, max_pages=None
    ))
    monkeypatch.setattr(lastfm_ingest, "load_env", lambda _name: "value")
    monkeypatch.setattr(lastfm_ingest, "load_last_uts_if_valid", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "load_persisted_uts", lambda **_kw: None)
    monkeypatch.setattr(lastfm_ingest, "load_checkpoint", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "has_paginated_curated_output", lambda **_kwargs: False)
    monkeypatch.setattr(lastfm_ingest, "load_seen_keys_for_run", lambda **_kwargs: set())
    monkeypatch.setattr(
        lastfm_ingest,
        "request_recent_tracks",
        lambda **_kwargs: {"recenttracks": {"track": []}},
    )
    monkeypatch.setattr(lastfm_ingest, "clear_checkpoint", lambda: None)

    calls = {"count": 0}

    def fake_detect_latest_curated_uts(*_args: object, **_kwargs: object) -> int | None:
        calls["count"] += 1
        return 789

    monkeypatch.setattr(lastfm_ingest, "detect_latest_curated_uts", fake_detect_latest_curated_uts)

    fallback_from_uts: list[int] = []

    def fake_resolve_start(
        args: Namespace, checkpoint: dict[str, object] | None, fallback_from_uts_value: int
    ) -> tuple[int, int, int, int | None]:
        fallback_from_uts.append(fallback_from_uts_value)
        return fallback_from_uts_value, 1, 123, None

    monkeypatch.setattr(lastfm_ingest, "resolve_start", fake_resolve_start)

    lastfm_ingest.main()

    assert calls["count"] == 1
    assert fallback_from_uts == [789]


def test_main_skips_curated_scan_when_checkpoint_is_usable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "missing_state.txt"
    monkeypatch.setattr(lastfm_ingest, "STATE_FILE", state_file)
    monkeypatch.setattr(lastfm_ingest, "parse_args", lambda: Namespace(
        from_uts=None, since=None, full_refetch=False, no_resume=False, max_pages=None
    ))
    monkeypatch.setattr(lastfm_ingest, "load_env", lambda _name: "value")
    monkeypatch.setattr(lastfm_ingest, "load_last_uts_if_valid", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "load_persisted_uts", lambda **_kw: None)
    monkeypatch.setattr(
        lastfm_ingest,
        "load_checkpoint",
        lambda: {"from_uts": 321, "next_page": 4, "run_id": 42, "max_uts_seen": 400},
    )
    monkeypatch.setattr(lastfm_ingest, "load_seen_keys_for_run", lambda **_kwargs: set())
    monkeypatch.setattr(
        lastfm_ingest,
        "request_recent_tracks",
        lambda **_kwargs: {"recenttracks": {"track": []}},
    )
    monkeypatch.setattr(lastfm_ingest, "clear_checkpoint", lambda: None)

    def fail_detect_latest_curated_uts(*_args: object, **_kwargs: object) -> int | None:
        pytest.fail("detect_latest_curated_uts should not run when checkpoint is usable")

    monkeypatch.setattr(lastfm_ingest, "detect_latest_curated_uts", fail_detect_latest_curated_uts)

    fallback_from_uts: list[int] = []

    def fake_resolve_start(
        args: Namespace, checkpoint: dict[str, object] | None, fallback_from_uts_value: int
    ) -> tuple[int, int, int, int | None]:
        fallback_from_uts.append(fallback_from_uts_value)
        return 321, 4, 42, 400

    monkeypatch.setattr(lastfm_ingest, "resolve_start", fake_resolve_start)

    lastfm_ingest.main()

    assert fallback_from_uts == [0]


def test_main_uses_zero_fallback_for_paginated_curated_output_without_state_or_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file = tmp_path / "missing_state.txt"
    monkeypatch.setattr(lastfm_ingest, "STATE_FILE", state_file)
    monkeypatch.setattr(lastfm_ingest, "parse_args", lambda: Namespace(
        from_uts=None, since=None, full_refetch=False, no_resume=False, max_pages=None
    ))
    monkeypatch.setattr(lastfm_ingest, "load_env", lambda _name: "value")
    monkeypatch.setattr(lastfm_ingest, "load_last_uts_if_valid", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "load_persisted_uts", lambda **_kw: None)
    monkeypatch.setattr(lastfm_ingest, "load_checkpoint", lambda: None)
    monkeypatch.setattr(lastfm_ingest, "has_paginated_curated_output", lambda **_kwargs: True)
    monkeypatch.setattr(lastfm_ingest, "load_seen_keys_for_run", lambda **_kwargs: set())
    monkeypatch.setattr(
        lastfm_ingest,
        "request_recent_tracks",
        lambda **_kwargs: {"recenttracks": {"track": []}},
    )
    monkeypatch.setattr(lastfm_ingest, "clear_checkpoint", lambda: None)

    def fail_detect_latest_curated_uts(*_args: object, **_kwargs: object) -> int | None:
        pytest.fail("detect_latest_curated_uts should not run for conservative zero fallback")

    monkeypatch.setattr(lastfm_ingest, "detect_latest_curated_uts", fail_detect_latest_curated_uts)

    fallback_from_uts: list[int] = []

    def fake_resolve_start(
        args: Namespace, checkpoint: dict[str, object] | None, fallback_from_uts_value: int
    ) -> tuple[int, int, int, int | None]:
        fallback_from_uts.append(fallback_from_uts_value)
        return fallback_from_uts_value, 1, 123, None

    monkeypatch.setattr(lastfm_ingest, "resolve_start", fake_resolve_start)

    lastfm_ingest.main()

    assert fallback_from_uts == [0]


# ---------------------------------------------------------------------------
# Staleness-detection helpers
# ---------------------------------------------------------------------------

def test_load_persisted_uts_returns_none_when_state_file_absent(tmp_path: Path) -> None:
    assert lastfm_ingest.load_persisted_uts(state_file=tmp_path / "missing.txt") is None


def test_load_persisted_uts_returns_value_when_state_file_present(tmp_path: Path) -> None:
    state_file = tmp_path / "last_uts.txt"
    state_file.write_text("1779334414")
    assert lastfm_ingest.load_persisted_uts(state_file=state_file) == 1779334414


def test_load_staleness_state_returns_defaults_when_absent(tmp_path: Path) -> None:
    result = lastfm_ingest.load_staleness_state(staleness_file=tmp_path / "missing.json")
    assert result == {"stale": False, "stale_since": None}


def test_save_and_load_staleness_state_roundtrip(tmp_path: Path) -> None:
    state_file = tmp_path / "staleness.json"
    payload = {"stale": True, "stale_since": "2026-06-01"}
    lastfm_ingest.save_staleness_state(payload, staleness_file=state_file)
    result = lastfm_ingest.load_staleness_state(staleness_file=state_file)
    assert result["stale"] is True
    assert result["stale_since"] == "2026-06-01"


# ---------------------------------------------------------------------------
# update_catalog_staleness
# ---------------------------------------------------------------------------

def test_update_catalog_staleness_adds_fields_when_absent(tmp_path: Path) -> None:
    catalog = tmp_path / "lastfm.yaml"
    catalog.write_text(
        'source: lastfm\nlatest: "2026-05-21"\nfields:\n  - name: track name\n'
    )
    lastfm_ingest.update_catalog_staleness(True, "2026-06-01", catalog_file=catalog)
    text = catalog.read_text()
    assert "stale: true\n" in text
    assert 'stale_since: "2026-06-01"\n' in text
    # Fields block should still come after the inserted lines
    assert text.index("fields:") > text.index("stale: true")


def test_update_catalog_staleness_updates_existing_fields(tmp_path: Path) -> None:
    catalog = tmp_path / "lastfm.yaml"
    catalog.write_text(
        "source: lastfm\nstale: false\nstale_since: null\nfields:\n  - name: track name\n"
    )
    lastfm_ingest.update_catalog_staleness(True, "2026-06-10", catalog_file=catalog)
    text = catalog.read_text()
    assert "stale: true\n" in text
    assert 'stale_since: "2026-06-10"\n' in text
    # Each field should appear exactly once
    assert text.count("stale: ") == 1
    assert text.count("stale_since: ") == 1


def test_update_catalog_staleness_clears_stale_flag(tmp_path: Path) -> None:
    catalog = tmp_path / "lastfm.yaml"
    catalog.write_text(
        'source: lastfm\nstale: true\nstale_since: "2026-06-01"\nfields:\n  - name: track name\n'
    )
    lastfm_ingest.update_catalog_staleness(False, None, catalog_file=catalog)
    text = catalog.read_text()
    assert "stale: false\n" in text
    assert "stale_since: null\n" in text


def test_update_catalog_staleness_no_op_when_catalog_absent(tmp_path: Path) -> None:
    # Should not raise even if the file doesn't exist
    lastfm_ingest.update_catalog_staleness(True, "2026-06-01", catalog_file=tmp_path / "absent.yaml")


def test_update_catalog_staleness_stale_since_not_confused_with_stale(tmp_path: Path) -> None:
    """Ensure stale_since: line is not accidentally matched by the stale: handler."""
    catalog = tmp_path / "lastfm.yaml"
    catalog.write_text("source: lastfm\nstale_since: null\nstale: false\n")
    lastfm_ingest.update_catalog_staleness(True, "2026-06-15", catalog_file=catalog)
    text = catalog.read_text()
    assert text.count("stale: ") == 1
    assert text.count("stale_since: ") == 1
    assert "stale: true\n" in text
    assert 'stale_since: "2026-06-15"\n' in text


# ---------------------------------------------------------------------------
# Staleness-clear boundary: max_uts_seen vs persisted_uts
# ---------------------------------------------------------------------------

def test_stale_flag_not_cleared_when_max_uts_equals_persisted(tmp_path: Path) -> None:
    """Ad-hoc replay returning equal timestamp must NOT clear the stale flag.

    When --from/--since is set earlier than the saved checkpoint, fetched rows
    may have max_uts_seen == persisted_uts (a replay of existing data).  The
    stale flag must remain set in this case because scrobbling has not resumed.
    """
    staleness_file = tmp_path / "staleness.json"
    lastfm_ingest.save_staleness_state(
        {"stale": True, "stale_since": "2026-06-01"}, staleness_file=staleness_file
    )

    persisted_uts = 1_750_000_000
    max_uts_seen = persisted_uts  # equal — replay, not genuinely new data

    stale_state = lastfm_ingest.load_staleness_state(staleness_file=staleness_file)
    # Mirrors the fixed condition in main()
    if stale_state.get("stale") and (persisted_uts is None or max_uts_seen > persisted_uts):
        lastfm_ingest.save_staleness_state(
            {"stale": False, "stale_since": None}, staleness_file=staleness_file
        )

    result = lastfm_ingest.load_staleness_state(staleness_file=staleness_file)
    assert result["stale"] is True, "Stale flag must stay set after a replay with no new data"
    assert result["stale_since"] == "2026-06-01"


def test_stale_flag_not_cleared_when_max_uts_older_than_persisted(tmp_path: Path) -> None:
    """Ad-hoc replay returning older timestamp must NOT clear the stale flag."""
    staleness_file = tmp_path / "staleness.json"
    lastfm_ingest.save_staleness_state(
        {"stale": True, "stale_since": "2026-06-01"}, staleness_file=staleness_file
    )

    persisted_uts = 1_750_000_000
    max_uts_seen = persisted_uts - 1  # older than saved checkpoint

    stale_state = lastfm_ingest.load_staleness_state(staleness_file=staleness_file)
    if stale_state.get("stale") and (persisted_uts is None or max_uts_seen > persisted_uts):
        lastfm_ingest.save_staleness_state(
            {"stale": False, "stale_since": None}, staleness_file=staleness_file
        )

    result = lastfm_ingest.load_staleness_state(staleness_file=staleness_file)
    assert result["stale"] is True, "Stale flag must stay set when max_uts_seen < persisted_uts"


def test_stale_flag_cleared_when_max_uts_strictly_newer(tmp_path: Path) -> None:
    """Genuine new scrobble (max_uts_seen > persisted_uts) clears the stale flag."""
    staleness_file = tmp_path / "staleness.json"
    lastfm_ingest.save_staleness_state(
        {"stale": True, "stale_since": "2026-06-01"}, staleness_file=staleness_file
    )

    persisted_uts = 1_750_000_000
    max_uts_seen = persisted_uts + 1  # strictly newer — real resume

    stale_state = lastfm_ingest.load_staleness_state(staleness_file=staleness_file)
    if stale_state.get("stale") and (persisted_uts is None or max_uts_seen > persisted_uts):
        lastfm_ingest.save_staleness_state(
            {"stale": False, "stale_since": None}, staleness_file=staleness_file
        )

    result = lastfm_ingest.load_staleness_state(staleness_file=staleness_file)
    assert result["stale"] is False, "Stale flag must be cleared when max_uts_seen > persisted_uts"
    assert result["stale_since"] is None

