from pathlib import Path
import importlib.util

import pyarrow as pa
import pyarrow.parquet as pq


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "lastfm_ingest.py"
spec = importlib.util.spec_from_file_location("lastfm_ingest", MODULE_PATH)
lastfm_ingest = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(lastfm_ingest)


def test_resolve_from_uts_uses_latest_state_or_curated_plus_one(tmp_path):
    state_file = tmp_path / "state" / "lastfm_last_uts.txt"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text("100")

    curated_dir = tmp_path / "curated"
    partition = curated_dir / "year=2026" / "month=02"
    partition.mkdir(parents=True, exist_ok=True)
    table = pa.table({"uts": [90, 150, 120]})
    pq.write_table(table, partition / "scrobbles_1.parquet")

    from_uts = lastfm_ingest.resolve_from_uts(
        cli_from_uts=None,
        cli_since=None,
        full_refetch=False,
        state_file=state_file,
        curated_root=curated_dir,
    )

    assert from_uts == 151


def test_resolve_from_uts_cli_precedence(tmp_path):
    state_file = tmp_path / "state.txt"
    curated_dir = tmp_path / "curated"

    assert lastfm_ingest.resolve_from_uts(10, None, False, state_file, curated_dir) == 10
    assert lastfm_ingest.resolve_from_uts(None, "1970-01-01T00:00:20Z", False, state_file, curated_dir) == 20
    assert lastfm_ingest.resolve_from_uts(10, "1970-01-01T00:00:20Z", True, state_file, curated_dir) == 0


def test_fetch_page_with_retries_retries_then_succeeds(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"recenttracks": {"track": []}}

    class FakeSession:
        def __init__(self):
            self.calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls < 3:
                raise lastfm_ingest.requests.Timeout("timeout")
            return FakeResponse()

    sleeps = []
    monkeypatch.setattr(lastfm_ingest.time, "sleep", lambda v: sleeps.append(v))
    monkeypatch.setattr(lastfm_ingest.random, "uniform", lambda a, b: 0.0)

    payload = lastfm_ingest.fetch_page_with_retries(
        session=FakeSession(),
        user="u",
        api_key="k",
        from_uts=0,
        page=1,
        limit=200,
        connect_timeout=1,
        read_timeout=1,
        max_retries=4,
        backoff_base=1,
    )

    assert payload == {"recenttracks": {"track": []}}
    assert sleeps == [1, 2]
