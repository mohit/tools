"""Storage resolution goes through datalake-paths and fails before any write."""

import json
from pathlib import Path

import amazon_orders_server as srv
import datalake_paths as dp
import pytest

UUID = "74EFF530-6DEC-4420-96C8-B46FA49DF852"


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """A datalake root that passes verification (volume checks mocked)."""
    dp._verified.clear()
    root = (tmp_path / "datalake.me").resolve()
    root.mkdir()
    (root / dp.IDENTITY_FILE).write_text(
        json.dumps({"volume_uuid": UUID, "canonical_root": str(root)})
    )
    monkeypatch.setenv("DATALAKE_ROOT", str(root))
    monkeypatch.delenv("MOER_MEDIA_VOLUME_UUID", raising=False)
    monkeypatch.setattr(dp, "_mount_point", lambda _p: Path("/Volumes/Test"))
    monkeypatch.setattr(dp, "volume_uuid", lambda _m: UUID)
    yield root
    dp._verified.clear()


@pytest.fixture
def no_lake(tmp_path, monkeypatch):
    dp._verified.clear()
    missing = tmp_path / "unplugged" / "datalake.me"
    monkeypatch.setenv("DATALAKE_ROOT", str(missing))
    yield missing
    dp._verified.clear()


def test_defaults_resolve_under_verified_root(lake):
    db, raw, curated = srv.resolve_storage()
    assert db == lake / "databases" / "amazon-orders-sync" / "amazon.duckdb"
    assert raw == lake / "raw" / "amazon"
    assert curated == lake / "curated" / "amazon"
    assert not (lake / "raw").exists(), "resolving must not create directories"


def test_no_legacy_home_defaults(lake):
    for path in srv.resolve_storage():
        assert "datalake.me" not in str(path.relative_to(lake))
        assert ".local" not in path.parts


def test_partial_override_must_stay_inside_lake(lake, tmp_path):
    inside = lake / "curated" / "amazon-test"
    _, _, curated = srv.resolve_storage(curated_dir=inside)
    assert curated == inside
    with pytest.raises(SystemExit, match="outside the datalake"):
        srv.resolve_storage(curated_dir=tmp_path / "elsewhere")


@pytest.mark.parametrize("command", ["serve", "curate", "rebuild", "status"])
def test_entry_points_fail_first_when_disk_missing(no_lake, command, monkeypatch):
    monkeypatch.setattr(srv, "create_app", lambda *_a, **_k: pytest.fail("app created"))
    monkeypatch.setattr(srv, "Store", lambda *_a, **_k: pytest.fail("store opened"))
    with pytest.raises(SystemExit, match="no data was written"):
        srv.main([command])
    assert not no_lake.exists()
    assert not no_lake.parent.exists()


def test_explicit_paths_skip_lake(no_lake, tmp_path):
    db, raw, curated = srv.resolve_storage(
        tmp_path / "a.duckdb", tmp_path / "raw", tmp_path / "curated"
    )
    assert (db, raw, curated) == (tmp_path / "a.duckdb", tmp_path / "raw", tmp_path / "curated")


def test_curate_against_lake_publishes_new_inode(lake):
    db, _raw, curated = srv.resolve_storage()
    srv.Store(db).close()
    assert srv.main(["status"]) == 0
    assert srv.main(["curate"]) == 0
    orders = curated / "orders.parquet"
    assert orders.exists()
    # A hard-linked historical alias must be left untouched by the next export.
    alias = lake / "versions" / "orders.parquet"
    alias.parent.mkdir()
    alias.hardlink_to(orders)
    before = alias.read_bytes()
    assert srv.main(["curate"]) == 0
    assert orders.stat().st_ino != alias.stat().st_ino
    assert alias.read_bytes() == before
    assert not list(curated.glob(".*tmp*")), "no temp files left behind"


def test_raw_batch_is_never_replaced(tmp_path):
    import datetime as dt

    received = dt.datetime(2025, 11, 20, 12, 0, 0)
    envelope = {"batch_id": "b-1", "records": []}
    path = srv.write_raw_batch(tmp_path, envelope, received)
    with pytest.raises(FileExistsError):
        srv.write_raw_batch(tmp_path, envelope, received)
    assert path.exists()
