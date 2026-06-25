"""Tests for curated parquet copy-out (_copy_parquet_to_curated)."""
import importlib.util
import sys
from pathlib import Path


def load_module():
    spec = importlib.util.spec_from_file_location(
        "strava_pull",
        Path(__file__).parent.parent / "strava_pull.py",
    )
    mod = importlib.util.module_from_spec(spec)
    # Stub heavy optional deps before loading
    import types

    for dep in ("duckdb", "requests"):
        if dep not in sys.modules:
            sys.modules[dep] = types.ModuleType(dep)
    spec.loader.exec_module(mod)
    return mod


strava_pull = load_module()


class TestCopyParquetToCurated:
    def test_copies_existing_parquet_files(self, tmp_path):
        """Existing parquet files in out_dir are copied to curated_dir."""
        out_dir = tmp_path / "raw"
        out_dir.mkdir()
        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()

        # Create a couple of parquet files
        (out_dir / "activities.parquet").write_bytes(b"fake-parquet-activities")
        (out_dir / "athlete.parquet").write_bytes(b"fake-parquet-athlete")
        # stats.parquet intentionally absent

        strava_pull._copy_parquet_to_curated(out_dir, curated_dir)

        assert (curated_dir / "activities.parquet").read_bytes() == b"fake-parquet-activities"
        assert (curated_dir / "athlete.parquet").read_bytes() == b"fake-parquet-athlete"
        assert not (curated_dir / "stats.parquet").exists()

    def test_skips_missing_files_silently(self, tmp_path):
        """Files absent from out_dir are not created in curated_dir."""
        out_dir = tmp_path / "raw"
        out_dir.mkdir()
        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()
        # No parquet files at all

        strava_pull._copy_parquet_to_curated(out_dir, curated_dir)

        assert list(curated_dir.iterdir()) == []

    def test_overwrites_stale_curated_file(self, tmp_path):
        """An older curated parquet is replaced with the current raw version."""
        out_dir = tmp_path / "raw"
        out_dir.mkdir()
        curated_dir = tmp_path / "curated"
        curated_dir.mkdir()

        (curated_dir / "activities.parquet").write_bytes(b"old-stale-content")
        (out_dir / "activities.parquet").write_bytes(b"new-content")

        strava_pull._copy_parquet_to_curated(out_dir, curated_dir)

        assert (curated_dir / "activities.parquet").read_bytes() == b"new-content"

    def test_creates_curated_dir_when_missing(self, tmp_path):
        """curated_dir is created if it does not already exist."""
        out_dir = tmp_path / "raw"
        out_dir.mkdir()
        curated_dir = tmp_path / "new" / "curated" / "strava"
        # curated_dir does NOT exist yet

        (out_dir / "activities.parquet").write_bytes(b"data")
        curated_dir.mkdir(parents=True)  # caller is responsible for mkdir; test helper
        strava_pull._copy_parquet_to_curated(out_dir, curated_dir)

        assert (curated_dir / "activities.parquet").exists()

    def test_all_curated_filenames_covered(self, tmp_path):
        """_CURATED_PARQUET_FILES lists all expected output files."""
        expected = {
            "activities.parquet",
            "athlete.parquet",
            "stats.parquet",
            "activity_details.parquet",
            "activity_streams.parquet",
        }
        assert set(strava_pull._CURATED_PARQUET_FILES) == expected
