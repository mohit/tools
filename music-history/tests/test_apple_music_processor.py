import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import apple_music_processor as processor
import duckdb


class AppleMusicProcessorTests(unittest.TestCase):
    def test_process_csv_deduplicates_and_writes_partitioned_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            csv_path = tmp_path / "Apple Music Play Activity.csv"
            csv_path.write_text(
                "Track Description,Artist Name,Container Description,Event Start Timestamp,Play Count\n"
                "Track A,Artist A,Album A,2023-11-01T10:00:00Z,1\n"
                "Track A,Artist A,Album A,2023-11-01T10:00:00Z,1\n"
                "Track B,Artist B,Album B,2023-12-05T18:30:00Z,2\n",
                encoding="utf-8",
            )

            curated_root = tmp_path / "curated"
            result = processor.process_csv(csv_path=csv_path, curated_root=curated_root)

            self.assertEqual(result["total_rows"], 2)

            con = duckdb.connect()
            try:
                dataset_glob = str(curated_root / "year=*" / "month=*" / "*.parquet")
                rows = con.sql(
                    f"""
SELECT track, artist, album, play_count, CAST(played_at_utc AS VARCHAR)
FROM read_parquet('{dataset_glob}', hive_partitioning=TRUE)
ORDER BY played_at_utc
"""
                ).fetchall()
            finally:
                con.close()

            self.assertEqual(rows[0][0], "Track A")
            self.assertEqual(rows[1][0], "Track B")
            self.assertEqual(len(rows), 2)

    def test_discover_csv_selects_latest_play_activity_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            old_file = tmp_path / "old_Apple Music Play Activity.csv"
            new_file = tmp_path / "new_Apple Music Play Activity.csv"
            old_file.write_text("Event Start Timestamp\n2023-01-01T00:00:00Z\n", encoding="utf-8")
            new_file.write_text("Event Start Timestamp\n2024-01-01T00:00:00Z\n", encoding="utf-8")

            selected = processor.discover_csv(raw_root=tmp_path, explicit_file=None)
            self.assertEqual(selected, new_file)

    def test_discover_csv_supports_underscore_filename(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            csv_path = tmp_path / "Apple_Music_Play_Activity.csv"
            csv_path.write_text("Event Start Timestamp\n2024-01-01T00:00:00Z\n", encoding="utf-8")

            selected = processor.discover_csv(raw_root=tmp_path, explicit_file=None)
            self.assertEqual(selected, csv_path)

    def test_default_roots_use_environment_variables(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "DATALAKE_RAW_ROOT": "/tmp/custom-raw",
                "DATALAKE_CURATED_ROOT": "/tmp/custom-curated",
            },
            clear=False,
        ):
            raw_root, curated_root = processor._default_roots()

        self.assertEqual(raw_root, Path("/tmp/custom-raw/apple-music"))
        self.assertEqual(curated_root, Path("/tmp/custom-curated/apple-music/play-activity"))

    def test_default_roots_use_raw_root_for_curated_when_curated_env_missing(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "DATALAKE_RAW_ROOT": "/tmp/custom-raw",
            },
            clear=True,
        ):
            raw_root, curated_root = processor._default_roots()

        self.assertEqual(raw_root, Path("/tmp/custom-raw/apple-music"))
        self.assertEqual(curated_root, Path("/tmp/custom-raw/datalake/curated/apple-music/play-activity"))


if __name__ == "__main__":
    unittest.main()
