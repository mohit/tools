import tempfile
import unittest
from pathlib import Path

import duckdb

import apple_music_musickit_sync as musickit


class AppleMusicMusicKitSyncTests(unittest.TestCase):
    def test_upsert_curated_deduplicates_same_snapshot(self) -> None:
        payload = {
            "fetched_at_utc": 1700000000,
            "data": [
                {
                    "id": "track-1",
                    "attributes": {
                        "name": "Track A",
                        "artistName": "Artist A",
                        "albumName": "Album A",
                        "url": "https://music.apple.com/track-1",
                        "playParams": {"id": "track-1"},
                    },
                },
                {
                    "id": "track-2",
                    "attributes": {
                        "name": "Track B",
                        "artistName": "Artist B",
                        "albumName": "Album B",
                        "url": "https://music.apple.com/track-2",
                        "playParams": {"id": "track-2"},
                    },
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            curated_root = Path(tmp_dir) / "curated"
            first_count = musickit.upsert_curated(payload=payload, curated_root=curated_root)
            second_count = musickit.upsert_curated(payload=payload, curated_root=curated_root)

            self.assertEqual(first_count, 2)
            self.assertEqual(second_count, 2)

            con = duckdb.connect()
            try:
                dataset_glob = str(curated_root / "year=*" / "month=*" / "*.parquet")
                rows = con.sql(
                    f"SELECT track_id, track FROM read_parquet('{dataset_glob}', hive_partitioning=TRUE) ORDER BY track_id"
                ).fetchall()
            finally:
                con.close()

            self.assertEqual(rows, [("track-1", "Track A"), ("track-2", "Track B")])

    def test_write_raw_snapshot_creates_json_file(self) -> None:
        payload = {"fetched_at_utc": 1700000001, "data": []}
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_root = Path(tmp_dir)
            out = musickit.write_raw_snapshot(raw_root=raw_root, payload=payload)
            self.assertTrue(out.exists())
            self.assertIn("recent_played_1700000001.json", str(out))


if __name__ == "__main__":
    unittest.main()
