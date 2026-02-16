import argparse
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import apple_music_sync as sync


class AppleMusicSyncTests(unittest.TestCase):
    def test_run_sync_missing_play_activity_returns_critical(self) -> None:
        args = argparse.Namespace(
            raw_root=Path("/tmp/raw"),
            csv_file=None,
            curated_root=Path("/tmp/curated"),
            warn_days=30,
            critical_days=90,
            skip_musickit=False,
            developer_token=None,
            user_token=None,
            musickit_raw_root=Path("/tmp/musickit-raw"),
            musickit_curated_root=Path("/tmp/musickit-curated"),
            json=False,
        )

        with mock.patch("apple_music_sync.processor.discover_csv", side_effect=FileNotFoundError):
            summary, exit_code = sync.run_sync(args)

        self.assertEqual(exit_code, 2)
        self.assertEqual(summary["play_activity"]["status"], "missing")
        self.assertFalse(summary["musickit"]["enabled"])

    def test_run_sync_processes_csv_and_runs_musickit(self) -> None:
        args = argparse.Namespace(
            raw_root=Path("/tmp/raw"),
            csv_file=None,
            curated_root=Path("/tmp/curated"),
            warn_days=30,
            critical_days=90,
            skip_musickit=False,
            developer_token="dev-token",
            user_token="user-token",
            musickit_raw_root=Path("/tmp/musickit-raw"),
            musickit_curated_root=Path("/tmp/musickit-curated"),
            json=False,
        )

        selected_csv = Path("/tmp/raw/20260210/Apple Music Play Activity.csv")
        latest_played = datetime(2026, 2, 10, 12, 0, tzinfo=UTC)

        with (
            mock.patch("apple_music_sync.processor.discover_csv", return_value=selected_csv),
            mock.patch(
                "apple_music_sync.processor.process_csv",
                return_value={"total_rows": 42, "curated_root": str(args.curated_root)},
            ),
            mock.patch("apple_music_sync.monitor.extract_latest_played_at", return_value=latest_played),
            mock.patch("apple_music_sync.monitor.compute_status", return_value=("fresh", 0)),
            mock.patch(
                "apple_music_sync.musickit.fetch_recent_tracks",
                return_value={"fetched_at_utc": 1700000000, "data": [{"id": "track-1"}]},
            ),
            mock.patch(
                "apple_music_sync.musickit.write_raw_snapshot",
                return_value=Path("/tmp/musickit-raw/recent_played_1700000000.json"),
            ),
            mock.patch("apple_music_sync.musickit.upsert_curated", return_value=10),
        ):
            summary, exit_code = sync.run_sync(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["play_activity"]["status"], "fresh")
        self.assertEqual(summary["play_activity"]["total_rows"], 42)
        self.assertTrue(summary["musickit"]["synced"])
        self.assertEqual(summary["musickit"]["fetched_tracks"], 1)


if __name__ == "__main__":
    unittest.main()
