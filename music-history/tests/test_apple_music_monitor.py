import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

import apple_music_monitor as monitor


class AppleMusicMonitorTests(unittest.TestCase):
    def test_extract_latest_played_at(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            csv_path = tmp_path / "Apple Music Play Activity.csv"
            csv_path.write_text(
                "Event Start Timestamp\n"
                "2023-02-01T10:00:00Z\n"
                "2023-03-05T18:30:00Z\n",
                encoding="utf-8",
            )

            latest = monitor.extract_latest_played_at(csv_path)
            self.assertEqual(latest, datetime(2023, 3, 5, 18, 30, 0, tzinfo=UTC))

    def test_compute_status_thresholds(self) -> None:
        self.assertEqual(monitor.compute_status(days_stale=5, warn_days=30, critical_days=90), ("fresh", 0))
        self.assertEqual(monitor.compute_status(days_stale=35, warn_days=30, critical_days=90), ("warning", 1))
        self.assertEqual(monitor.compute_status(days_stale=120, warn_days=30, critical_days=90), ("critical", 2))

    def test_discover_csv_matches_underscore_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            csv_path = tmp_path / "Apple_Music_Play_Activity.csv"
            csv_path.write_text("Event Start Timestamp\n2024-01-01T00:00:00Z\n", encoding="utf-8")

            selected = monitor.discover_csv(raw_root=tmp_path, explicit_file=None)
            self.assertEqual(selected, csv_path)

    def test_main_missing_csv_exits_with_distinct_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            with self.assertRaises(SystemExit) as ctx:
                monitor.main(["--raw-root", str(tmp_path), "--json"])
            self.assertEqual(ctx.exception.code, 3)


if __name__ == "__main__":
    unittest.main()
