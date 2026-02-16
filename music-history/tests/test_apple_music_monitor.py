import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

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

    def test_main_returns_distinct_code_when_csv_missing(self) -> None:
        args = mock.Mock(
            raw_root=Path("/tmp/raw"),
            csv_file=None,
            warn_days=30,
            critical_days=90,
            json=False,
        )

        with (
            mock.patch("apple_music_monitor.parse_args", return_value=args),
            mock.patch("apple_music_monitor.discover_csv", side_effect=FileNotFoundError("missing csv")),
        ):
            with self.assertRaises(SystemExit) as cm:
                monitor.main()

        self.assertEqual(cm.exception.code, monitor.EXIT_CODE_MISSING_CSV)


if __name__ == "__main__":
    unittest.main()
