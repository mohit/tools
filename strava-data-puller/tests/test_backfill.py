import importlib.util
import json
import shutil
import sys
import types
from pathlib import Path
from unittest import TestCase


def load_module():
    sys.modules.setdefault("duckdb", types.SimpleNamespace(connect=lambda: None))
    sys.modules.setdefault("requests", types.SimpleNamespace(get=None, post=None))
    module_path = Path(__file__).resolve().parents[1] / "strava_pull.py"
    spec = importlib.util.spec_from_file_location("strava_pull", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


strava_pull = load_module()


class TestBackfill(TestCase):
    def setUp(self):
        self.tmp_dir = Path(__file__).resolve().parent / "tmp_backfill"
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_find_missing_detail_ids_detects_missing_and_partial_detail(self):
        activities = [
            {"id": 111, "type": "Ride", "start_date": "2024-01-01T08:00:00Z"},
            {"id": 222, "type": "Ride", "start_date": "2024-01-02T08:00:00Z"},
            {"id": 333, "type": "Ride", "start_date": "2024-01-03T08:00:00Z"},
        ]

        activities_dir = self.tmp_dir / "activities"
        streams_dir = self.tmp_dir / "streams"
        activities_dir.mkdir(parents=True, exist_ok=True)
        streams_dir.mkdir(parents=True, exist_ok=True)

        # Partial detail file: has laps but no splits.
        (activities_dir / "222.json").write_text(
            json.dumps({"id": 222, "laps": []}),
            encoding="utf-8",
        )
        # Complete detail + streams.
        (activities_dir / "333.json").write_text(
            json.dumps({"id": 333, "laps": [], "splits_metric": [], "splits_standard": []}),
            encoding="utf-8",
        )
        (streams_dir / "333.json").write_text(
            json.dumps({"time": {"data": [0, 1]}, "distance": {"data": [0.0, 10.0]}}),
            encoding="utf-8",
        )

        missing = strava_pull.find_missing_detail_ids(
            activities=activities,
            out_dir=self.tmp_dir,
            types={"Ride"},
            after=None,
            before=None,
            include_streams=True,
        )

        self.assertEqual([item[0] for item in missing], [111, 222])
        self.assertIn("missing_detail_file", missing[0][1])
        self.assertIn("missing_laps_or_splits", missing[1][1])
        self.assertIn("missing_streams_file", missing[1][1])

    def test_build_activity_streams_ndjson_includes_activity_id(self):
        streams_dir = self.tmp_dir / "streams"
        streams_dir.mkdir(parents=True, exist_ok=True)
        (streams_dir / "444.json").write_text(
            json.dumps({"time": {"data": [0, 1]}, "watts": {"data": [150, 180]}}),
            encoding="utf-8",
        )

        rows = strava_pull.build_activity_streams_ndjson(self.tmp_dir)
        self.assertEqual(rows, 1)

        ndjson_path = self.tmp_dir / "activity_streams.ndjson"
        self.assertTrue(ndjson_path.exists())
        payload = json.loads(ndjson_path.read_text(encoding="utf-8").strip())
        self.assertEqual(payload["activity_id"], 444)
        self.assertIn("time", payload)
        self.assertIn("watts", payload)
