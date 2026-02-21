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

    def test_build_activity_details_ndjson_excludes_stale_files(self):
        activities_dir = self.tmp_dir / "activities"
        activities_dir.mkdir(parents=True, exist_ok=True)
        (activities_dir / "111.json").write_text(
            json.dumps({"id": 111, "laps": [], "splits_metric": []}),
            encoding="utf-8",
        )
        (activities_dir / "222.json").write_text(
            json.dumps({"id": 222, "laps": [], "splits_metric": []}),
            encoding="utf-8",
        )

        rows = strava_pull.build_activity_details_ndjson(self.tmp_dir, {111})
        self.assertEqual(rows, 1)

        ndjson_path = self.tmp_dir / "activity_details.ndjson"
        payloads = [json.loads(line) for line in ndjson_path.read_text(encoding="utf-8").splitlines() if line]
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["id"], 111)

    def test_build_activity_streams_ndjson_excludes_stale_files_and_defaults_optional_streams(self):
        streams_dir = self.tmp_dir / "streams"
        streams_dir.mkdir(parents=True, exist_ok=True)
        (streams_dir / "444.json").write_text(
            json.dumps({"time": {"data": [0, 1]}}),
            encoding="utf-8",
        )
        (streams_dir / "555.json").write_text(
            json.dumps({"time": {"data": [0, 1]}, "watts": {"data": [220, 230]}}),
            encoding="utf-8",
        )

        rows = strava_pull.build_activity_streams_ndjson(self.tmp_dir, {444})
        self.assertEqual(rows, 1)

        ndjson_path = self.tmp_dir / "activity_streams.ndjson"
        payloads = [json.loads(line) for line in ndjson_path.read_text(encoding="utf-8").splitlines() if line]
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["activity_id"], 444)
        self.assertEqual(payloads[0]["watts"]["data"], [])
        self.assertEqual(payloads[0]["heartrate"]["data"], [])

    def test_collect_in_scope_activity_ids_filters_by_type_and_time_window(self):
        activities = [
            {"id": 101, "type": "Ride", "start_date": "2024-01-03T08:00:00Z"},
            {"id": 202, "type": "Run", "start_date": "2024-01-03T08:00:00Z"},
            {"id": 303, "type": "Ride", "start_date": "2024-01-01T08:00:00Z"},
        ]
        after = strava_pull.parse_date("2024-01-02")

        activity_ids = strava_pull.collect_in_scope_activity_ids(
            activities=activities,
            types={"Ride"},
            after=after,
            before=None,
        )

        self.assertEqual(activity_ids, {101})

    def test_export_parquet_handles_missing_watts_stream_column(self):
        out_dir = self.tmp_dir
        (out_dir / "activities.ndjson").write_text("{}\n", encoding="utf-8")
        (out_dir / "athlete.json").write_text("{}", encoding="utf-8")
        (out_dir / "stats.json").write_text("{}", encoding="utf-8")
        (out_dir / "activity_streams.ndjson").write_text('{"activity_id": 1, "time": {"data": [0, 1]}}\n', encoding="utf-8")

        class FakeResult:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return self._rows

        class FakeConnection:
            def __init__(self):
                self.statements = []

            def execute(self, sql, params=None):
                self.statements.append((sql, params))
                if "PRAGMA table_info('activity_streams_raw')" in sql:
                    # Match DuckDB PRAGMA table_info shape: cid, name, type, ...
                    return FakeResult(
                        [
                            (0, "activity_id", "BIGINT", False, None, False),
                            (1, "time", "STRUCT(data BIGINT[])", False, None, False),
                            (2, "distance", "STRUCT(data DOUBLE[])", False, None, False),
                            (3, "heartrate", "STRUCT(data BIGINT[])", False, None, False),
                            (4, "cadence", "STRUCT(data BIGINT[])", False, None, False),
                        ]
                    )
                return self

        fake_con = FakeConnection()
        original_duckdb = strava_pull.duckdb
        strava_pull.duckdb = types.SimpleNamespace(connect=lambda: fake_con)
        try:
            strava_pull.export_parquet(out_dir)
        finally:
            strava_pull.duckdb = original_duckdb

        streams_create_sql = next(
            sql
            for sql, _ in fake_con.statements
            if "CREATE OR REPLACE TABLE activity_streams AS" in sql
        )
        self.assertIn("0 AS power_points", streams_create_sql)
