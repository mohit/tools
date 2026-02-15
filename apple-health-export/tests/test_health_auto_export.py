"""Tests for Health Auto Export ingestion support."""

import json
import sys
import tempfile
import threading
import time
from pathlib import Path
import unittest
from unittest import mock

import duckdb

# Add parent directory to path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent))

import health_auto_export


class TestHealthAutoExportIngest(unittest.TestCase):
    """Test ingestion and merge behavior."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.raw_dir = self.temp_path / "raw"
        self.curated_dir = self.temp_path / "curated"

        self.sample_payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": "2211",
                    "startDate": "2026-02-10T09:00:00Z",
                    "endDate": "2026-02-10T09:15:00Z",
                    "creationDate": "2026-02-10T09:15:10Z",
                    "metadata": {"source": "HealthAutoExport"},
                }
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeRunning",
                    "sourceName": "Watch",
                    "duration": "1800",
                    "durationUnit": "sec",
                    "totalDistance": "5000",
                    "totalDistanceUnit": "m",
                    "totalEnergyBurned": "400",
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-10T07:00:00Z",
                    "endDate": "2026-02-10T07:30:00Z",
                    "creationDate": "2026-02-10T07:35:00Z",
                }
            ],
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_ingest_payload_writes_raw_and_parquet(self):
        result = health_auto_export.ingest_payload(
            self.sample_payload,
            raw_root=self.raw_dir,
            curated_root=self.curated_dir,
        )

        self.assertEqual(result["records_received"], 1)
        self.assertEqual(result["workouts_received"], 1)

        raw_file = Path(result["raw_payload_path"])
        self.assertTrue(raw_file.exists())
        self.assertIn("health-auto-export", str(raw_file))

        health_records = self.curated_dir / "health_records.parquet"
        workouts = self.curated_dir / "workouts.parquet"
        self.assertTrue(health_records.exists())
        self.assertTrue(workouts.exists())

        with duckdb.connect() as con:
            records_total = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(health_records)]
            ).fetchone()[0]
            workouts_total = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(workouts)]
            ).fetchone()[0]

        self.assertEqual(records_total, 1)
        self.assertEqual(workouts_total, 1)

    def test_ingest_payload_deduplicates_repeated_batches(self):
        health_auto_export.ingest_payload(
            self.sample_payload,
            raw_root=self.raw_dir,
            curated_root=self.curated_dir,
        )
        health_auto_export.ingest_payload(
            self.sample_payload,
            raw_root=self.raw_dir,
            curated_root=self.curated_dir,
        )

        with duckdb.connect() as con:
            records_total = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(self.curated_dir / "health_records.parquet")],
            ).fetchone()[0]
            workouts_total = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(self.curated_dir / "workouts.parquet")],
            ).fetchone()[0]

        self.assertEqual(records_total, 1)
        self.assertEqual(workouts_total, 1)

    def test_ingest_payload_deduplicates_rows_within_single_payload(self):
        duplicated_payload = {
            "records": [
                self.sample_payload["records"][0],
                dict(self.sample_payload["records"][0]),
            ],
            "workouts": [
                self.sample_payload["workouts"][0],
                dict(self.sample_payload["workouts"][0]),
            ],
        }

        result = health_auto_export.ingest_payload(
            duplicated_payload,
            raw_root=self.raw_dir,
            curated_root=self.curated_dir,
        )

        self.assertEqual(result["records_received"], 2)
        self.assertEqual(result["workouts_received"], 2)
        self.assertEqual(result["health_records_total"], 1)
        self.assertEqual(result["workouts_total"], 1)

    def test_ingest_payload_rejects_invalid_record(self):
        invalid = {"records": [{"value": "10"}]}
        with self.assertRaises(ValueError):
            health_auto_export.ingest_payload(
                invalid,
                raw_root=self.raw_dir,
                curated_root=self.curated_dir,
            )

    def test_ingest_payload_reports_existing_totals_for_missing_entity_type(self):
        health_auto_export.ingest_payload(
            self.sample_payload,
            raw_root=self.raw_dir,
            curated_root=self.curated_dir,
        )

        records_only_payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": "3000",
                    "startDate": "2026-02-11T09:00:00Z",
                    "endDate": "2026-02-11T09:15:00Z",
                }
            ]
        }
        result = health_auto_export.ingest_payload(
            records_only_payload,
            raw_root=self.raw_dir,
            curated_root=self.curated_dir,
        )

        self.assertEqual(result["records_received"], 1)
        self.assertEqual(result["workouts_received"], 0)
        self.assertEqual(result["health_records_total"], 2)
        self.assertEqual(result["workouts_total"], 1)

    def test_ingest_payload_serializes_parallel_merges(self):
        original_merge = health_auto_export._merge_parquet
        active_merges = 0
        max_active_merges = 0
        counter_lock = threading.Lock()

        def wrapped_merge(*args, **kwargs):
            nonlocal active_merges, max_active_merges
            with counter_lock:
                active_merges += 1
                max_active_merges = max(max_active_merges, active_merges)
            try:
                time.sleep(0.05)
                return original_merge(*args, **kwargs)
            finally:
                with counter_lock:
                    active_merges -= 1

        def ingest_one(index: int):
            payload = {
                "records": [
                    {
                        "type": "HKQuantityTypeIdentifierStepCount",
                        "sourceName": "iPhone",
                        "unit": "count",
                        "value": str(1000 + index),
                        "startDate": f"2026-02-10T09:{index:02d}:00Z",
                        "endDate": f"2026-02-10T09:{index:02d}:30Z",
                    }
                ]
            }
            health_auto_export.ingest_payload(
                payload,
                raw_root=self.raw_dir,
                curated_root=self.curated_dir,
            )

        with mock.patch.object(health_auto_export, "_merge_parquet", side_effect=wrapped_merge):
            threads = [threading.Thread(target=ingest_one, args=(i,)) for i in range(5)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(max_active_merges, 1)

    def test_ingest_payload_parallel_requests_preserve_unique_rows(self):
        total_requests = 8

        def ingest_one(index: int):
            payload = {
                "records": [
                    {
                        "type": "HKQuantityTypeIdentifierStepCount",
                        "sourceName": "iPhone",
                        "unit": "count",
                        "value": str(2000 + index),
                        "startDate": f"2026-02-12T10:{index:02d}:00Z",
                        "endDate": f"2026-02-12T10:{index:02d}:30Z",
                    }
                ]
            }
            health_auto_export.ingest_payload(
                payload,
                raw_root=self.raw_dir,
                curated_root=self.curated_dir,
            )

        threads = [threading.Thread(target=ingest_one, args=(i,)) for i in range(total_requests)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        with duckdb.connect() as con:
            records_total = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)",
                [str(self.curated_dir / "health_records.parquet")],
            ).fetchone()[0]

        self.assertEqual(records_total, total_requests)


class TestHealthAutoExportAPI(unittest.TestCase):
    """Test Flask API endpoint behavior."""

    def setUp(self):
        if not health_auto_export.HAS_FLASK:
            self.skipTest("Flask is not installed in this environment")
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        app = health_auto_export.create_app(
            raw_root=self.temp_path / "raw",
            curated_root=self.temp_path / "curated",
            token="secret-token",
        )
        app.testing = True
        self.client = app.test_client()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_ingest_endpoint_requires_token(self):
        response = self.client.post(
            "/health-auto-export/v1/ingest",
            json={"records": []},
        )
        self.assertEqual(response.status_code, 401)

    def test_ingest_endpoint_accepts_bearer_token(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierHeartRate",
                    "startDate": "2026-02-10T08:00:00Z",
                    "endDate": "2026-02-10T08:00:05Z",
                    "value": "58",
                    "unit": "count/min",
                }
            ]
        }
        response = self.client.post(
            "/health-auto-export/v1/ingest",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"Authorization": "Bearer secret-token"},
        )

        self.assertEqual(response.status_code, 202)
        data = response.get_json()
        self.assertEqual(data["records_received"], 1)
        self.assertEqual(data["workouts_received"], 0)

    def test_ingest_endpoint_accepts_x_api_key(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "startDate": "2026-02-10T08:00:00Z",
                    "endDate": "2026-02-10T08:05:00Z",
                    "value": "650",
                    "unit": "count",
                }
            ]
        }
        response = self.client.post(
            "/health-auto-export/v1/ingest",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"X-API-Key": "secret-token"},
        )

        self.assertEqual(response.status_code, 202)
        data = response.get_json()
        self.assertEqual(data["records_received"], 1)

    def test_ingest_endpoint_rejects_invalid_json(self):
        response = self.client.post(
            "/health-auto-export/v1/ingest",
            data="not-json",
            content_type="application/json",
            headers={"Authorization": "Bearer secret-token"},
        )
        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        self.assertEqual(data["error"], "Invalid JSON payload")

    def test_healthcheck_endpoint(self):
        response = self.client.get("/health-auto-export/v1/health")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["status"], "ok")


if __name__ == "__main__":
    unittest.main()
