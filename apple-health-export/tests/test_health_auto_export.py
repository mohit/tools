"""Tests for health_auto_export.py."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

# Add parent directory to path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent))

import health_auto_export


class TestNormalizePayload(unittest.TestCase):
    """Test payload validation and normalization."""

    def test_normalize_records_and_workouts(self):
        payload = {
            "records": [
                {
                    "dataType": "HKQuantityTypeIdentifierStepCount",
                    "value": 123,
                    "unit": "count",
                    "startDate": "2026-02-09T07:00:00Z",
                    "endDate": "2026-02-09T07:05:00Z",
                    "sourceName": "iPhone",
                }
            ],
            "workouts": [
                {
                    "activityType": "HKWorkoutActivityTypeRunning",
                    "start": "2026-02-09T06:00:00Z",
                    "end": "2026-02-09T06:30:00Z",
                    "distance": 5.2,
                    "distanceUnit": "km",
                    "energyBurned": 380,
                }
            ],
        }

        records, workouts = health_auto_export.normalize_payload(payload)

        self.assertEqual(len(records), 1)
        self.assertEqual(len(workouts), 1)
        self.assertEqual(records[0]["type"], "HKQuantityTypeIdentifierStepCount")
        self.assertEqual(records[0]["sourceName"], "iPhone")
        self.assertEqual(workouts[0]["workoutActivityType"], "HKWorkoutActivityTypeRunning")
        self.assertEqual(workouts[0]["totalDistance"], "5.2")

    def test_normalize_rejects_empty_payload(self):
        with self.assertRaises(health_auto_export.PayloadValidationError):
            health_auto_export.normalize_payload({})


class TestIngestor(unittest.TestCase):
    """Test ingestion behavior."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)

    def tearDown(self):
        import shutil

        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)

    def test_ingest_stores_raw_payload_without_parquet(self):
        ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.temp_path / "raw",
            curated_dir=self.temp_path / "curated",
            enable_parquet=False,
        )

        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierHeartRate",
                    "value": "60",
                    "unit": "count/min",
                    "startDate": "2026-02-09T07:00:00Z",
                    "endDate": "2026-02-09T07:01:00Z",
                }
            ]
        }

        result = ingestor.ingest_payload(payload)

        self.assertTrue(result.raw_file.exists())
        self.assertEqual(result.records_received, 1)
        self.assertEqual(result.records_written, 0)
        self.assertEqual(result.workouts_written, 0)


class TestApiKeyValidation(unittest.TestCase):
    """Test API key policy helpers without opening network sockets."""

    def test_validate_api_key_when_unset(self):
        self.assertTrue(health_auto_export.validate_api_key({}, None))

    def test_validate_api_key_match_and_mismatch(self):
        self.assertTrue(
            health_auto_export.validate_api_key({"X-API-Key": "good"}, "good")
        )
        self.assertFalse(
            health_auto_export.validate_api_key({"X-API-Key": "bad"}, "good")
        )


if __name__ == "__main__":
    unittest.main()
