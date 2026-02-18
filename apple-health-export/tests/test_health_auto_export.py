"""Tests for health_auto_export.py."""

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

import health_auto_export


class TestHealthAutoExportIngestor(unittest.TestCase):
    """Test ingestion and parquet conversion behavior."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.raw_dir = self.root / "raw"
        self.curated_dir = self.root / "curated"
        self.ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir,
            curated_dir=self.curated_dir,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def sample_payload() -> dict:
        return {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 1234,
                    "startDate": "2026-02-17T10:00:00Z",
                    "endDate": "2026-02-17T10:15:00Z",
                    "creationDate": "2026-02-17T10:15:30Z",
                    "metadata": {"HKMetadataKeySyncVersion": "1"},
                },
                {
                    "type": "HKQuantityTypeIdentifierHeartRate",
                    "sourceName": "Apple Watch",
                    "unit": "count/min",
                    "value": 62,
                    "startDate": "2026-02-17 06:10:00 -0800",
                    "endDate": "2026-02-17 06:10:00 -0800",
                },
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeRunning",
                    "duration": 30,
                    "durationUnit": "min",
                    "totalDistance": 5.1,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 320,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-17T07:00:00Z",
                    "endDate": "2026-02-17T07:30:00Z",
                }
            ],
        }

    def test_ingest_payload_writes_raw_and_parquet(self):
        payload = self.sample_payload()

        result = self.ingestor.ingest_payload(payload, request_metadata={"source": "test"})

        self.assertEqual(result["records_ingested"], 2)
        self.assertEqual(result["workouts_ingested"], 1)
        self.assertTrue(Path(result["raw_path"]).exists())
        self.assertTrue((self.curated_dir / "health_records.parquet").exists())
        self.assertTrue((self.curated_dir / "health_workouts.parquet").exists())

        con = duckdb.connect(":memory:")
        record_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchone()[0]
        workout_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchone()[0]
        con.close()

        self.assertEqual(record_count, 2)
        self.assertEqual(workout_count, 1)

    def test_ingest_payload_deduplicates_on_reingest(self):
        payload = self.sample_payload()

        self.ingestor.ingest_payload(payload)
        self.ingestor.ingest_payload(payload)

        con = duckdb.connect(":memory:")
        record_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchone()[0]
        workout_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchone()[0]
        con.close()

        self.assertEqual(record_count, 2)
        self.assertEqual(workout_count, 1)

    def test_ingest_payload_rejects_invalid_payload(self):
        with self.assertRaises(ValueError):
            self.ingestor.ingest_payload({"foo": "bar"})


@unittest.skipUnless(health_auto_export.HAS_FLASK, "flask not installed")
class TestHealthAutoExportAPI(unittest.TestCase):
    """Test API endpoint behavior."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=root / "raw",
            curated_dir=root / "curated",
        )
        self.app = health_auto_export.create_app(ingestor, token="secret-token")
        self.client = self.app.test_client()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_api_requires_auth_token(self):
        response = self.client.post(
            "/v1/health/auto-export",
            data=json.dumps({"records": []}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 401)

    def test_api_ingests_payload(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "value": 100,
                    "unit": "count",
                    "startDate": "2026-02-17T12:00:00Z",
                }
            ]
        }

        response = self.client.post(
            "/v1/health/auto-export",
            data=json.dumps(payload),
            content_type="application/json",
            headers={"Authorization": "Bearer secret-token"},
        )

        self.assertEqual(response.status_code, 201)
        body = response.get_json()
        self.assertEqual(body["records_ingested"], 1)
        self.assertEqual(body["workouts_ingested"], 0)
        self.assertTrue(Path(body["raw_path"]).exists())


class TestHealthAutoExportCLI(unittest.TestCase):
    """CLI tests for ingest-file mode."""

    def test_ingest_file_load_error(self):
        with self.assertRaises(ValueError):
            health_auto_export._load_json_file(Path("/tmp/does-not-exist.json"))


if __name__ == "__main__":
    unittest.main()
