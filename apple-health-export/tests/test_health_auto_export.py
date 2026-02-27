"""Tests for health_auto_export.py."""

import contextlib
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import health_auto_export


class _CountingLock:
    def __init__(self):
        self.enter_count = 0
        self.exit_count = 0

    def __enter__(self):
        self.enter_count += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exit_count += 1
        return False


class _TrackingLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0

    def __enter__(self):
        self._lock.acquire()
        self._active += 1
        self.max_active = max(self.max_active, self._active)
        return self

    def __exit__(self, exc_type, exc, tb):
        self._active -= 1
        self._lock.release()
        return False


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

    def test_ingest_payload_deduplicates_duplicates_within_single_batch(self):
        payload = self.sample_payload()
        payload["records"].append(dict(payload["records"][0]))
        payload["workouts"].append(dict(payload["workouts"][0]))

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 2)
        self.assertEqual(result["workouts_ingested"], 1)

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

    def test_ingest_payload_list_input_deduplicates_within_single_batch(self):
        payload = [
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "sourceName": "iPhone",
                "unit": "count",
                "value": 555,
                "startDate": "2026-02-19T10:00:00Z",
                "endDate": "2026-02-19T10:15:00Z",
            },
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "sourceName": "iPhone",
                "unit": "count",
                "value": 555,
                "startDate": "2026-02-19T10:00:00Z",
                "endDate": "2026-02-19T10:15:00Z",
            },
        ]

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 1)
        self.assertEqual(result["workouts_ingested"], 0)

        con = duckdb.connect(":memory:")
        record_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchone()[0]
        con.close()

        self.assertEqual(record_count, 1)

    def test_normalize_payload_deduplicates_within_payload_batch(self):
        payload = self.sample_payload()
        payload["records"].append(dict(payload["records"][0]))
        payload["workouts"].append(dict(payload["workouts"][0]))

        records, workouts, errors = self.ingestor._normalize_payload(payload)

        self.assertEqual(errors, [])
        self.assertEqual(len(records), 2)
        self.assertEqual(len(workouts), 1)

    def test_normalize_payload_deduplicates_within_nested_data_batch(self):
        payload = {
            "data": {
                "records": [
                    {
                        "type": "HKQuantityTypeIdentifierStepCount",
                        "sourceName": "iPhone",
                        "unit": "count",
                        "value": 777,
                        "startDate": "2026-02-24T10:00:00Z",
                        "endDate": "2026-02-24T10:15:00Z",
                        "metadata": {"variant": "first"},
                    },
                    {
                        "type": "HKQuantityTypeIdentifierStepCount",
                        "sourceName": "iPhone",
                        "unit": "count",
                        "value": 777,
                        "startDate": "2026-02-24T10:00:00Z",
                        "endDate": "2026-02-24T10:15:00Z",
                        "metadata": {"variant": "second"},
                    },
                ],
                "workouts": [
                    {
                        "workoutActivityType": "HKWorkoutActivityTypeWalking",
                        "duration": 35,
                        "durationUnit": "min",
                        "totalDistance": 2.2,
                        "totalDistanceUnit": "km",
                        "totalEnergyBurned": 130,
                        "totalEnergyBurnedUnit": "kcal",
                        "sourceName": "Apple Watch",
                        "startDate": "2026-02-24T11:00:00Z",
                        "endDate": "2026-02-24T11:35:00Z",
                        "metadata": {"variant": "first"},
                    },
                    {
                        "workoutActivityType": "HKWorkoutActivityTypeWalking",
                        "duration": 35,
                        "durationUnit": "min",
                        "totalDistance": 2.2,
                        "totalDistanceUnit": "km",
                        "totalEnergyBurned": 130,
                        "totalEnergyBurnedUnit": "kcal",
                        "sourceName": "Apple Watch",
                        "startDate": "2026-02-24T11:00:00Z",
                        "endDate": "2026-02-24T11:35:00Z",
                        "metadata": {"variant": "second"},
                    },
                ],
            }
        }

        records, workouts, errors = self.ingestor._normalize_payload(payload)

        self.assertEqual(errors, [])
        self.assertEqual(len(records), 1)
        self.assertEqual(len(workouts), 1)
        self.assertEqual(json.loads(records[0]["metadata_json"]), {"variant": "first"})
        self.assertEqual(json.loads(workouts[0]["metadata_json"]), {"variant": "first"})

    def test_normalize_payload_deduplicates_invalid_rows_before_error_counting(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 100,
                },
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 100,
                },
            ]
        }

        records, workouts, errors = self.ingestor._normalize_payload(payload)

        self.assertEqual(records, [])
        self.assertEqual(workouts, [])
        self.assertEqual(errors, ["1 record(s) missing required fields", "no valid records or workouts found"])

    def test_merge_to_parquet_deduplicates_incoming_batch(self):
        payload = self.sample_payload()
        records, workouts, errors = self.ingestor._normalize_payload(payload)
        self.assertEqual(errors, [])

        duplicate_records = [records[0], dict(records[0]), records[1]]
        duplicate_workouts = [workouts[0], dict(workouts[0])]

        result = self.ingestor._merge_to_parquet(
            duplicate_records,
            duplicate_workouts,
            self.raw_dir / "test_raw.json",
        )

        self.assertEqual(result["records_ingested"], 2)
        self.assertEqual(result["workouts_ingested"], 1)

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

    def test_merge_to_parquet_keeps_first_duplicate_row_in_batch(self):
        payload = self.sample_payload()
        records, workouts, errors = self.ingestor._normalize_payload(payload)
        self.assertEqual(errors, [])

        first_record = dict(records[0])
        second_record = dict(records[0])
        first_record["metadata_json"] = '{"source":"first"}'
        second_record["metadata_json"] = '{"source":"second"}'

        first_workout = dict(workouts[0])
        second_workout = dict(workouts[0])
        first_workout["metadata_json"] = '{"source":"first"}'
        second_workout["metadata_json"] = '{"source":"second"}'

        self.ingestor._merge_to_parquet(
            [first_record, second_record],
            [first_workout, second_workout],
            self.raw_dir / "test_raw.json",
        )

        con = duckdb.connect(":memory:")
        record_metadata = con.execute(
            """
            SELECT metadata_json
            FROM read_parquet(?)
            WHERE type = ?
            """,
            [
                str(self.curated_dir / "health_records.parquet"),
                first_record["type"],
            ],
        ).fetchone()[0]
        workout_metadata = con.execute(
            """
            SELECT metadata_json
            FROM read_parquet(?)
            WHERE workoutActivityType = ?
            """,
            [
                str(self.curated_dir / "health_workouts.parquet"),
                first_workout["workoutActivityType"],
            ],
        ).fetchone()[0]
        con.close()

        self.assertEqual(record_metadata, '{"source":"first"}')
        self.assertEqual(workout_metadata, '{"source":"first"}')

    def test_merge_to_parquet_rolls_back_if_second_promote_fails(self):
        baseline_payload = self.sample_payload()
        self.ingestor.ingest_payload(baseline_payload)

        update_payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 999,
                    "startDate": "2026-02-25T10:00:00Z",
                    "endDate": "2026-02-25T10:10:00Z",
                }
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 99,
                    "durationUnit": "min",
                    "totalDistance": 9.9,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 999,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-25T10:00:00Z",
                    "endDate": "2026-02-25T11:39:00Z",
                }
            ],
        }
        records, workouts, errors = self.ingestor._normalize_payload(update_payload)
        self.assertEqual(errors, [])

        original_replace = Path.replace

        def flaky_replace(path_obj: Path, target: Path):
            if Path(path_obj) == self.ingestor.workouts_parquet.with_name("health_workouts.parquet.fail.tmp"):
                raise OSError("simulated promote failure")
            return original_replace(path_obj, target)

        records_tmp = self.ingestor.records_parquet.with_name("health_records.parquet.ok.tmp")
        workouts_tmp = self.ingestor.workouts_parquet.with_name("health_workouts.parquet.fail.tmp")

        with (
            mock.patch.object(
                self.ingestor,
                "_write_records_parquet",
                return_value=records_tmp,
            ),
            mock.patch.object(
                self.ingestor,
                "_write_workouts_parquet",
                return_value=workouts_tmp,
            ),
            mock.patch.object(Path, "replace", autospec=True, side_effect=flaky_replace),
        ):
            records_tmp.write_text("records tmp", encoding="utf-8")
            workouts_tmp.write_text("workouts tmp", encoding="utf-8")
            with self.assertRaises(OSError):
                self.ingestor._merge_to_parquet(records, workouts, self.raw_dir / "test_raw.json")

        self.assertFalse(records_tmp.exists())
        self.assertFalse(workouts_tmp.exists())

        con = duckdb.connect(":memory:")
        record_values = con.execute(
            """
            SELECT value
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        workout_durations = con.execute(
            """
            SELECT duration
            FROM read_parquet(?)
            WHERE workoutActivityType = 'HKWorkoutActivityTypeRunning'
            """,
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual([row[0] for row in record_values], ["1234"])
        self.assertEqual([row[0] for row in workout_durations], ["30"])

    def test_dedupe_incoming_batch_keeps_first_record_and_workout(self):
        payload = self.sample_payload()
        records, workouts, errors = self.ingestor._normalize_payload(payload)
        self.assertEqual(errors, [])

        duplicate_record = dict(records[0])
        duplicate_record["metadata_json"] = '{"alternate":"value"}'
        duplicate_workout = dict(workouts[0])
        duplicate_workout["metadata_json"] = '{"alternate":"value"}'

        deduped_records = self.ingestor._dedupe_incoming_records_batch([records[0], duplicate_record])
        deduped_workouts = self.ingestor._dedupe_incoming_workouts_batch([workouts[0], duplicate_workout])

        self.assertEqual(len(deduped_records), 1)
        self.assertEqual(len(deduped_workouts), 1)
        self.assertEqual(deduped_records[0]["metadata_json"], records[0]["metadata_json"])
        self.assertEqual(deduped_workouts[0]["metadata_json"], workouts[0]["metadata_json"])

    def test_dedupe_incoming_batch_handles_missing_key_fields(self):
        record_a = {"type": "HKQuantityTypeIdentifierStepCount", "sourceName": "iPhone"}
        record_b = {"type": "HKQuantityTypeIdentifierStepCount", "sourceName": "iPhone"}
        workout_a = {"workoutActivityType": "HKWorkoutActivityTypeWalking", "sourceName": "Apple Watch"}
        workout_b = {"workoutActivityType": "HKWorkoutActivityTypeWalking", "sourceName": "Apple Watch"}

        deduped_records = self.ingestor._dedupe_incoming_records_batch([record_a, record_b])
        deduped_workouts = self.ingestor._dedupe_incoming_workouts_batch([workout_a, workout_b])

        self.assertEqual(len(deduped_records), 1)
        self.assertEqual(len(deduped_workouts), 1)

    def test_dedupe_incoming_batch_handles_non_hashable_key_values(self):
        record_a = {
            "type": "HKCategoryTypeIdentifierSleepAnalysis",
            "sourceName": "iPhone",
            "unit": "min",
            "value": {"minutes": 45},
            "startDate": "2026-02-21T22:00:00Z",
            "endDate": "2026-02-21T22:45:00Z",
            "metadata_json": '{"source":"first"}',
        }
        record_b = dict(record_a)
        record_b["value"] = {"minutes": 45}
        record_b["metadata_json"] = '{"source":"second"}'

        workout_a = {
            "workoutActivityType": "HKWorkoutActivityTypeOther",
            "sourceName": "Apple Watch",
            "startDate": "2026-02-21T18:00:00Z",
            "endDate": "2026-02-21T18:30:00Z",
            "duration": [30],
            "totalDistance": [5.0],
            "totalEnergyBurned": [250],
            "metadata_json": '{"source":"first"}',
        }
        workout_b = dict(workout_a)
        workout_b["duration"] = [30]
        workout_b["totalDistance"] = [5.0]
        workout_b["totalEnergyBurned"] = [250]
        workout_b["metadata_json"] = '{"source":"second"}'

        deduped_records = self.ingestor._dedupe_incoming_records_batch([record_a, record_b])
        deduped_workouts = self.ingestor._dedupe_incoming_workouts_batch([workout_a, workout_b])

        self.assertEqual(len(deduped_records), 1)
        self.assertEqual(len(deduped_workouts), 1)
        self.assertEqual(deduped_records[0]["metadata_json"], '{"source":"first"}')
        self.assertEqual(deduped_workouts[0]["metadata_json"], '{"source":"first"}')

    def test_acquire_merge_lock_uses_thread_and_process_locks(self):
        events: list[str] = []

        @contextlib.contextmanager
        def fake_process_lock():
            events.append("process_enter")
            try:
                yield
            finally:
                events.append("process_exit")

        class _InlineLock:
            def __enter__(self):
                events.append("thread_enter")
                return self

            def __exit__(self, exc_type, exc, tb):
                events.append("thread_exit")
                return False

        self.ingestor._parquet_merge_lock = _InlineLock()
        with mock.patch.object(self.ingestor, "_acquire_process_merge_lock", fake_process_lock):
            with self.ingestor._acquire_merge_lock():
                events.append("inside")

        self.assertEqual(
            events,
            ["thread_enter", "process_enter", "inside", "process_exit", "thread_exit"],
        )

    def test_ingest_payload_uses_parquet_merge_lock(self):
        payload = self.sample_payload()
        lock = _CountingLock()
        self.ingestor._parquet_merge_lock = lock

        self.ingestor.ingest_payload(payload)

        self.assertEqual(lock.enter_count, 1)
        self.assertEqual(lock.exit_count, 1)

    def test_merge_to_parquet_uses_parquet_merge_lock(self):
        payload = self.sample_payload()
        records, workouts, errors = self.ingestor._normalize_payload(payload)
        self.assertEqual(errors, [])
        lock = _CountingLock()
        self.ingestor._parquet_merge_lock = lock

        self.ingestor._merge_to_parquet(records, workouts, self.raw_dir / "test_raw.json")

        self.assertEqual(lock.enter_count, 1)
        self.assertEqual(lock.exit_count, 1)

    def test_ingestors_share_merge_lock_for_same_curated_dir(self):
        other_ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir / "other",
            curated_dir=self.curated_dir,
        )

        self.assertIs(self.ingestor._parquet_merge_lock, other_ingestor._parquet_merge_lock)

    def test_ingestors_share_merge_lock_for_tilde_and_absolute_curated_dir(self):
        with mock.patch.dict(os.environ, {"HOME": str(self.root)}):
            tilde_curated = Path("~") / "curated"
            first_ingestor = health_auto_export.HealthAutoExportIngestor(
                raw_dir=self.raw_dir / "first",
                curated_dir=tilde_curated,
            )
            second_ingestor = health_auto_export.HealthAutoExportIngestor(
                raw_dir=self.raw_dir / "other",
                curated_dir=self.root / "curated",
            )

        self.assertIs(first_ingestor._parquet_merge_lock, second_ingestor._parquet_merge_lock)

    def test_ingestors_share_merge_lock_for_symlinked_curated_dir(self):
        linked_curated = self.root / "linked-curated"
        linked_curated.symlink_to(self.curated_dir, target_is_directory=True)

        first_ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir / "first",
            curated_dir=self.curated_dir,
        )
        second_ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir / "second",
            curated_dir=linked_curated,
        )

        self.assertIs(first_ingestor._parquet_merge_lock, second_ingestor._parquet_merge_lock)

    def test_concurrent_ingests_serialize_merge_lock_and_preserve_rows(self):
        tracking_lock = _TrackingLock()
        self.ingestor._parquet_merge_lock = tracking_lock

        other_ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir / "other",
            curated_dir=self.curated_dir,
        )
        other_ingestor._parquet_merge_lock = tracking_lock

        payload_a = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 100,
                    "startDate": "2026-02-18T10:00:00Z",
                    "endDate": "2026-02-18T10:10:00Z",
                }
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 10,
                    "durationUnit": "min",
                    "totalDistance": 1.0,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 75,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-18T10:00:00Z",
                    "endDate": "2026-02-18T10:10:00Z",
                }
            ],
        }
        payload_b = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 200,
                    "startDate": "2026-02-18T11:00:00Z",
                    "endDate": "2026-02-18T11:10:00Z",
                }
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 20,
                    "durationUnit": "min",
                    "totalDistance": 2.0,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 150,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-18T11:00:00Z",
                    "endDate": "2026-02-18T11:20:00Z",
                }
            ],
        }

        errors: list[Exception] = []

        def _ingest_with_capture(ingestor, payload):
            try:
                ingestor.ingest_payload(payload)
            except Exception as exc:  # pragma: no cover - defensive in thread
                errors.append(exc)

        thread_a = threading.Thread(target=_ingest_with_capture, args=(self.ingestor, payload_a))
        thread_b = threading.Thread(target=_ingest_with_capture, args=(other_ingestor, payload_b))
        thread_a.start()
        thread_b.start()
        thread_a.join()
        thread_b.join()

        self.assertEqual(errors, [])
        self.assertEqual(tracking_lock.max_active, 1)

        con = duckdb.connect(":memory:")
        rows = con.execute(
            """
            SELECT value
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        workout_rows = con.execute(
            """
            SELECT duration
            FROM read_parquet(?)
            WHERE workoutActivityType = 'HKWorkoutActivityTypeWalking'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual([row[0] for row in rows], ["100", "200"])
        self.assertEqual([row[0] for row in workout_rows], ["10", "20"])

    def test_concurrent_ingests_with_shared_default_lock_preserve_rows(self):
        other_ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir / "other",
            curated_dir=self.curated_dir,
        )

        payload_a = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 300,
                    "startDate": "2026-02-20T10:00:00Z",
                    "endDate": "2026-02-20T10:10:00Z",
                }
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 30,
                    "durationUnit": "min",
                    "totalDistance": 2.5,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 180,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-20T10:00:00Z",
                    "endDate": "2026-02-20T10:30:00Z",
                }
            ],
        }
        payload_b = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 400,
                    "startDate": "2026-02-20T11:00:00Z",
                    "endDate": "2026-02-20T11:10:00Z",
                }
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 40,
                    "durationUnit": "min",
                    "totalDistance": 3.0,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 220,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-20T11:00:00Z",
                    "endDate": "2026-02-20T11:40:00Z",
                }
            ],
        }

        errors: list[Exception] = []

        def _ingest_with_capture(ingestor, payload):
            try:
                ingestor.ingest_payload(payload)
            except Exception as exc:  # pragma: no cover - defensive in thread
                errors.append(exc)

        thread_a = threading.Thread(target=_ingest_with_capture, args=(self.ingestor, payload_a))
        thread_b = threading.Thread(target=_ingest_with_capture, args=(other_ingestor, payload_b))
        thread_a.start()
        thread_b.start()
        thread_a.join()
        thread_b.join()

        self.assertEqual(errors, [])

        con = duckdb.connect(":memory:")
        rows = con.execute(
            """
            SELECT value
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        workout_rows = con.execute(
            """
            SELECT duration
            FROM read_parquet(?)
            WHERE workoutActivityType = 'HKWorkoutActivityTypeWalking'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual([row[0] for row in rows], ["300", "400"])
        self.assertEqual([row[0] for row in workout_rows], ["30", "40"])

    def test_concurrent_ingests_preserve_unique_rows_when_each_payload_has_duplicates(self):
        other_ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir / "other",
            curated_dir=self.curated_dir,
        )

        payload_a = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 510,
                    "startDate": "2026-02-21T10:00:00Z",
                    "endDate": "2026-02-21T10:10:00Z",
                    "metadata": {"variant": "first"},
                },
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 510,
                    "startDate": "2026-02-21T10:00:00Z",
                    "endDate": "2026-02-21T10:10:00Z",
                    "metadata": {"variant": "second"},
                },
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 51,
                    "durationUnit": "min",
                    "totalDistance": 3.1,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 210,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-21T10:00:00Z",
                    "endDate": "2026-02-21T10:51:00Z",
                    "metadata": {"variant": "first"},
                },
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 51,
                    "durationUnit": "min",
                    "totalDistance": 3.1,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 210,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-21T10:00:00Z",
                    "endDate": "2026-02-21T10:51:00Z",
                    "metadata": {"variant": "second"},
                },
            ],
        }
        payload_b = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 520,
                    "startDate": "2026-02-21T11:00:00Z",
                    "endDate": "2026-02-21T11:10:00Z",
                    "metadata": {"variant": "first"},
                },
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 520,
                    "startDate": "2026-02-21T11:00:00Z",
                    "endDate": "2026-02-21T11:10:00Z",
                    "metadata": {"variant": "second"},
                },
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 52,
                    "durationUnit": "min",
                    "totalDistance": 3.2,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 220,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-21T11:00:00Z",
                    "endDate": "2026-02-21T11:52:00Z",
                    "metadata": {"variant": "first"},
                },
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 52,
                    "durationUnit": "min",
                    "totalDistance": 3.2,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 220,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-21T11:00:00Z",
                    "endDate": "2026-02-21T11:52:00Z",
                    "metadata": {"variant": "second"},
                },
            ],
        }

        errors: list[Exception] = []

        def _ingest_with_capture(ingestor, payload):
            try:
                ingestor.ingest_payload(payload)
            except Exception as exc:  # pragma: no cover - defensive in thread
                errors.append(exc)

        thread_a = threading.Thread(target=_ingest_with_capture, args=(self.ingestor, payload_a))
        thread_b = threading.Thread(target=_ingest_with_capture, args=(other_ingestor, payload_b))
        thread_a.start()
        thread_b.start()
        thread_a.join()
        thread_b.join()

        self.assertEqual(errors, [])

        con = duckdb.connect(":memory:")
        record_rows = con.execute(
            """
            SELECT value
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        workout_rows = con.execute(
            """
            SELECT duration
            FROM read_parquet(?)
            WHERE workoutActivityType = 'HKWorkoutActivityTypeWalking'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual([row[0] for row in record_rows], ["510", "520"])
        self.assertEqual([row[0] for row in workout_rows], ["51", "52"])

    def test_concurrent_ingests_same_ingestor_preserve_rows(self):
        payload_a = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 700,
                    "startDate": "2026-02-21T10:00:00Z",
                    "endDate": "2026-02-21T10:10:00Z",
                }
            ]
        }
        payload_b = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 800,
                    "startDate": "2026-02-21T11:00:00Z",
                    "endDate": "2026-02-21T11:10:00Z",
                }
            ]
        }

        errors: list[Exception] = []

        def _ingest_with_capture(payload):
            try:
                self.ingestor.ingest_payload(payload)
            except Exception as exc:  # pragma: no cover - defensive in thread
                errors.append(exc)

        thread_a = threading.Thread(target=_ingest_with_capture, args=(payload_a,))
        thread_b = threading.Thread(target=_ingest_with_capture, args=(payload_b,))
        thread_a.start()
        thread_b.start()
        thread_a.join()
        thread_b.join()

        self.assertEqual(errors, [])

        con = duckdb.connect(":memory:")
        rows = con.execute(
            """
            SELECT value
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            ORDER BY startDate
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual([row[0] for row in rows], ["700", "800"])

    def test_ingest_payload_deduplicates_batch_when_only_metadata_differs(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 1234,
                    "startDate": "2026-02-22T10:00:00Z",
                    "endDate": "2026-02-22T10:15:00Z",
                    "metadata": {"dedupe": "first"},
                },
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 1234,
                    "startDate": "2026-02-22T10:00:00Z",
                    "endDate": "2026-02-22T10:15:00Z",
                    "metadata": {"dedupe": "second"},
                },
            ]
        }

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 1)
        self.assertEqual(result["workouts_ingested"], 0)

        con = duckdb.connect(":memory:")
        ingested = con.execute(
            """
            SELECT value, metadata_json
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual(len(ingested), 1)
        self.assertEqual(ingested[0][0], "1234")
        self.assertEqual(json.loads(ingested[0][1]), {"dedupe": "first"})

    def test_ingest_payload_deduplicates_record_batch_when_only_non_key_fields_differ(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "sourceVersion": "1.0",
                    "unit": "count",
                    "value": 4321,
                    "startDate": "2026-02-22T12:00:00Z",
                    "endDate": "2026-02-22T12:15:00Z",
                    "creationDate": "2026-02-22T12:15:01Z",
                    "metadata": {"variant": "first"},
                },
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "sourceVersion": "2.0",
                    "unit": "count",
                    "value": 4321,
                    "startDate": "2026-02-22T12:00:00Z",
                    "endDate": "2026-02-22T12:15:00Z",
                    "creationDate": "2026-02-22T12:15:59Z",
                    "metadata": {"variant": "second"},
                },
            ]
        }

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 1)
        self.assertEqual(result["workouts_ingested"], 0)

        con = duckdb.connect(":memory:")
        ingested = con.execute(
            """
            SELECT sourceVersion, metadata_json
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual(len(ingested), 1)
        self.assertEqual(ingested[0][0], "1.0")
        self.assertEqual(json.loads(ingested[0][1]), {"variant": "first"})

    def test_ingest_payload_deduplicates_workout_batch_when_only_metadata_differs(self):
        payload = {
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 45,
                    "durationUnit": "min",
                    "totalDistance": 3.5,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 210,
                    "totalEnergyBurnedUnit": "kcal",
                    "sourceName": "Apple Watch",
                    "startDate": "2026-02-22T16:00:00Z",
                    "endDate": "2026-02-22T16:45:00Z",
                    "metadata": {"dedupe": "first"},
                },
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 45,
                    "durationUnit": "min",
                    "totalDistance": 3.5,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 210,
                    "totalEnergyBurnedUnit": "kcal",
                    "sourceName": "Apple Watch",
                    "startDate": "2026-02-22T16:00:00Z",
                    "endDate": "2026-02-22T16:45:00Z",
                    "metadata": {"dedupe": "second"},
                },
            ]
        }

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 0)
        self.assertEqual(result["workouts_ingested"], 1)

        con = duckdb.connect(":memory:")
        ingested = con.execute(
            """
            SELECT duration, metadata_json
            FROM read_parquet(?)
            WHERE workoutActivityType = 'HKWorkoutActivityTypeWalking'
            """,
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual(len(ingested), 1)
        self.assertEqual(ingested[0][0], "45")
        self.assertEqual(json.loads(ingested[0][1]), {"dedupe": "first"})

    def test_ingest_payload_list_batch_deduplicates_when_only_metadata_fields_differ(self):
        payload = [
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "sourceName": "iPhone",
                "unit": "count",
                "value": 2000,
                "startDate": "2026-02-23T10:00:00Z",
                "endDate": "2026-02-23T10:15:00Z",
                "metadata_steps_mode": "walking",
            },
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "sourceName": "iPhone",
                "unit": "count",
                "value": 2000,
                "startDate": "2026-02-23T10:00:00Z",
                "endDate": "2026-02-23T10:15:00Z",
                "metadata_steps_mode": "running",
            },
        ]

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 1)
        self.assertEqual(result["workouts_ingested"], 0)

        con = duckdb.connect(":memory:")
        ingested = con.execute(
            """
            SELECT value, metadata_json
            FROM read_parquet(?)
            WHERE type = 'HKQuantityTypeIdentifierStepCount'
            """,
            [str(self.curated_dir / "health_records.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual(len(ingested), 1)
        self.assertEqual(ingested[0][0], "2000")
        self.assertEqual(json.loads(ingested[0][1]), {"metadata_steps_mode": "walking"})

    def test_ingest_payload_deduplicates_workout_batch_when_only_non_key_fields_differ(self):
        payload = {
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 50,
                    "durationUnit": "min",
                    "totalDistance": 4.0,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 250,
                    "totalEnergyBurnedUnit": "kcal",
                    "sourceName": "Apple Watch",
                    "startDate": "2026-02-22T18:00:00Z",
                    "endDate": "2026-02-22T18:50:00Z",
                    "creationDate": "2026-02-22T18:50:01Z",
                    "metadata": {"variant": "first"},
                },
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 50,
                    "durationUnit": "minutes",
                    "totalDistance": 4.0,
                    "totalDistanceUnit": "mi",
                    "totalEnergyBurned": 250,
                    "totalEnergyBurnedUnit": "kilocalorie",
                    "sourceName": "Apple Watch",
                    "startDate": "2026-02-22T18:00:00Z",
                    "endDate": "2026-02-22T18:50:00Z",
                    "creationDate": "2026-02-22T18:50:59Z",
                    "metadata": {"variant": "second"},
                },
            ]
        }

        result = self.ingestor.ingest_payload(payload)
        self.assertEqual(result["records_ingested"], 0)
        self.assertEqual(result["workouts_ingested"], 1)

        con = duckdb.connect(":memory:")
        ingested = con.execute(
            """
            SELECT durationUnit, totalDistanceUnit, totalEnergyBurnedUnit, metadata_json
            FROM read_parquet(?)
            WHERE workoutActivityType = 'HKWorkoutActivityTypeWalking'
            """,
            [str(self.curated_dir / "health_workouts.parquet")],
        ).fetchall()
        con.close()

        self.assertEqual(len(ingested), 1)
        self.assertEqual(ingested[0][0], "min")
        self.assertEqual(ingested[0][1], "km")
        self.assertEqual(ingested[0][2], "kcal")
        self.assertEqual(json.loads(ingested[0][3]), {"variant": "first"})

    @unittest.skipIf(health_auto_export.fcntl is None, "fcntl not available")
    def test_ingest_payload_uses_process_file_lock(self):
        payload = self.sample_payload()

        with mock.patch.object(health_auto_export.fcntl, "flock") as mock_flock:
            self.ingestor.ingest_payload(payload)

        lock_calls = [
            call
            for call in mock_flock.call_args_list
            if call.args and call.args[1] == health_auto_export.fcntl.LOCK_EX
        ]
        unlock_calls = [
            call
            for call in mock_flock.call_args_list
            if call.args and call.args[1] == health_auto_export.fcntl.LOCK_UN
        ]

        self.assertEqual(len(lock_calls), 1)
        self.assertEqual(len(unlock_calls), 1)

    @unittest.skipIf(health_auto_export.fcntl is None, "fcntl not available")
    def test_acquire_process_merge_lock_creates_lock_dir(self):
        lock_dir = self.curated_dir / "nested" / "curated"
        ingestor = health_auto_export.HealthAutoExportIngestor(
            raw_dir=self.raw_dir,
            curated_dir=lock_dir,
        )

        self.assertFalse(lock_dir.exists())
        with ingestor._acquire_process_merge_lock():
            self.assertTrue(lock_dir.exists())
            self.assertTrue((lock_dir / ".parquet_merge.lock").exists())

    def test_acquire_process_merge_lock_uses_msvcrt_when_fcntl_missing(self):
        mock_msvcrt = mock.Mock()
        mock_msvcrt.LK_LOCK = 1
        mock_msvcrt.LK_UNLCK = 2

        with (
            mock.patch.object(health_auto_export, "fcntl", None),
            mock.patch.object(health_auto_export, "msvcrt", mock_msvcrt),
        ):
            with self.ingestor._acquire_process_merge_lock():
                pass

        self.assertEqual(mock_msvcrt.locking.call_count, 2)
        lock_call = mock_msvcrt.locking.call_args_list[0]
        unlock_call = mock_msvcrt.locking.call_args_list[1]
        self.assertEqual(lock_call.args[1], mock_msvcrt.LK_LOCK)
        self.assertEqual(unlock_call.args[1], mock_msvcrt.LK_UNLCK)


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

    def test_api_deduplicates_duplicates_within_single_payload(self):
        payload = {
            "records": [
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 111,
                    "startDate": "2026-02-23T09:00:00Z",
                    "endDate": "2026-02-23T09:15:00Z",
                },
                {
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "sourceName": "iPhone",
                    "unit": "count",
                    "value": 111,
                    "startDate": "2026-02-23T09:00:00Z",
                    "endDate": "2026-02-23T09:15:00Z",
                },
            ],
            "workouts": [
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 25,
                    "durationUnit": "min",
                    "totalDistance": 1.8,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 140,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-23T09:30:00Z",
                    "endDate": "2026-02-23T09:55:00Z",
                },
                {
                    "workoutActivityType": "HKWorkoutActivityTypeWalking",
                    "duration": 25,
                    "durationUnit": "min",
                    "totalDistance": 1.8,
                    "totalDistanceUnit": "km",
                    "totalEnergyBurned": 140,
                    "totalEnergyBurnedUnit": "kcal",
                    "startDate": "2026-02-23T09:30:00Z",
                    "endDate": "2026-02-23T09:55:00Z",
                },
            ],
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
        self.assertEqual(body["workouts_ingested"], 1)


class TestHealthAutoExportCLI(unittest.TestCase):
    """CLI tests for ingest-file mode."""

    def test_ingest_file_load_error(self):
        with self.assertRaises(ValueError):
            health_auto_export._load_json_file(Path("/tmp/does-not-exist.json"))


if __name__ == "__main__":
    unittest.main()
