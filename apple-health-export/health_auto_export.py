#!/usr/bin/env python3
"""Health Auto Export ingestion service and CLI for Apple Health data."""

import argparse
import contextlib
import json
import secrets
import sys
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - import guard
    raise RuntimeError("duckdb is required for health_auto_export") from exc

try:
    import fcntl
except ImportError:  # pragma: no cover - not available on Windows
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt
except ImportError:  # pragma: no cover - not available on POSIX
    msvcrt = None  # type: ignore[assignment]

try:
    from flask import Flask, jsonify, request
    HAS_FLASK = True
except ImportError:  # pragma: no cover - optional dependency in some environments
    Flask = Any  # type: ignore[misc,assignment]
    HAS_FLASK = False

DEFAULT_RAW_DIR = Path.home() / "datalake.me" / "raw" / "apple-health" / "auto-export"
DEFAULT_CURATED_DIR = Path.home() / "datalake.me" / "curated" / "apple-health"
_PARQUET_MERGE_LOCKS: dict[Path, threading.RLock] = {}
_PARQUET_MERGE_LOCKS_GUARD = threading.Lock()


def _get_parquet_merge_lock(curated_dir: Path) -> threading.RLock:
    lock_key = curated_dir.expanduser().resolve()
    with _PARQUET_MERGE_LOCKS_GUARD:
        lock = _PARQUET_MERGE_LOCKS.get(lock_key)
        if lock is None:
            lock = threading.RLock()
            _PARQUET_MERGE_LOCKS[lock_key] = lock
        return lock


def _parse_datetime(value: str | None) -> str | None:
    if not value:
        return None

    dt_value = value.strip()
    if not dt_value:
        return None

    for parser in (
        lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S %z"),
    ):
        try:
            return parser(dt_value).astimezone(UTC).isoformat()
        except ValueError:
            continue

    return None


class HealthAutoExportIngestor:
    """Persist Health Auto Export payloads to raw and curated parquet."""

    # Incoming record duplicates are identified using only these fields.
    RECORD_DEDUPE_KEY_FIELDS: tuple[str, ...] = (
        "type",
        "sourceName",
        "unit",
        "value",
        "startDate",
        "endDate",
    )
    # Incoming workout duplicates are identified using only these fields.
    WORKOUT_DEDUPE_KEY_FIELDS: tuple[str, ...] = (
        "workoutActivityType",
        "sourceName",
        "startDate",
        "endDate",
        "duration",
        "totalDistance",
        "totalEnergyBurned",
    )

    def __init__(self, raw_dir: Path = DEFAULT_RAW_DIR, curated_dir: Path = DEFAULT_CURATED_DIR):
        self.raw_dir = Path(raw_dir)
        self.curated_dir = Path(curated_dir).expanduser().resolve()
        self.records_parquet = self.curated_dir / "health_records.parquet"
        self.workouts_parquet = self.curated_dir / "health_workouts.parquet"
        self._parquet_merge_lock_file = self.curated_dir / ".parquet_merge.lock"
        self._parquet_merge_lock = _get_parquet_merge_lock(self.curated_dir)

    def ingest_payload(self, payload: Any, request_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        records, workouts, errors = self._normalize_payload(payload)
        if errors:
            raise ValueError("; ".join(errors))

        raw_path = self._write_raw_payload(payload, request_metadata=request_metadata)
        result = self._merge_to_parquet(records, workouts, raw_path)
        result["raw_path"] = str(raw_path)
        return result

    def _write_raw_payload(self, payload: Any, request_metadata: dict[str, Any] | None = None) -> Path:
        now = datetime.now(UTC)
        raw_day_dir = self.raw_dir / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d")
        raw_day_dir.mkdir(parents=True, exist_ok=True)

        raw_file = raw_day_dir / f"health_auto_export_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex}.json"
        envelope = {
            "received_at": now.isoformat(),
            "request_metadata": request_metadata or {},
            "payload": payload,
        }
        raw_file.write_text(json.dumps(envelope, ensure_ascii=True), encoding="utf-8")
        return raw_file

    def _normalize_payload(self, payload: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
        errors: list[str] = []

        if isinstance(payload, list):
            raw_records = payload
            raw_workouts: list[dict[str, Any]] = []
        elif isinstance(payload, dict):
            raw_records = self._extract_list(payload, ["records", "samples", "items", "data.records"])
            raw_workouts = self._extract_list(payload, ["workouts", "data.workouts"])

            if not raw_records and not raw_workouts and self._looks_like_record(payload):
                raw_records = [payload]

            if not raw_records and not raw_workouts:
                errors.append("payload does not include records or workouts")
        else:
            errors.append("payload must be an object or list")
            return [], [], errors

        records = [self._normalize_record(item) for item in raw_records if isinstance(item, dict)]
        workouts = [self._normalize_workout(item) for item in raw_workouts if isinstance(item, dict)]

        deduped_records, deduped_workouts = self._dedupe_normalized_batches(records, workouts)

        valid_records = [r for r in deduped_records if r["type"] and r["startDate"]]
        valid_workouts = [w for w in deduped_workouts if w["workoutActivityType"] and w["startDate"]]

        invalid_records = len(deduped_records) - len(valid_records)
        invalid_workouts = len(deduped_workouts) - len(valid_workouts)
        if invalid_records:
            errors.append(f"{invalid_records} record(s) missing required fields")
        if invalid_workouts:
            errors.append(f"{invalid_workouts} workout(s) missing required fields")

        if not valid_records and not valid_workouts:
            errors.append("no valid records or workouts found")

        return valid_records, valid_workouts, errors

    @staticmethod
    def _extract_list(payload: dict[str, Any], paths: list[str]) -> list[dict[str, Any]]:
        for path in paths:
            current: Any = payload
            for key in path.split("."):
                if not isinstance(current, dict):
                    current = None
                    break
                current = current.get(key)
            if isinstance(current, list):
                return current
        return []

    @staticmethod
    def _looks_like_record(payload: dict[str, Any]) -> bool:
        keys = {"type", "startDate", "value", "workoutActivityType"}
        return bool(keys.intersection(payload.keys()))

    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        metadata = record.get("metadata")
        if metadata is None:
            metadata = {k: v for k, v in record.items() if str(k).startswith("metadata_")}

        return {
            "type": record.get("type") or record.get("recordType") or record.get("metric"),
            "sourceName": record.get("sourceName") or record.get("source") or "Health Auto Export",
            "sourceVersion": record.get("sourceVersion") or "",
            "unit": record.get("unit") or "",
            "value": str(record.get("value", "")),
            "startDate": _parse_datetime(record.get("startDate") or record.get("dateFrom") or record.get("date")),
            "endDate": _parse_datetime(record.get("endDate") or record.get("dateTo") or record.get("startDate")),
            "creationDate": _parse_datetime(record.get("creationDate") or record.get("createdAt") or record.get("startDate")),
            "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True) if isinstance(metadata, dict) else "{}",
        }

    def _normalize_workout(self, workout: dict[str, Any]) -> dict[str, Any]:
        metadata = workout.get("metadata")
        if metadata is None:
            metadata = {k: v for k, v in workout.items() if str(k).startswith("metadata_")}

        return {
            "workoutActivityType": workout.get("workoutActivityType") or workout.get("type") or "",
            "duration": str(workout.get("duration", "")),
            "durationUnit": workout.get("durationUnit") or "min",
            "totalDistance": str(workout.get("totalDistance", "")),
            "totalDistanceUnit": workout.get("totalDistanceUnit") or "",
            "totalEnergyBurned": str(workout.get("totalEnergyBurned", "")),
            "totalEnergyBurnedUnit": workout.get("totalEnergyBurnedUnit") or "",
            "sourceName": workout.get("sourceName") or workout.get("source") or "Health Auto Export",
            "startDate": _parse_datetime(workout.get("startDate") or workout.get("dateFrom") or workout.get("date")),
            "endDate": _parse_datetime(workout.get("endDate") or workout.get("dateTo") or workout.get("startDate")),
            "creationDate": _parse_datetime(workout.get("creationDate") or workout.get("createdAt") or workout.get("startDate")),
            "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True) if isinstance(metadata, dict) else "{}",
        }

    def _merge_to_parquet(self, records: list[dict[str, Any]], workouts: list[dict[str, Any]], raw_path: Path) -> dict[str, Any]:
        # Serialize the full merge lifecycle so parallel requests cannot clobber
        # each other's parquet read-modify-write updates.
        with self._acquire_merge_lock():
            self.curated_dir.mkdir(parents=True, exist_ok=True)
            # Defensive dedupe: keep merge behavior safe even if future callers
            # bypass _normalize_payload.
            records = self._dedupe_incoming_records_batch(records)
            workouts = self._dedupe_incoming_workouts_batch(workouts)
            ingested_at = datetime.now(UTC).isoformat()

            records_with_lineage = [
                {
                    **row,
                    "ingestionSource": "health_auto_export",
                    "rawFile": str(raw_path),
                    "ingestedAt": ingested_at,
                }
                for row in records
            ]
            workouts_with_lineage = [
                {
                    **row,
                    "ingestionSource": "health_auto_export",
                    "rawFile": str(raw_path),
                    "ingestedAt": ingested_at,
                }
                for row in workouts
            ]

            with duckdb.connect(":memory:") as con:
                con.execute("BEGIN TRANSACTION")
                try:
                    records_tmp_path = self._write_records_parquet(con, records_with_lineage)
                    workouts_tmp_path = self._write_workouts_parquet(con, workouts_with_lineage)
                    con.execute("COMMIT")
                except Exception:
                    con.execute("ROLLBACK")
                    raise
            self._promote_parquet_outputs(records_tmp_path, workouts_tmp_path)

        return {
            "records_ingested": len(records_with_lineage),
            "workouts_ingested": len(workouts_with_lineage),
            "records_parquet": str(self.records_parquet),
            "workouts_parquet": str(self.workouts_parquet),
        }

    @contextlib.contextmanager
    def _acquire_merge_lock(self):
        with self._parquet_merge_lock, self._acquire_process_merge_lock():
            yield

    @contextlib.contextmanager
    def _acquire_process_merge_lock(self):
        self._parquet_merge_lock_file.parent.mkdir(parents=True, exist_ok=True)
        with self._parquet_merge_lock_file.open("a+", encoding="utf-8") as lock_file:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                return

            if msvcrt is not None:
                # Lock a single byte for cross-process serialization on Windows.
                lock_file.seek(0)
                if lock_file.read(1) == "":
                    lock_file.seek(0)
                    lock_file.write("\0")
                    lock_file.flush()
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                return

            yield

    @staticmethod
    def _dedupe_incoming_records_batch(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return HealthAutoExportIngestor._dedupe_incoming_batch(
            records,
            lambda row: tuple(row.get(field) for field in HealthAutoExportIngestor.RECORD_DEDUPE_KEY_FIELDS),
        )

    @staticmethod
    def _dedupe_incoming_workouts_batch(workouts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return HealthAutoExportIngestor._dedupe_incoming_batch(
            workouts,
            lambda row: tuple(row.get(field) for field in HealthAutoExportIngestor.WORKOUT_DEDUPE_KEY_FIELDS),
        )

    @staticmethod
    def _dedupe_normalized_batches(
        records: list[dict[str, Any]],
        workouts: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return (
            HealthAutoExportIngestor._dedupe_incoming_records_batch(records),
            HealthAutoExportIngestor._dedupe_incoming_workouts_batch(workouts),
        )

    @staticmethod
    def _dedupe_incoming_batch(
        rows: list[dict[str, Any]],
        key_builder: Callable[[dict[str, Any]], tuple[Any, ...]],
    ) -> list[dict[str, Any]]:
        deduped_rows: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()
        for row in rows:
            key = tuple(HealthAutoExportIngestor._canonicalize_dedupe_value(value) for value in key_builder(row))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_rows.append(row)
        return deduped_rows

    @staticmethod
    def _canonicalize_dedupe_value(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return value

    def _promote_parquet_outputs(self, records_tmp_path: Path, workouts_tmp_path: Path) -> None:
        records_backup_path = self.records_parquet.with_name(
            f"{self.records_parquet.name}.{uuid.uuid4().hex}.bak"
        )
        workouts_backup_path = self.workouts_parquet.with_name(
            f"{self.workouts_parquet.name}.{uuid.uuid4().hex}.bak"
        )

        had_records_backup = False
        had_workouts_backup = False
        records_promoted = False
        workouts_promoted = False

        try:
            if self.records_parquet.exists():
                self.records_parquet.replace(records_backup_path)
                had_records_backup = True

            if self.workouts_parquet.exists():
                self.workouts_parquet.replace(workouts_backup_path)
                had_workouts_backup = True

            records_tmp_path.replace(self.records_parquet)
            records_promoted = True
            workouts_tmp_path.replace(self.workouts_parquet)
            workouts_promoted = True
        except Exception:
            if records_promoted and self.records_parquet.exists():
                self.records_parquet.unlink()
            if workouts_promoted and self.workouts_parquet.exists():
                self.workouts_parquet.unlink()

            if had_records_backup and records_backup_path.exists():
                records_backup_path.replace(self.records_parquet)
            if had_workouts_backup and workouts_backup_path.exists():
                workouts_backup_path.replace(self.workouts_parquet)
            raise
        finally:
            if records_tmp_path.exists():
                records_tmp_path.unlink()
            if workouts_tmp_path.exists():
                workouts_tmp_path.unlink()
            if had_records_backup and records_backup_path.exists():
                records_backup_path.unlink()
            if had_workouts_backup and workouts_backup_path.exists():
                workouts_backup_path.unlink()

    def _write_records_parquet(self, con: duckdb.DuckDBPyConnection, records: list[dict[str, Any]]) -> Path:
        con.execute(
            """
            CREATE TEMP TABLE incoming_records (
                type VARCHAR,
                sourceName VARCHAR,
                sourceVersion VARCHAR,
                unit VARCHAR,
                value VARCHAR,
                startDate TIMESTAMPTZ,
                endDate TIMESTAMPTZ,
                creationDate TIMESTAMPTZ,
                metadata_json VARCHAR,
                ingestionSource VARCHAR,
                rawFile VARCHAR,
                ingestedAt TIMESTAMPTZ,
                ingestOrdinal BIGINT
            )
            """
        )

        if records:
            con.executemany(
                """
                INSERT INTO incoming_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["type"],
                        r["sourceName"],
                        r["sourceVersion"],
                        r["unit"],
                        r["value"],
                        r["startDate"],
                        r["endDate"],
                        r["creationDate"],
                        r["metadata_json"],
                        r["ingestionSource"],
                        r["rawFile"],
                        r["ingestedAt"],
                        idx,
                    )
                    for idx, r in enumerate(records)
                ],
            )

        con.execute(
            f"""
            CREATE TEMP TABLE deduped_incoming_records AS
            SELECT * EXCLUDE (row_num)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            {", ".join(self.RECORD_DEDUPE_KEY_FIELDS)}
                        ORDER BY ingestOrdinal ASC
                    ) AS row_num
                FROM incoming_records
            )
            WHERE row_num = 1
            """
        )

        if self.records_parquet.exists():
            con.execute(
                "CREATE TEMP TABLE existing_records AS SELECT * FROM read_parquet(?)",
                [str(self.records_parquet)],
            )
            con.execute(
                "CREATE TEMP TABLE merged_records AS SELECT * FROM existing_records UNION ALL SELECT * FROM deduped_incoming_records"
            )
        else:
            con.execute("CREATE TEMP TABLE merged_records AS SELECT * FROM deduped_incoming_records")

        con.execute(
            f"""
            CREATE TEMP TABLE deduped_records AS
            SELECT * EXCLUDE (row_num)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            {", ".join(self.RECORD_DEDUPE_KEY_FIELDS)}
                        ORDER BY ingestedAt DESC, rawFile DESC, ingestOrdinal DESC
                    ) AS row_num
                FROM merged_records
            )
            WHERE row_num = 1
            """
        )
        records_tmp_path = self.records_parquet.with_name(
            f"{self.records_parquet.name}.{uuid.uuid4().hex}.tmp"
        )
        con.execute(
            "COPY deduped_records TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(records_tmp_path)],
        )
        return records_tmp_path

    def _write_workouts_parquet(self, con: duckdb.DuckDBPyConnection, workouts: list[dict[str, Any]]) -> Path:
        con.execute(
            """
            CREATE TEMP TABLE incoming_workouts (
                workoutActivityType VARCHAR,
                duration VARCHAR,
                durationUnit VARCHAR,
                totalDistance VARCHAR,
                totalDistanceUnit VARCHAR,
                totalEnergyBurned VARCHAR,
                totalEnergyBurnedUnit VARCHAR,
                sourceName VARCHAR,
                startDate TIMESTAMPTZ,
                endDate TIMESTAMPTZ,
                creationDate TIMESTAMPTZ,
                metadata_json VARCHAR,
                ingestionSource VARCHAR,
                rawFile VARCHAR,
                ingestedAt TIMESTAMPTZ,
                ingestOrdinal BIGINT
            )
            """
        )

        if workouts:
            con.executemany(
                """
                INSERT INTO incoming_workouts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        w["workoutActivityType"],
                        w["duration"],
                        w["durationUnit"],
                        w["totalDistance"],
                        w["totalDistanceUnit"],
                        w["totalEnergyBurned"],
                        w["totalEnergyBurnedUnit"],
                        w["sourceName"],
                        w["startDate"],
                        w["endDate"],
                        w["creationDate"],
                        w["metadata_json"],
                        w["ingestionSource"],
                        w["rawFile"],
                        w["ingestedAt"],
                        idx,
                    )
                    for idx, w in enumerate(workouts)
                ],
            )

        con.execute(
            f"""
            CREATE TEMP TABLE deduped_incoming_workouts AS
            SELECT * EXCLUDE (row_num)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            {", ".join(self.WORKOUT_DEDUPE_KEY_FIELDS)}
                        ORDER BY ingestOrdinal ASC
                    ) AS row_num
                FROM incoming_workouts
            )
            WHERE row_num = 1
            """
        )

        if self.workouts_parquet.exists():
            con.execute(
                "CREATE TEMP TABLE existing_workouts AS SELECT * FROM read_parquet(?)",
                [str(self.workouts_parquet)],
            )
            con.execute(
                "CREATE TEMP TABLE merged_workouts AS SELECT * FROM existing_workouts UNION ALL SELECT * FROM deduped_incoming_workouts"
            )
        else:
            con.execute("CREATE TEMP TABLE merged_workouts AS SELECT * FROM deduped_incoming_workouts")

        con.execute(
            f"""
            CREATE TEMP TABLE deduped_workouts AS
            SELECT * EXCLUDE (row_num)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            {", ".join(self.WORKOUT_DEDUPE_KEY_FIELDS)}
                        ORDER BY ingestedAt DESC, rawFile DESC, ingestOrdinal DESC
                    ) AS row_num
                FROM merged_workouts
            )
            WHERE row_num = 1
            """
        )
        workouts_tmp_path = self.workouts_parquet.with_name(
            f"{self.workouts_parquet.name}.{uuid.uuid4().hex}.tmp"
        )
        con.execute(
            "COPY deduped_workouts TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(workouts_tmp_path)],
        )
        return workouts_tmp_path


def create_app(ingestor: HealthAutoExportIngestor, token: str | None = None) -> Flask:
    """Create Flask app for receiving Health Auto Export POST payloads."""
    if not HAS_FLASK:
        raise RuntimeError("flask is required to run the API server")

    app = Flask(__name__)

    @app.get("/health")
    def health_check():
        return jsonify({"status": "ok"})

    @app.post("/v1/health/auto-export")
    def ingest_auto_export():
        if token:
            auth_header = request.headers.get("Authorization", "")
            expected = f"Bearer {token}"
            if not secrets.compare_digest(auth_header, expected):
                return jsonify({"error": "unauthorized"}), 401

        payload = request.get_json(silent=True)
        if payload is None:
            return jsonify({"error": "request body must be valid JSON"}), 400

        request_meta = {
            "remote_addr": request.remote_addr,
            "user_agent": request.headers.get("User-Agent", ""),
        }

        try:
            result = ingestor.ingest_payload(payload, request_metadata=request_meta)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 201

    return app


def _load_json_file(file_path: Path) -> Any:
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"failed to load JSON file {file_path}: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Apple Health Auto Export payloads")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest-file", help="Ingest a JSON payload from file")
    ingest_parser.add_argument("--file", required=True, help="Path to Health Auto Export JSON payload")
    ingest_parser.add_argument("--raw-dir", help="Raw datalake root directory")
    ingest_parser.add_argument("--curated-dir", help="Curated datalake directory")

    serve_parser = subparsers.add_parser("serve", help="Run REST API endpoint for Health Auto Export")
    serve_parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    serve_parser.add_argument("--port", type=int, default=8787, help="Bind port")
    serve_parser.add_argument("--token", help="Bearer token required by endpoint")
    serve_parser.add_argument("--raw-dir", help="Raw datalake root directory")
    serve_parser.add_argument("--curated-dir", help="Curated datalake directory")

    args = parser.parse_args()

    ingestor = HealthAutoExportIngestor(
        raw_dir=Path(args.raw_dir) if args.raw_dir else DEFAULT_RAW_DIR,
        curated_dir=Path(args.curated_dir) if args.curated_dir else DEFAULT_CURATED_DIR,
    )

    if args.command == "ingest-file":
        payload = _load_json_file(Path(args.file))
        result = ingestor.ingest_payload(payload, request_metadata={"source": "ingest-file"})
        print(json.dumps(result, indent=2))
        return

    app = create_app(ingestor, token=args.token)
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
