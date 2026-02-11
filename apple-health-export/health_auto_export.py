#!/usr/bin/env python3
"""Health Auto Export ingestion server for Apple Health data."""

import argparse
import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

try:
    from flask import Flask, jsonify, request
    HAS_FLASK = True
except ModuleNotFoundError:  # pragma: no cover - exercised only in constrained envs
    Flask = Any  # type: ignore[assignment]
    jsonify = None
    request = None
    HAS_FLASK = False


RECORD_FIELDS = [
    "type",
    "sourceName",
    "sourceVersion",
    "unit",
    "value",
    "startDate",
    "endDate",
    "creationDate",
    "device",
]

WORKOUT_FIELDS = [
    "workoutActivityType",
    "duration",
    "durationUnit",
    "totalDistance",
    "totalDistanceUnit",
    "totalEnergyBurned",
    "totalEnergyBurnedUnit",
    "sourceName",
    "startDate",
    "endDate",
    "creationDate",
]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _metadata_to_text(metadata: Any) -> str:
    if metadata is None:
        return ""
    if isinstance(metadata, str):
        return metadata
    return json.dumps(metadata, separators=(",", ":"), sort_keys=True)


def _record_hash(item: Dict[str, str]) -> str:
    key = "|".join(
        [
            item.get("type", ""),
            item.get("startDate", ""),
            item.get("endDate", ""),
            item.get("value", ""),
            item.get("unit", ""),
            item.get("sourceName", ""),
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _workout_hash(item: Dict[str, str]) -> str:
    key = "|".join(
        [
            item.get("workoutActivityType", ""),
            item.get("startDate", ""),
            item.get("endDate", ""),
            item.get("totalDistance", ""),
            item.get("totalEnergyBurned", ""),
            item.get("sourceName", ""),
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _classify_items(payload: Any) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if isinstance(payload, dict):
        if "records" in payload or "workouts" in payload:
            records = payload.get("records", [])
            workouts = payload.get("workouts", [])
            if not isinstance(records, list) or not isinstance(workouts, list):
                raise ValueError("'records' and 'workouts' must be lists")
            return records, workouts

        if isinstance(payload.get("data"), list):
            candidates = payload["data"]
        else:
            candidates = [payload]
    elif isinstance(payload, list):
        candidates = payload
    else:
        raise ValueError("JSON payload must be an object or array")

    records: List[Dict[str, Any]] = []
    workouts: List[Dict[str, Any]] = []

    for item in candidates:
        if not isinstance(item, dict):
            raise ValueError("Each payload item must be an object")
        if "workoutActivityType" in item:
            workouts.append(item)
        else:
            records.append(item)

    return records, workouts


def _normalize_payload(payload: Any) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    raw_records, raw_workouts = _classify_items(payload)

    errors: List[str] = []
    normalized_records: List[Dict[str, str]] = []
    normalized_workouts: List[Dict[str, str]] = []

    for index, record in enumerate(raw_records):
        required = ["type", "startDate", "endDate"]
        missing = [field for field in required if not _as_text(record.get(field)).strip()]
        if missing:
            errors.append(f"records[{index}] missing required field(s): {', '.join(missing)}")
            continue

        normalized = {field: _as_text(record.get(field)) for field in RECORD_FIELDS}
        normalized["metadata_json"] = _metadata_to_text(record.get("metadata"))
        normalized["record_hash"] = _record_hash(normalized)
        normalized_records.append(normalized)

    for index, workout in enumerate(raw_workouts):
        required = ["workoutActivityType", "startDate", "endDate"]
        missing = [field for field in required if not _as_text(workout.get(field)).strip()]
        if missing:
            errors.append(f"workouts[{index}] missing required field(s): {', '.join(missing)}")
            continue

        normalized = {field: _as_text(workout.get(field)) for field in WORKOUT_FIELDS}
        normalized["metadata_json"] = _metadata_to_text(workout.get("metadata"))
        normalized["workout_hash"] = _workout_hash(normalized)
        normalized_workouts.append(normalized)

    if errors:
        raise ValueError("; ".join(errors))

    if not normalized_records and not normalized_workouts:
        raise ValueError("Payload contained no records or workouts")

    return normalized_records, normalized_workouts


def _write_ndjson(path: Path, rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True))
            handle.write("\n")


def _merge_parquet(
    con: duckdb.DuckDBPyConnection,
    incoming_file: Path,
    parquet_file: Path,
    hash_column: str,
    select_sql: str,
) -> int:
    con.execute(f"CREATE OR REPLACE TEMP TABLE incoming AS {select_sql}", [str(incoming_file)])

    if parquet_file.exists():
        con.execute("CREATE OR REPLACE TEMP TABLE existing AS SELECT * FROM read_parquet(?)", [str(parquet_file)])
        con.execute("CREATE OR REPLACE TEMP TABLE merged AS SELECT * FROM existing UNION ALL SELECT * FROM incoming")
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE merged AS SELECT * FROM incoming")

    copy_sql = f"""
        COPY (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {hash_column}
                    ORDER BY ingested_at DESC
                ) AS rn
                FROM merged
            )
            WHERE rn = 1
        ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    con.execute(copy_sql, [str(parquet_file)])
    total = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(parquet_file)]).fetchone()[0]

    con.execute("DROP TABLE IF EXISTS incoming")
    con.execute("DROP TABLE IF EXISTS existing")
    con.execute("DROP TABLE IF EXISTS merged")
    return int(total)


def _count_parquet_rows(con: duckdb.DuckDBPyConnection, parquet_file: Path) -> int:
    if not parquet_file.exists():
        return 0
    total = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(parquet_file)]).fetchone()[0]
    return int(total)


def ingest_payload(payload: Any, raw_root: Path, curated_root: Path) -> Dict[str, Any]:
    raw_root = Path(raw_root)
    curated_root = Path(curated_root)

    records, workouts = _normalize_payload(payload)

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    batch_id = f"{timestamp}_{uuid.uuid4().hex[:8]}"

    raw_dir = raw_root / "health-auto-export" / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d")
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_payload_path = raw_dir / f"{batch_id}.json"

    with raw_payload_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")

    ingested_at = now.isoformat()
    for record in records:
        record["batch_id"] = batch_id
        record["ingested_at"] = ingested_at
        record["raw_payload_path"] = str(raw_payload_path)

    for workout in workouts:
        workout["batch_id"] = batch_id
        workout["ingested_at"] = ingested_at
        workout["raw_payload_path"] = str(raw_payload_path)

    curated_root.mkdir(parents=True, exist_ok=True)

    record_count_after_merge = 0
    workout_count_after_merge = 0

    db_path = curated_root / "_health_auto_export.duckdb"
    records_parquet = curated_root / "health_records.parquet"
    workouts_parquet = curated_root / "workouts.parquet"

    with duckdb.connect(str(db_path)) as con:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            if records:
                records_ndjson = tmp_path / "records.ndjson"
                _write_ndjson(records_ndjson, records)
                record_count_after_merge = _merge_parquet(
                    con,
                    records_ndjson,
                    records_parquet,
                    hash_column="record_hash",
                    select_sql="""
                        SELECT
                            CAST(record_hash AS VARCHAR) AS record_hash,
                            CAST(type AS VARCHAR) AS type,
                            CAST(sourceName AS VARCHAR) AS sourceName,
                            CAST(sourceVersion AS VARCHAR) AS sourceVersion,
                            CAST(unit AS VARCHAR) AS unit,
                            CAST(value AS VARCHAR) AS value,
                            CAST(startDate AS VARCHAR) AS startDate,
                            CAST(endDate AS VARCHAR) AS endDate,
                            CAST(creationDate AS VARCHAR) AS creationDate,
                            CAST(device AS VARCHAR) AS device,
                            CAST(metadata_json AS VARCHAR) AS metadata_json,
                            CAST(batch_id AS VARCHAR) AS batch_id,
                            CAST(ingested_at AS TIMESTAMPTZ) AS ingested_at,
                            CAST(raw_payload_path AS VARCHAR) AS raw_payload_path
                        FROM read_ndjson_auto(?)
                    """,
                )

            if workouts:
                workouts_ndjson = tmp_path / "workouts.ndjson"
                _write_ndjson(workouts_ndjson, workouts)
                workout_count_after_merge = _merge_parquet(
                    con,
                    workouts_ndjson,
                    workouts_parquet,
                    hash_column="workout_hash",
                    select_sql="""
                        SELECT
                            CAST(workout_hash AS VARCHAR) AS workout_hash,
                            CAST(workoutActivityType AS VARCHAR) AS workoutActivityType,
                            CAST(duration AS VARCHAR) AS duration,
                            CAST(durationUnit AS VARCHAR) AS durationUnit,
                            CAST(totalDistance AS VARCHAR) AS totalDistance,
                            CAST(totalDistanceUnit AS VARCHAR) AS totalDistanceUnit,
                            CAST(totalEnergyBurned AS VARCHAR) AS totalEnergyBurned,
                            CAST(totalEnergyBurnedUnit AS VARCHAR) AS totalEnergyBurnedUnit,
                            CAST(sourceName AS VARCHAR) AS sourceName,
                            CAST(startDate AS VARCHAR) AS startDate,
                            CAST(endDate AS VARCHAR) AS endDate,
                            CAST(creationDate AS VARCHAR) AS creationDate,
                            CAST(metadata_json AS VARCHAR) AS metadata_json,
                            CAST(batch_id AS VARCHAR) AS batch_id,
                            CAST(ingested_at AS TIMESTAMPTZ) AS ingested_at,
                            CAST(raw_payload_path AS VARCHAR) AS raw_payload_path
                        FROM read_ndjson_auto(?)
                    """,
                )

            if not records:
                record_count_after_merge = _count_parquet_rows(con, records_parquet)
            if not workouts:
                workout_count_after_merge = _count_parquet_rows(con, workouts_parquet)

    return {
        "batch_id": batch_id,
        "raw_payload_path": str(raw_payload_path),
        "records_received": len(records),
        "workouts_received": len(workouts),
        "health_records_total": record_count_after_merge,
        "workouts_total": workout_count_after_merge,
    }


def _is_authorized(token: Optional[str]) -> bool:
    if not token:
        return True

    auth_header = request.headers.get("Authorization", "")
    api_key = request.headers.get("X-API-Key", "")

    if auth_header == f"Bearer {token}":
        return True
    if api_key == token:
        return True
    return False


def create_app(raw_root: Path, curated_root: Path, token: Optional[str] = None) -> Flask:
    if not HAS_FLASK:
        raise RuntimeError("Flask is required. Install dependencies and retry.")

    app = Flask(__name__)

    @app.get("/health-auto-export/v1/health")
    def healthcheck() -> Any:
        return jsonify({"status": "ok"})

    @app.post("/health-auto-export/v1/ingest")
    def ingest() -> Any:
        if not _is_authorized(token):
            return jsonify({"error": "Unauthorized"}), 401

        payload = request.get_json(silent=True)
        if payload is None:
            return jsonify({"error": "Invalid JSON payload"}), 400

        try:
            result = ingest_payload(payload, raw_root=Path(raw_root), curated_root=Path(curated_root))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 202

    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a Health Auto Export ingestion endpoint and write curated parquet files."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run HTTP ingestion server")
    serve_parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    serve_parser.add_argument("--port", default=8080, type=int, help="Bind port")
    serve_parser.add_argument(
        "--raw-dir",
        default=str(Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/datalake.me/raw/apple-health"),
        help="Root directory for immutable raw payload archives",
    )
    serve_parser.add_argument(
        "--curated-dir",
        default=str(Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/datalake.me/curated/apple-health"),
        help="Root directory for curated parquet outputs",
    )
    serve_parser.add_argument("--token", default=None, help="Static API token for Authorization header")
    serve_parser.add_argument(
        "--token-env",
        default="HEALTH_AUTO_EXPORT_TOKEN",
        help="Environment variable used if --token is omitted",
    )

    ingest_file_parser = subparsers.add_parser("ingest-file", help="Ingest one JSON payload from disk")
    ingest_file_parser.add_argument("payload_file", help="Path to JSON payload file")
    ingest_file_parser.add_argument(
        "--raw-dir",
        default=str(Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/datalake.me/raw/apple-health"),
    )
    ingest_file_parser.add_argument(
        "--curated-dir",
        default=str(Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/datalake.me/curated/apple-health"),
    )

    args = parser.parse_args()

    if args.command == "serve":
        token = args.token or os.getenv(args.token_env)
        app = create_app(raw_root=Path(args.raw_dir), curated_root=Path(args.curated_dir), token=token)
        app.run(host=args.host, port=args.port)
        return

    payload_path = Path(args.payload_file)
    if not payload_path.exists():
        raise FileNotFoundError(f"Payload file not found: {payload_path}")

    with payload_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    result = ingest_payload(payload, raw_root=Path(args.raw_dir), curated_root=Path(args.curated_dir))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
