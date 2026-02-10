#!/usr/bin/env python3
"""Health Auto Export ingestion server and parquet writer.

This module accepts JSON payloads sent from the Health Auto Export iOS app,
stores immutable raw payloads, and merges normalized records/workouts into
parquet files compatible with the existing Apple Health schema.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional, Tuple

try:
    import duckdb
except ImportError:  # pragma: no cover - covered indirectly by runtime checks
    duckdb = None


DEFAULT_RAW_DIR = Path.home() / "datalake.me" / "raw" / "apple-health" / "health-auto-export"
DEFAULT_CURATED_DIR = Path.home() / "datalake.me" / "datalake" / "curated" / "apple-health"


@dataclass
class IngestionResult:
    """Outcome of a single payload ingestion."""

    raw_file: Path
    records_received: int
    workouts_received: int
    records_written: int
    workouts_written: int


class PayloadValidationError(ValueError):
    """Raised when payload structure is invalid."""


class HealthAutoExportIngestor:
    """Ingest Health Auto Export payloads to immutable raw + curated parquet."""

    def __init__(self, raw_dir: Path, curated_dir: Path, enable_parquet: bool = True):
        self.raw_dir = Path(raw_dir)
        self.curated_dir = Path(curated_dir)
        self.enable_parquet = enable_parquet

    def ingest_payload(self, payload: Dict[str, Any]) -> IngestionResult:
        """Validate, archive, normalize, and merge a payload."""
        records, workouts = normalize_payload(payload)
        raw_file = self._archive_raw_payload(payload)

        records_written = 0
        workouts_written = 0
        if self.enable_parquet:
            records_written = merge_records_to_parquet(records, self.curated_dir / "health_records.parquet")
            workouts_written = merge_workouts_to_parquet(workouts, self.curated_dir / "health_workouts.parquet")

        return IngestionResult(
            raw_file=raw_file,
            records_received=len(records),
            workouts_received=len(workouts),
            records_written=records_written,
            workouts_written=workouts_written,
        )

    def _archive_raw_payload(self, payload: Dict[str, Any]) -> Path:
        """Store the incoming payload as immutable JSON."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"health_auto_export_{ts}_{secrets.token_hex(4)}.json"

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        raw_file = self.raw_dir / filename
        raw_file.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
        return raw_file


def _value_as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _first_non_empty(*values: Any, default: str = "") -> str:
    for value in values:
        text = _value_as_text(value).strip()
        if text:
            return text
    return default


def validate_api_key(headers: Dict[str, str], api_key: Optional[str]) -> bool:
    """Return True when request headers satisfy configured API key policy."""
    if not api_key:
        return True
    candidate = headers.get("X-API-Key", "")
    return candidate == api_key


def normalize_payload(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize Health Auto Export payload variants.

    Supports common shapes where records are under one of:
    - records
    - samples

    and workouts are under one of:
    - workouts
    - workoutSamples
    """
    if not isinstance(payload, dict):
        raise PayloadValidationError("Payload must be a JSON object")

    raw_records = payload.get("records", payload.get("samples", []))
    raw_workouts = payload.get("workouts", payload.get("workoutSamples", []))

    if not isinstance(raw_records, list):
        raise PayloadValidationError("Field 'records' must be a list")
    if not isinstance(raw_workouts, list):
        raise PayloadValidationError("Field 'workouts' must be a list")

    normalized_records: List[Dict[str, Any]] = []
    normalized_workouts: List[Dict[str, Any]] = []

    for item in raw_records:
        if not isinstance(item, dict):
            raise PayloadValidationError("Each record must be a JSON object")

        record_type = _first_non_empty(item.get("type"), item.get("dataType"))
        start_date = _first_non_empty(item.get("startDate"), item.get("start"))
        end_date = _first_non_empty(item.get("endDate"), item.get("end"), item.get("startDate"), item.get("start"))

        if not record_type or not start_date:
            raise PayloadValidationError("Every record must include type/dataType and startDate/start")

        normalized_records.append(
            {
                "type": record_type,
                "sourceName": _first_non_empty(item.get("sourceName"), item.get("source"), default="Health Auto Export"),
                "sourceVersion": _first_non_empty(item.get("sourceVersion")),
                "unit": _first_non_empty(item.get("unit")),
                "value": _first_non_empty(item.get("value")),
                "startDate": start_date,
                "endDate": end_date,
                "creationDate": _first_non_empty(item.get("creationDate"), item.get("dateAdded"), item.get("startDate")),
                "device": _first_non_empty(item.get("device")),
                "metadata_json": json.dumps(item.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                "ingestedAt": datetime.now(timezone.utc).isoformat(),
                "ingestionSource": "health_auto_export",
            }
        )

    for item in raw_workouts:
        if not isinstance(item, dict):
            raise PayloadValidationError("Each workout must be a JSON object")

        workout_type = _first_non_empty(
            item.get("workoutActivityType"),
            item.get("type"),
            item.get("activityType"),
            default="HKWorkoutActivityTypeOther",
        )
        start_date = _first_non_empty(item.get("startDate"), item.get("start"))
        end_date = _first_non_empty(item.get("endDate"), item.get("end"), item.get("startDate"), item.get("start"))

        if not start_date:
            raise PayloadValidationError("Every workout must include startDate/start")

        normalized_workouts.append(
            {
                "workoutActivityType": workout_type,
                "duration": _first_non_empty(item.get("duration"), item.get("durationMinutes"), default=""),
                "durationUnit": _first_non_empty(item.get("durationUnit"), default="min"),
                "totalDistance": _first_non_empty(item.get("totalDistance"), item.get("distance"), default=""),
                "totalDistanceUnit": _first_non_empty(item.get("totalDistanceUnit"), item.get("distanceUnit"), default=""),
                "totalEnergyBurned": _first_non_empty(item.get("totalEnergyBurned"), item.get("energyBurned"), default=""),
                "totalEnergyBurnedUnit": _first_non_empty(item.get("totalEnergyBurnedUnit"), item.get("energyUnit"), default="kcal"),
                "sourceName": _first_non_empty(item.get("sourceName"), item.get("source"), default="Health Auto Export"),
                "startDate": start_date,
                "endDate": end_date,
                "creationDate": _first_non_empty(item.get("creationDate"), item.get("dateAdded"), item.get("startDate")),
                "metadata_json": json.dumps(item.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                "ingestedAt": datetime.now(timezone.utc).isoformat(),
                "ingestionSource": "health_auto_export",
            }
        )

    if not normalized_records and not normalized_workouts:
        raise PayloadValidationError("Payload contained no records or workouts")

    return normalized_records, normalized_workouts


def _duckdb_required() -> None:
    if duckdb is None:
        raise RuntimeError("duckdb is required for parquet output. Install with: pip install duckdb")


def _sql_quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def merge_records_to_parquet(records: List[Dict[str, Any]], parquet_path: Path) -> int:
    """Merge normalized records into parquet with deduplication."""
    if not records:
        return 0
    _duckdb_required()

    parquet_path = Path(parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory() as tmp_dir:
        ndjson_path = Path(tmp_dir) / "records.ndjson"
        with ndjson_path.open("w", encoding="utf-8") as handle:
            for row in records:
                handle.write(json.dumps(row, ensure_ascii=True) + "\n")

        con = duckdb.connect(database=":memory:")
        try:
            con.execute(
                f"""
                CREATE TABLE incoming AS
                SELECT
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
                    CAST(ingestedAt AS VARCHAR) AS ingestedAt,
                    CAST(ingestionSource AS VARCHAR) AS ingestionSource
                FROM read_json_auto('{_sql_quote_path(ndjson_path)}', format='newline_delimited')
                """
            )

            if parquet_path.exists():
                con.execute(
                    f"CREATE TABLE existing AS SELECT * FROM read_parquet('{_sql_quote_path(parquet_path)}')"
                )
                con.execute(
                    """
                    CREATE TABLE merged AS
                    SELECT * FROM existing
                    UNION ALL
                    SELECT i.*
                    FROM incoming i
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM existing e
                        WHERE e.type = i.type
                          AND e.startDate = i.startDate
                          AND e.endDate = i.endDate
                          AND e.value = i.value
                          AND e.sourceName = i.sourceName
                          AND e.unit = i.unit
                    )
                    """
                )
            else:
                con.execute("CREATE TABLE merged AS SELECT * FROM incoming")

            con.execute(f"COPY merged TO '{_sql_quote_path(parquet_path)}' (FORMAT PARQUET)")
            count = con.execute("SELECT COUNT(*) FROM incoming").fetchone()[0]
            return int(count)
        finally:
            con.close()


def merge_workouts_to_parquet(workouts: List[Dict[str, Any]], parquet_path: Path) -> int:
    """Merge normalized workouts into parquet with deduplication."""
    if not workouts:
        return 0
    _duckdb_required()

    parquet_path = Path(parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory() as tmp_dir:
        ndjson_path = Path(tmp_dir) / "workouts.ndjson"
        with ndjson_path.open("w", encoding="utf-8") as handle:
            for row in workouts:
                handle.write(json.dumps(row, ensure_ascii=True) + "\n")

        con = duckdb.connect(database=":memory:")
        try:
            con.execute(
                f"""
                CREATE TABLE incoming AS
                SELECT
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
                    CAST(ingestedAt AS VARCHAR) AS ingestedAt,
                    CAST(ingestionSource AS VARCHAR) AS ingestionSource
                FROM read_json_auto('{_sql_quote_path(ndjson_path)}', format='newline_delimited')
                """
            )

            if parquet_path.exists():
                con.execute(
                    f"CREATE TABLE existing AS SELECT * FROM read_parquet('{_sql_quote_path(parquet_path)}')"
                )
                con.execute(
                    """
                    CREATE TABLE merged AS
                    SELECT * FROM existing
                    UNION ALL
                    SELECT i.*
                    FROM incoming i
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM existing e
                        WHERE e.workoutActivityType = i.workoutActivityType
                          AND e.startDate = i.startDate
                          AND e.endDate = i.endDate
                          AND e.totalDistance = i.totalDistance
                          AND e.totalEnergyBurned = i.totalEnergyBurned
                          AND e.sourceName = i.sourceName
                    )
                    """
                )
            else:
                con.execute("CREATE TABLE merged AS SELECT * FROM incoming")

            con.execute(f"COPY merged TO '{_sql_quote_path(parquet_path)}' (FORMAT PARQUET)")
            count = con.execute("SELECT COUNT(*) FROM incoming").fetchone()[0]
            return int(count)
        finally:
            con.close()


class HealthAutoExportRequestHandler(BaseHTTPRequestHandler):
    """HTTP handler for receiving Health Auto Export payloads."""

    ingestor: HealthAutoExportIngestor
    api_key: Optional[str]

    server_version = "HealthAutoExportIngestor/1.0"

    def _send_json(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/health-auto-export":
            self._send_json(404, {"error": "Not found"})
            return

        if not validate_api_key(self.headers, self.api_key):
            self._send_json(401, {"error": "Unauthorized"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(400, {"error": "Invalid Content-Length"})
            return

        body = self.rfile.read(content_length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Body must be valid JSON"})
            return

        try:
            result = self.ingestor.ingest_payload(payload)
        except PayloadValidationError as exc:
            self._send_json(400, {"error": str(exc)})
            return
        except RuntimeError as exc:
            self._send_json(500, {"error": str(exc)})
            return
        except Exception as exc:  # pragma: no cover - unexpected failures
            self._send_json(500, {"error": f"Unexpected server error: {exc}"})
            return

        self._send_json(
            200,
            {
                "status": "ok",
                "raw_file": str(result.raw_file),
                "records_received": result.records_received,
                "workouts_received": result.workouts_received,
                "records_written": result.records_written,
                "workouts_written": result.workouts_written,
            },
        )

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        self._send_json(404, {"error": "Not found"})

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        # Keep logs concise and machine-friendly.
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))


def run_server(
    host: str,
    port: int,
    raw_dir: Path,
    curated_dir: Path,
    api_key: Optional[str],
    enable_parquet: bool,
) -> None:
    """Run the HTTP server for Health Auto Export ingestion."""
    ingestor = HealthAutoExportIngestor(raw_dir=raw_dir, curated_dir=curated_dir, enable_parquet=enable_parquet)

    handler_cls = type(
        "ConfiguredHealthAutoExportRequestHandler",
        (HealthAutoExportRequestHandler,),
        {
            "ingestor": ingestor,
            "api_key": api_key,
        },
    )

    server = ThreadingHTTPServer((host, port), handler_cls)
    print(f"Listening on http://{host}:{port}")
    print("POST /health-auto-export to ingest payloads")
    print("GET /health for liveness checks")
    server.serve_forever()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive Health Auto Export payloads and merge into parquet",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="Run HTTP ingestion server")
    serve.add_argument("--host", default="0.0.0.0", help="Bind address")
    serve.add_argument("--port", type=int, default=8787, help="Bind port")
    serve.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR), help="Raw immutable payload directory")
    serve.add_argument("--curated-dir", default=str(DEFAULT_CURATED_DIR), help="Curated parquet directory")
    serve.add_argument("--api-key", default=os.getenv("HEALTH_AUTO_EXPORT_API_KEY"), help="Shared secret for X-API-Key")
    serve.add_argument("--skip-parquet", action="store_true", help="Store raw JSON only (no parquet conversion)")

    ingest = subparsers.add_parser("ingest-file", help="Ingest a local JSON payload file")
    ingest.add_argument("--file", required=True, help="Path to payload JSON file")
    ingest.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR), help="Raw immutable payload directory")
    ingest.add_argument("--curated-dir", default=str(DEFAULT_CURATED_DIR), help="Curated parquet directory")
    ingest.add_argument("--skip-parquet", action="store_true", help="Store raw JSON only (no parquet conversion)")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command == "serve":
        run_server(
            host=args.host,
            port=args.port,
            raw_dir=Path(args.raw_dir),
            curated_dir=Path(args.curated_dir),
            api_key=args.api_key,
            enable_parquet=not args.skip_parquet,
        )
        return

    if args.command == "ingest-file":
        payload_path = Path(args.file)
        if not payload_path.exists():
            print(f"Error: file not found: {payload_path}", file=sys.stderr)
            sys.exit(1)

        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        ingestor = HealthAutoExportIngestor(
            raw_dir=Path(args.raw_dir),
            curated_dir=Path(args.curated_dir),
            enable_parquet=not args.skip_parquet,
        )
        try:
            result = ingestor.ingest_payload(payload)
        except (PayloadValidationError, RuntimeError, json.JSONDecodeError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)

        print("Ingestion complete")
        print(f"  Raw file: {result.raw_file}")
        print(f"  Records received: {result.records_received}")
        print(f"  Workouts received: {result.workouts_received}")
        print(f"  Records written: {result.records_written}")
        print(f"  Workouts written: {result.workouts_written}")


if __name__ == "__main__":
    main()
