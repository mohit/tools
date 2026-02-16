from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zipfile import ZipFile


DEFAULT_RAW_ROOT = Path(os.getenv("DATALAKE_RAW_ROOT", "~/datalake.me/raw")).expanduser()
DEFAULT_CURATED_ROOT = Path(os.getenv("DATALAKE_CURATED_ROOT", "~/datalake.me/curated")).expanduser()
DEFAULT_STATE_FILE = Path.home() / ".local" / "share" / "datalake" / "google_takeout_focused_state.json"


@dataclass(frozen=True)
class JsonDocument:
    logical_path: str
    payload: Any


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_ts_millis(value: str | int | None) -> datetime | None:
    if value is None:
        return None
    millis = int(value)
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).replace(tzinfo=None)


def _e7(value: int | None) -> float | None:
    if value is None:
        return None
    return value / 1e7


def _stable_id(prefix: str, payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def _normalize_logical_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    marker = "/Takeout/"
    if marker in normalized:
        normalized = normalized.split(marker, 1)[1]
    elif normalized.startswith("Takeout/"):
        normalized = normalized[len("Takeout/") :]
    return normalized.lstrip("./")


def _load_json_documents(source: Path) -> list[JsonDocument]:
    docs: list[JsonDocument] = []
    if source.is_dir():
        for path in sorted(source.rglob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            docs.append(JsonDocument(logical_path=_normalize_logical_path(str(path.relative_to(source))), payload=payload))
        return docs

    if source.is_file() and source.suffix.lower() == ".zip":
        with ZipFile(source) as archive:
            for member in sorted(archive.namelist()):
                if not member.lower().endswith(".json"):
                    continue
                try:
                    payload = json.loads(archive.read(member).decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                docs.append(JsonDocument(logical_path=_normalize_logical_path(member), payload=payload))
        return docs

    raise ValueError(f"Unsupported source input: {source}")


def _extract_location_rows(docs: Iterable[JsonDocument]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    visits: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []

    for doc in docs:
        path = doc.logical_path
        payload = doc.payload

        if "Location History" in path and path.endswith("Records.json") and isinstance(payload, dict):
            for item in payload.get("locations", []):
                visits.append(
                    {
                        "event_id": _stable_id("record", item),
                        "source_type": "record",
                        "event_ts": _parse_ts_millis(item.get("timestampMs")),
                        "start_ts": None,
                        "end_ts": None,
                        "place_name": None,
                        "place_id": None,
                        "lat": _e7(item.get("latitudeE7")),
                        "lon": _e7(item.get("longitudeE7")),
                        "confidence": None,
                        "source_file": path,
                        "payload": json.dumps(item, ensure_ascii=True),
                    }
                )

        if "Semantic Location History" in path and isinstance(payload, dict):
            timeline_objects = payload.get("timelineObjects", [])
            for timeline_object in timeline_objects:
                place_visit = timeline_object.get("placeVisit")
                if place_visit:
                    duration = place_visit.get("duration", {})
                    location = place_visit.get("location", {})
                    visits.append(
                        {
                            "event_id": _stable_id("visit", place_visit),
                            "source_type": "place_visit",
                            "event_ts": _parse_iso(duration.get("startTimestamp")),
                            "start_ts": _parse_iso(duration.get("startTimestamp")),
                            "end_ts": _parse_iso(duration.get("endTimestamp")),
                            "place_name": location.get("name"),
                            "place_id": location.get("placeId"),
                            "lat": _e7(location.get("latitudeE7")),
                            "lon": _e7(location.get("longitudeE7")),
                            "confidence": float(place_visit.get("visitConfidence")) if place_visit.get("visitConfidence") else None,
                            "source_file": path,
                            "payload": json.dumps(place_visit, ensure_ascii=True),
                        }
                    )

                activity_segment = timeline_object.get("activitySegment")
                if activity_segment:
                    duration = activity_segment.get("duration", {})
                    start_loc = activity_segment.get("startLocation", {})
                    end_loc = activity_segment.get("endLocation", {})
                    routes.append(
                        {
                            "route_id": _stable_id("route", activity_segment),
                            "event_ts": _parse_iso(duration.get("startTimestamp")),
                            "start_ts": _parse_iso(duration.get("startTimestamp")),
                            "end_ts": _parse_iso(duration.get("endTimestamp")),
                            "activity_type": activity_segment.get("activityType"),
                            "distance_m": activity_segment.get("distance"),
                            "start_lat": _e7(start_loc.get("latitudeE7")),
                            "start_lon": _e7(start_loc.get("longitudeE7")),
                            "end_lat": _e7(end_loc.get("latitudeE7")),
                            "end_lon": _e7(end_loc.get("longitudeE7")),
                            "source_file": path,
                            "payload": json.dumps(activity_segment, ensure_ascii=True),
                        }
                    )

    return visits, routes


def _extract_search_query(title: str | None) -> str | None:
    if not title:
        return None
    prefixes = ["Searched for ", "Searched for:"]
    for prefix in prefixes:
        if title.startswith(prefix):
            return title[len(prefix) :].strip().strip('"')
    return title.strip()


def _extract_search_rows(docs: Iterable[JsonDocument]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for doc in docs:
        if "My Activity/Search/" not in doc.logical_path:
            continue
        payload = doc.payload
        if not isinstance(payload, list):
            continue

        for item in payload:
            title = item.get("title")
            rows.append(
                {
                    "search_id": _stable_id("search", item),
                    "event_ts": _parse_iso(item.get("time")),
                    "query": _extract_search_query(title),
                    "title": title,
                    "title_url": item.get("titleUrl"),
                    "products": json.dumps(item.get("products", []), ensure_ascii=True),
                    "source_file": doc.logical_path,
                    "payload": json.dumps(item, ensure_ascii=True),
                }
            )
    return rows


def _extract_subtitles(item: dict[str, Any]) -> str | None:
    subtitles = item.get("subtitles")
    if not isinstance(subtitles, list):
        return None
    names = [sub.get("name") for sub in subtitles if isinstance(sub, dict) and sub.get("name")]
    if not names:
        return None
    return " | ".join(names)


def _is_youtube_music_event(item: dict[str, Any]) -> bool:
    header = (item.get("header") or "").lower()
    title_url = (item.get("titleUrl") or "").lower()
    title = (item.get("title") or "").lower()
    subtitle_text = (_extract_subtitles(item) or "").lower()
    signals = [header, title_url, title, subtitle_text]
    return any("youtube music" in signal or "music.youtube.com" in signal for signal in signals)


def _extract_music_rows(docs: Iterable[JsonDocument]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for doc in docs:
        if "My Activity/YouTube and YouTube Music/" not in doc.logical_path:
            continue
        payload = doc.payload
        if not isinstance(payload, list):
            continue

        for item in payload:
            if not isinstance(item, dict) or not _is_youtube_music_event(item):
                continue
            rows.append(
                {
                    "event_id": _stable_id("ytm", item),
                    "event_ts": _parse_iso(item.get("time")),
                    "title": item.get("title"),
                    "title_url": item.get("titleUrl"),
                    "header": item.get("header"),
                    "subtitle": _extract_subtitles(item),
                    "source_file": doc.logical_path,
                    "payload": json.dumps(item, ensure_ascii=True),
                }
            )
    return rows


def _partition_values(ts: datetime | None) -> tuple[str, str]:
    if ts is None:
        return "unknown", "unknown"
    return f"{ts.year:04d}", f"{ts.month:02d}"


def _write_partitioned_parquet(
    rows: list[dict[str, Any]],
    dataset_root: Path,
    source_token: str,
    table_name: str,
    columns: list[tuple[str, str]],
    ts_column: str,
) -> int:
    if not rows:
        return 0
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError("duckdb is required for parquet export. Install dependencies first.") from exc

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        year, month = _partition_values(row.get(ts_column))
        grouped.setdefault((year, month), []).append(row)

    written = 0
    for (year, month), group_rows in grouped.items():
        out_dir = dataset_root / f"year={year}" / f"month={month}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{source_token}.parquet"

        conn = duckdb.connect()
        try:
            cols_sql = ", ".join(f"{name} {sql_type}" for name, sql_type in columns)
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"insert into {table_name} values ({placeholders})"
            conn.execute(f"create table {table_name} ({cols_sql})")
            conn.executemany(
                insert_sql,
                [[row.get(name) for name, _ in columns] for row in group_rows],
            )
            conn.execute(f"copy {table_name} to ? (format parquet)", [str(out_path)])
            written += len(group_rows)
        finally:
            conn.close()

    return written


def _stable_source_token(path: Path) -> str:
    stat = path.stat()
    digest = hashlib.sha256(f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}".encode("utf-8")).hexdigest()[:12]
    base = path.stem.replace(" ", "_").replace(".", "_")
    return f"{base}_{digest}"


def _state_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"processed": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"processed": {}}
    if "processed" not in payload or not isinstance(payload["processed"], dict):
        return {"processed": {}}
    return payload


def _state_save(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _process_one_takeout(source: Path, curated_root: Path) -> dict[str, int]:
    docs = _load_json_documents(source)
    location_visits, location_routes = _extract_location_rows(docs)
    search_rows = _extract_search_rows(docs)
    music_rows = _extract_music_rows(docs)

    token = _stable_source_token(source)
    base = curated_root / "google_takeout"

    location_count = _write_partitioned_parquet(
        rows=location_visits,
        dataset_root=base / "location_visits",
        source_token=token,
        table_name="location_visits",
        columns=[
            ("event_id", "VARCHAR"),
            ("source_type", "VARCHAR"),
            ("event_ts", "TIMESTAMP"),
            ("start_ts", "TIMESTAMP"),
            ("end_ts", "TIMESTAMP"),
            ("place_name", "VARCHAR"),
            ("place_id", "VARCHAR"),
            ("lat", "DOUBLE"),
            ("lon", "DOUBLE"),
            ("confidence", "DOUBLE"),
            ("source_file", "VARCHAR"),
            ("payload", "JSON"),
        ],
        ts_column="event_ts",
    )

    route_count = _write_partitioned_parquet(
        rows=location_routes,
        dataset_root=base / "location_routes",
        source_token=token,
        table_name="location_routes",
        columns=[
            ("route_id", "VARCHAR"),
            ("event_ts", "TIMESTAMP"),
            ("start_ts", "TIMESTAMP"),
            ("end_ts", "TIMESTAMP"),
            ("activity_type", "VARCHAR"),
            ("distance_m", "DOUBLE"),
            ("start_lat", "DOUBLE"),
            ("start_lon", "DOUBLE"),
            ("end_lat", "DOUBLE"),
            ("end_lon", "DOUBLE"),
            ("source_file", "VARCHAR"),
            ("payload", "JSON"),
        ],
        ts_column="event_ts",
    )

    search_count = _write_partitioned_parquet(
        rows=search_rows,
        dataset_root=base / "search_history",
        source_token=token,
        table_name="search_history",
        columns=[
            ("search_id", "VARCHAR"),
            ("event_ts", "TIMESTAMP"),
            ("query", "VARCHAR"),
            ("title", "VARCHAR"),
            ("title_url", "VARCHAR"),
            ("products", "JSON"),
            ("source_file", "VARCHAR"),
            ("payload", "JSON"),
        ],
        ts_column="event_ts",
    )

    music_count = _write_partitioned_parquet(
        rows=music_rows,
        dataset_root=base / "youtube_music_history",
        source_token=token,
        table_name="youtube_music_history",
        columns=[
            ("event_id", "VARCHAR"),
            ("event_ts", "TIMESTAMP"),
            ("title", "VARCHAR"),
            ("title_url", "VARCHAR"),
            ("header", "VARCHAR"),
            ("subtitle", "VARCHAR"),
            ("source_file", "VARCHAR"),
            ("payload", "JSON"),
        ],
        ts_column="event_ts",
    )

    return {
        "location_visits": location_count,
        "location_routes": route_count,
        "search_history": search_count,
        "youtube_music_history": music_count,
    }


def _copy_raw_archive(source: Path, raw_root: Path) -> Path:
    if not source.is_file():
        return source
    raw_dir = raw_root / "google_takeout" / "archives"
    raw_dir.mkdir(parents=True, exist_ok=True)
    destination = raw_dir / source.name
    if not destination.exists():
        destination.write_bytes(source.read_bytes())
    return destination


def _iter_takeout_sources(input_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for path in sorted(input_dir.iterdir()):
        if path.is_file() and path.suffix.lower() == ".zip":
            candidates.append(path)
        elif path.is_dir() and "takeout" in path.name.lower():
            candidates.append(path)
    return candidates


def _print_guide() -> None:
    print("Google Takeout setup (focused scope for issue #22):")
    print("1. Open https://takeout.google.com/")
    print("2. Click 'Deselect all'.")
    print("3. Enable only these products:")
    print("   - Location History (Timeline)")
    print("   - Search")
    print("   - YouTube and YouTube Music")
    print("4. Keep all Workspace products disabled (Gmail, Drive, Docs, Photos, Videos).")
    print("5. Keep Chrome data disabled (Chrome, Browser History).")
    print("6. Export as .zip and drop into your ingest folder.")
    print("7. Run: google-takeout-focused sync --takeout-dir <folder>")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Focused Google Takeout processing for location, search, and YouTube Music")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("guide", help="Print selective Takeout export checklist")

    process_parser = subparsers.add_parser("process", help="Process one Takeout zip/folder")
    process_parser.add_argument("--source", required=True, help="Path to Takeout .zip or extracted Takeout folder")
    process_parser.add_argument("--curated-root", default=str(DEFAULT_CURATED_ROOT))
    process_parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))

    sync_parser = subparsers.add_parser("sync", help="Process new Takeout exports from a directory")
    sync_parser.add_argument("--takeout-dir", required=True, help="Directory containing Takeout zips/folders")
    sync_parser.add_argument("--curated-root", default=str(DEFAULT_CURATED_ROOT))
    sync_parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    sync_parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "guide":
        _print_guide()
        return

    if args.command == "process":
        source = Path(args.source).expanduser().resolve()
        curated_root = Path(args.curated_root).expanduser().resolve()
        raw_root = Path(args.raw_root).expanduser().resolve()

        copied_source = _copy_raw_archive(source, raw_root)
        result = _process_one_takeout(copied_source, curated_root)
        print(f"Processed {copied_source}")
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    if args.command == "sync":
        takeout_dir = Path(args.takeout_dir).expanduser().resolve()
        curated_root = Path(args.curated_root).expanduser().resolve()
        raw_root = Path(args.raw_root).expanduser().resolve()
        state_file = Path(args.state_file).expanduser().resolve()

        state = _state_load(state_file)
        processed = state["processed"]

        total = {
            "location_visits": 0,
            "location_routes": 0,
            "search_history": 0,
            "youtube_music_history": 0,
        }
        processed_sources = 0

        for source in _iter_takeout_sources(takeout_dir):
            token = _stable_source_token(source)
            if token in processed:
                continue

            copied_source = _copy_raw_archive(source, raw_root)
            result = _process_one_takeout(copied_source, curated_root)
            processed[token] = {
                "path": str(source),
                "processed_at": datetime.utcnow().isoformat() + "Z",
                "rows": result,
            }
            for key, value in result.items():
                total[key] += value
            processed_sources += 1

        _state_save(state_file, state)
        print(f"Processed sources: {processed_sources}")
        print(json.dumps(total, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
