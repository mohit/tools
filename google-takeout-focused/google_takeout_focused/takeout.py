from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import duckdb

LOCATION_COLUMNS = [
    "event_id",
    "event_ts",
    "ended_at",
    "event_type",
    "lat",
    "lon",
    "end_lat",
    "end_lon",
    "place_name",
    "place_id",
    "activity_type",
    "source_file",
    "payload_json",
]

SEARCH_COLUMNS = [
    "event_id",
    "event_ts",
    "query",
    "title",
    "title_url",
    "source_file",
    "payload_json",
]

MUSIC_COLUMNS = [
    "event_id",
    "event_ts",
    "track_title",
    "artist",
    "title",
    "url",
    "source_file",
    "payload_json",
]


@dataclass
class TakeoutData:
    location_events: list[dict[str, Any]]
    search_events: list[dict[str, Any]]
    music_events: list[dict[str, Any]]
    matched_files: dict[str, list[Path]]


def parse_takeout(input_path: Path) -> TakeoutData:
    with _takeout_root(input_path) as root:
        location_files = _find_location_files(root)
        search_files = _find_search_files(root)
        music_files = _find_music_files(root)

        return TakeoutData(
            location_events=_parse_location_events(location_files, root),
            search_events=_parse_search_events(search_files, root),
            music_events=_parse_music_events(music_files, root),
            matched_files={
                "location": location_files,
                "search": search_files,
                "music": music_files,
            },
        )


def write_raw_snapshots(matched_files: dict[str, list[Path]], root: Path, raw_root: Path, snapshot_id: str) -> dict[str, int]:
    copied_counts = {"location": 0, "search": 0, "music": 0}
    for category, files in matched_files.items():
        if not files:
            continue
        out_dir = raw_root / f"google-{category}" / f"takeout_{snapshot_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        for src in files:
            rel = src.relative_to(root)
            target = out_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
            copied_counts[category] += 1
    return copied_counts


def merge_to_curated(
    records: list[dict[str, Any]],
    dataset_name: str,
    columns: list[str],
    ts_column: str,
    curated_root: Path,
) -> int:
    dataset_dir = curated_root / "google" / dataset_name
    existing = _read_existing_records(dataset_dir, columns)

    merged: dict[str, dict[str, Any]] = {}
    for row in existing:
        merged[row["event_id"]] = row
    for row in records:
        merged[row["event_id"]] = row

    merged_rows = list(merged.values())
    if not merged_rows:
        return 0

    with tempfile.TemporaryDirectory(prefix=f"{dataset_name}-") as tmpdir:
        tmpdir_path = Path(tmpdir)
        ndjson_path = tmpdir_path / "merged.ndjson"
        _write_ndjson(ndjson_path, merged_rows, columns)

        out_dir = tmpdir_path / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect()
        ndjson_sql = str(ndjson_path).replace("'", "''")
        out_sql = str(out_dir).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT *, year({ts_column}) AS year, month({ts_column}) AS month
                FROM read_json_auto('{ndjson_sql}')
                WHERE {ts_column} IS NOT NULL
            )
            TO '{out_sql}' (FORMAT PARQUET, PARTITION_BY (year, month))
            """
        )

        if dataset_dir.exists():
            shutil.rmtree(dataset_dir)
        dataset_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(out_dir), str(dataset_dir))

    return len(merged_rows)


def write_catalog(catalog_root: Path, snapshot_id: str, data: TakeoutData, totals: dict[str, int]) -> Path:
    catalog_root.mkdir(parents=True, exist_ok=True)
    catalog_path = catalog_root / "google_takeout_focused.json"
    payload = {
        "source": "google_takeout_focused",
        "snapshot_id": snapshot_id,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "datasets": {
            "location_timeline": {
                "events": totals["location_timeline"],
                "source_files": [str(p) for p in data.matched_files["location"]],
            },
            "search_history": {
                "events": totals["search_history"],
                "source_files": [str(p) for p in data.matched_files["search"]],
            },
            "youtube_music_history": {
                "events": totals["youtube_music_history"],
                "source_files": [str(p) for p in data.matched_files["music"]],
            },
        },
    }
    catalog_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return catalog_path


def build_analysis_report(data: TakeoutData) -> dict[str, Any]:
    report = {
        "location_timeline": _dataset_report(data.location_events, "event_ts"),
        "search_history": _dataset_report(data.search_events, "event_ts"),
        "youtube_music_history": _dataset_report(data.music_events, "event_ts"),
    }
    report["matched_files"] = {
        "location": [str(p) for p in data.matched_files["location"]],
        "search": [str(p) for p in data.matched_files["search"]],
        "music": [str(p) for p in data.matched_files["music"]],
    }
    return report


def _dataset_report(records: list[dict[str, Any]], ts_column: str) -> dict[str, Any]:
    timestamps = [r[ts_column] for r in records if r.get(ts_column)]
    timestamps.sort()
    return {
        "events": len(records),
        "first_event_utc": timestamps[0] if timestamps else None,
        "last_event_utc": timestamps[-1] if timestamps else None,
    }


def _read_existing_records(dataset_dir: Path, columns: list[str]) -> list[dict[str, Any]]:
    parquet_glob = dataset_dir / "year=*" / "month=*" / "*.parquet"
    if not dataset_dir.exists() or not list(dataset_dir.rglob("*.parquet")):
        return []

    con = duckdb.connect()
    rows = con.execute(
        f"SELECT {', '.join(columns)} FROM read_parquet(?)",
        [str(parquet_glob)],
    ).fetchall()
    names = [desc[0] for desc in con.description]

    output: list[dict[str, Any]] = []
    for row in rows:
        payload: dict[str, Any] = {}
        for idx, name in enumerate(names):
            value = row[idx]
            if isinstance(value, datetime):
                payload[name] = value.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
            else:
                payload[name] = value
        output.append(payload)
    return output


def _write_ndjson(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            normalized = {col: row.get(col) for col in columns}
            handle.write(json.dumps(normalized, sort_keys=True))
            handle.write("\n")


def _find_location_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.json"):
        text = str(path).lower()
        name = path.name.lower()
        if name == "records.json":
            files.append(path)
            continue
        if "semantic location history" in text:
            files.append(path)
            continue
        if "timeline" in text and "location history" in text:
            files.append(path)
    return sorted(set(files))


def _find_search_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.json"):
        text = str(path).lower()
        if "/search/" in text and "my activity" in text:
            files.append(path)
        elif path.name.lower() in {"myactivity.json", "my activity.json"} and "search" in text:
            files.append(path)
    return sorted(set(files))


def _find_music_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.json"):
        text = str(path).lower()
        if "youtube and youtube music" in text and "history" in text:
            files.append(path)
        elif "youtube music" in text and "my activity" in text:
            files.append(path)
    return sorted(set(files))


def _parse_location_events(files: list[Path], root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in files:
        payload = _load_json(path)
        rel = str(path.relative_to(root))

        if isinstance(payload, dict) and "locations" in payload:
            for item in payload.get("locations", []):
                event_ts = _parse_timestamp_ms(item.get("timestampMs"))
                row = {
                    "event_id": _event_id("location", rel, item.get("timestampMs"), item.get("latitudeE7"), item.get("longitudeE7")),
                    "event_ts": event_ts,
                    "ended_at": None,
                    "event_type": "raw_point",
                    "lat": _e7_to_float(item.get("latitudeE7")),
                    "lon": _e7_to_float(item.get("longitudeE7")),
                    "end_lat": None,
                    "end_lon": None,
                    "place_name": None,
                    "place_id": None,
                    "activity_type": None,
                    "source_file": rel,
                    "payload_json": json.dumps(item, sort_keys=True),
                }
                out.append(row)

        timeline_objects = []
        if isinstance(payload, dict):
            timeline_objects = payload.get("timelineObjects", [])

        for timeline in timeline_objects:
            place_visit = timeline.get("placeVisit")
            if place_visit:
                duration = place_visit.get("duration", {})
                location = place_visit.get("location", {})
                row = {
                    "event_id": _event_id(
                        "location",
                        rel,
                        duration.get("startTimestamp"),
                        duration.get("endTimestamp"),
                        location.get("placeId"),
                    ),
                    "event_ts": _parse_iso(duration.get("startTimestamp")),
                    "ended_at": _parse_iso(duration.get("endTimestamp")),
                    "event_type": "place_visit",
                    "lat": _e7_to_float(location.get("latitudeE7")),
                    "lon": _e7_to_float(location.get("longitudeE7")),
                    "end_lat": None,
                    "end_lon": None,
                    "place_name": location.get("name"),
                    "place_id": location.get("placeId"),
                    "activity_type": None,
                    "source_file": rel,
                    "payload_json": json.dumps(place_visit, sort_keys=True),
                }
                out.append(row)

            activity_segment = timeline.get("activitySegment")
            if activity_segment:
                duration = activity_segment.get("duration", {})
                start_loc = activity_segment.get("startLocation", {})
                end_loc = activity_segment.get("endLocation", {})
                row = {
                    "event_id": _event_id(
                        "location",
                        rel,
                        duration.get("startTimestamp"),
                        duration.get("endTimestamp"),
                        activity_segment.get("activityType"),
                    ),
                    "event_ts": _parse_iso(duration.get("startTimestamp")),
                    "ended_at": _parse_iso(duration.get("endTimestamp")),
                    "event_type": "activity_segment",
                    "lat": _e7_to_float(start_loc.get("latitudeE7")),
                    "lon": _e7_to_float(start_loc.get("longitudeE7")),
                    "end_lat": _e7_to_float(end_loc.get("latitudeE7")),
                    "end_lon": _e7_to_float(end_loc.get("longitudeE7")),
                    "place_name": None,
                    "place_id": None,
                    "activity_type": activity_segment.get("activityType"),
                    "source_file": rel,
                    "payload_json": json.dumps(activity_segment, sort_keys=True),
                }
                out.append(row)

    return [row for row in out if row["event_ts"]]


def _parse_search_events(files: list[Path], root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in files:
        payload = _load_json(path)
        rel = str(path.relative_to(root))
        entries = payload if isinstance(payload, list) else payload.get("events", []) if isinstance(payload, dict) else []

        for item in entries:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "")
            query = _extract_search_query(title)
            if not query and "/search" not in str(item.get("titleUrl") or "").lower():
                continue

            event_ts = _parse_iso(item.get("time"))
            if not event_ts:
                continue

            row = {
                "event_id": _event_id("search", rel, item.get("time"), query, item.get("titleUrl")),
                "event_ts": event_ts,
                "query": query,
                "title": title,
                "title_url": item.get("titleUrl"),
                "source_file": rel,
                "payload_json": json.dumps(item, sort_keys=True),
            }
            out.append(row)

    return out


def _parse_music_events(files: list[Path], root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in files:
        payload = _load_json(path)
        rel = str(path.relative_to(root))
        entries = payload if isinstance(payload, list) else payload.get("events", []) if isinstance(payload, dict) else []

        for item in entries:
            if not isinstance(item, dict):
                continue

            if not _is_youtube_music_event(item):
                continue

            event_ts = _parse_iso(item.get("time"))
            if not event_ts:
                continue

            title = str(item.get("title") or "")
            subtitles = item.get("subtitles") or []
            artist = None
            if subtitles and isinstance(subtitles, list) and isinstance(subtitles[0], dict):
                artist = subtitles[0].get("name")

            row = {
                "event_id": _event_id("music", rel, item.get("time"), title, item.get("titleUrl")),
                "event_ts": event_ts,
                "track_title": _normalize_track_title(title),
                "artist": artist,
                "title": title,
                "url": item.get("titleUrl"),
                "source_file": rel,
                "payload_json": json.dumps(item, sort_keys=True),
            }
            out.append(row)

    return out


def _is_youtube_music_event(item: dict[str, Any]) -> bool:
    title_url = str(item.get("titleUrl") or "").lower()
    if "music.youtube.com" in title_url:
        return True

    header = str(item.get("header") or "").lower()
    if "youtube music" in header:
        return True

    products = item.get("products")
    if isinstance(products, list):
        for product in products:
            if "youtube music" in str(product).lower():
                return True

    title = str(item.get("title") or "").lower()
    return title.startswith("listened to")


def _extract_search_query(title: str) -> str | None:
    text = title.strip()
    match = re.match(r"^Searched for (.+)$", text)
    if match:
        return match.group(1).strip()
    match = re.match(r"^Visited (.+)$", text)
    if match and "google.com/search" in text.lower():
        return match.group(1).strip()
    return None


def _normalize_track_title(title: str) -> str | None:
    text = title.strip()
    prefixes = ["Listened to ", "Watched "]
    for prefix in prefixes:
        if text.startswith(prefix):
            return text[len(prefix) :].strip()
    return text or None


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _e7_to_float(value: int | None) -> float | None:
    if value is None:
        return None
    return value / 1e7


def _parse_timestamp_ms(value: str | int | None) -> str | None:
    if value is None:
        return None
    dt = datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_iso(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


def _event_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()  # nosec: deterministic ID only
    return f"{prefix}-{digest}"


class _TakeoutRoot:
    def __init__(self, root: Path, temp_dir: tempfile.TemporaryDirectory[str] | None = None) -> None:
        self.root = root
        self._temp_dir = temp_dir

    def __enter__(self) -> Path:
        return self.root

    def __exit__(self, *_args: object) -> None:
        if self._temp_dir is not None:
            self._temp_dir.cleanup()


def _takeout_root(input_path: Path) -> _TakeoutRoot:
    if input_path.is_dir():
        return _TakeoutRoot(input_path)

    if input_path.is_file() and input_path.suffix.lower() == ".zip":
        temp_dir: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory(prefix="takeout-")
        with ZipFile(input_path, "r") as archive:
            archive.extractall(temp_dir.name)
        return _TakeoutRoot(Path(temp_dir.name), temp_dir)

    raise ValueError(f"Unsupported input path: {input_path}")
