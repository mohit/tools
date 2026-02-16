from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .base import RawEventRecord, VisitRecord


def _parse_ts_millis(value: str | int | None) -> datetime | None:
    if value is None:
        return None
    millis = int(value)
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).replace(tzinfo=None)


def _e7_to_float(value: int | None) -> float | None:
    if value is None:
        return None
    return value / 1e7


def _stable_id(prefix: str, payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def load_google_takeout(base_path: str) -> tuple[list[RawEventRecord], list[VisitRecord]]:
    root = Path(base_path)
    raw_events: list[RawEventRecord] = []
    visits: list[VisitRecord] = []

    records_file = next(root.rglob("Records.json"), None)
    if records_file and records_file.exists():
        payload = json.loads(records_file.read_text(encoding="utf-8"))
        for item in payload.get("locations", []):
            raw_events.append(
                RawEventRecord(
                    event_id=_stable_id("google-record", item),
                    source_name="google_takeout",
                    event_ts=_parse_ts_millis(item.get("timestampMs")),
                    lat=_e7_to_float(item.get("latitudeE7")),
                    lon=_e7_to_float(item.get("longitudeE7")),
                    place_id=None,
                    payload=item,
                )
            )

    for path in root.rglob("*.json"):
        if "Semantic Location History" not in str(path):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for timeline_object in data.get("timelineObjects", []):
            visit = timeline_object.get("placeVisit")
            if not visit:
                continue
            location = visit.get("location", {})
            duration = visit.get("duration", {})
            visits.append(
                VisitRecord(
                    visit_id=_stable_id("google-visit", visit),
                    source_name="google_takeout",
                    started_at=_parse_iso(duration.get("startTimestamp")),
                    ended_at=_parse_iso(duration.get("endTimestamp")),
                    lat=location.get("latitudeE7", 0) / 1e7 if location.get("latitudeE7") else None,
                    lon=location.get("longitudeE7", 0) / 1e7 if location.get("longitudeE7") else None,
                    place_name=location.get("name"),
                    place_id=location.get("placeId"),
                    list_name=None,
                    confidence=float(visit.get("visitConfidence", 0)) if visit.get("visitConfidence") else None,
                    payload=visit,
                )
            )

    return raw_events, visits


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt
