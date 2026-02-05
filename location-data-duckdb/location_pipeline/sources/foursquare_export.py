from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from .base import VisitRecord


def load_foursquare_export(base_path: str) -> list[VisitRecord]:
    root = Path(base_path)
    visits: list[VisitRecord] = []

    for csv_path in root.rglob("*.csv"):
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lat = _safe_float(row.get("latitude") or row.get("lat"))
                lon = _safe_float(row.get("longitude") or row.get("lon") or row.get("lng"))
                visits.append(
                    VisitRecord(
                        visit_id=row.get("checkin_id") or f"foursquare-csv-{len(visits)}",
                        source_name="foursquare_export",
                        started_at=_safe_dt(row.get("created_at") or row.get("timestamp")),
                        ended_at=None,
                        lat=lat,
                        lon=lon,
                        place_name=row.get("venue_name") or row.get("name"),
                        place_id=row.get("venue_id") or row.get("place_id"),
                        list_name=row.get("list_name"),
                        confidence=None,
                        payload=row,
                    )
                )

    for json_path in root.rglob("*.json"):
        data = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            continue
        for row in data:
            if not isinstance(row, dict):
                continue
            visits.append(
                VisitRecord(
                    visit_id=str(row.get("checkin_id") or f"foursquare-json-{len(visits)}"),
                    source_name="foursquare_export",
                    started_at=_safe_dt(row.get("created_at") or row.get("timestamp")),
                    ended_at=None,
                    lat=_safe_float(row.get("latitude") or row.get("lat")),
                    lon=_safe_float(row.get("longitude") or row.get("lon") or row.get("lng")),
                    place_name=row.get("venue_name") or row.get("name"),
                    place_id=row.get("venue_id") or row.get("place_id"),
                    list_name=row.get("list_name"),
                    confidence=None,
                    payload=row,
                )
            )

    return visits


def _safe_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _safe_float(value: str | float | int | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None
