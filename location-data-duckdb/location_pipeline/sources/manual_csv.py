from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from .base import VisitRecord


def load_manual_csv(path: str) -> list[VisitRecord]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []

    visits: list[VisitRecord] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            visits.append(
                VisitRecord(
                    visit_id=row.get("visit_id") or f"manual-{len(visits)}",
                    source_name="manual_csv",
                    started_at=_safe_dt(row.get("started_at")),
                    ended_at=_safe_dt(row.get("ended_at")),
                    lat=float(row["lat"]) if row.get("lat") else None,
                    lon=float(row["lon"]) if row.get("lon") else None,
                    place_name=row.get("place_name"),
                    place_id=row.get("place_id"),
                    list_name=row.get("list_name"),
                    confidence=float(row["confidence"]) if row.get("confidence") else None,
                    payload=row,
                )
            )
    return visits


def _safe_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
