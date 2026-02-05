from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class VisitRecord:
    visit_id: str
    source_name: str
    started_at: datetime | None
    ended_at: datetime | None
    lat: float | None
    lon: float | None
    place_name: str | None
    place_id: str | None
    list_name: str | None
    confidence: float | None
    payload: dict[str, Any]


@dataclass
class RawEventRecord:
    event_id: str
    source_name: str
    event_ts: datetime | None
    lat: float | None
    lon: float | None
    place_id: str | None
    payload: dict[str, Any]


@dataclass
class SavedPlaceRecord:
    saved_id: str
    source_name: str
    saved_at: datetime | None
    place_name: str | None
    place_id: str | None
    lat: float | None
    lon: float | None
    list_name: str | None
    notes: str | None
    payload: dict[str, Any]


@dataclass
class PlaceReviewRecord:
    review_id: str
    source_name: str
    created_at: datetime | None
    place_name: str | None
    place_id: str | None
    rating: float | None
    review_text: str | None
    payload: dict[str, Any]
