from __future__ import annotations

import csv
import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .base import VisitRecord


def load_foursquare_export(
    base_path: str,
    *,
    places_api_key: str | None = None,
    cache_path: str | Path | None = None,
    places_api_base_url: str = "https://api.foursquare.com/v3/places",
    request_timeout_seconds: float = 30.0,
) -> list[VisitRecord]:
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

    if not places_api_key:
        return visits

    cache_file = Path(cache_path) if cache_path else (root / ".foursquare_places_cache.json")
    venue_cache = _load_cache(cache_file)
    _enrich_with_places_api(
        visits=visits,
        places_api_key=places_api_key,
        places_api_base_url=places_api_base_url,
        request_timeout_seconds=request_timeout_seconds,
        venue_cache=venue_cache,
    )
    _save_cache(cache_file, venue_cache)
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


def _enrich_with_places_api(
    visits: list[VisitRecord],
    places_api_key: str,
    places_api_base_url: str,
    request_timeout_seconds: float,
    venue_cache: dict[str, dict[str, Any]],
) -> None:
    for visit in visits:
        if (visit.lat is not None and visit.lon is not None) or not visit.place_id:
            continue

        venue_id = visit.place_id
        cached = venue_cache.get(venue_id)
        if cached is None:
            cached = _fetch_venue_location(
                venue_id=venue_id,
                places_api_key=places_api_key,
                places_api_base_url=places_api_base_url,
                request_timeout_seconds=request_timeout_seconds,
            )
            venue_cache[venue_id] = cached

        if visit.lat is None:
            visit.lat = _safe_float(cached.get("lat"))
        if visit.lon is None:
            visit.lon = _safe_float(cached.get("lon"))

        if isinstance(visit.payload, dict):
            visit.payload.setdefault("venue_location", {})
            location = visit.payload["venue_location"]
            if isinstance(location, dict):
                location.update(cached)


def _fetch_venue_location(
    venue_id: str,
    places_api_key: str,
    places_api_base_url: str,
    request_timeout_seconds: float,
) -> dict[str, Any]:
    url = f"{places_api_base_url.rstrip('/')}/{venue_id}"
    headers = {
        "Authorization": places_api_key,
        "accept": "application/json",
    }
    try:
        response = requests.get(url, headers=headers, timeout=request_timeout_seconds)
        if not response.ok:
            return {}
        data = response.json()
    except (requests.RequestException, ValueError):
        return {}

    geocodes = data.get("geocodes") if isinstance(data, Mapping) else None
    main_geocode = geocodes.get("main") if isinstance(geocodes, Mapping) else None
    location = data.get("location") if isinstance(data, Mapping) else None

    lat = None
    lon = None
    if isinstance(main_geocode, Mapping):
        lat = _safe_float(main_geocode.get("latitude"))
        lon = _safe_float(main_geocode.get("longitude"))

    if lat is None and isinstance(location, Mapping):
        lat = _safe_float(location.get("lat") or location.get("latitude"))
    if lon is None and isinstance(location, Mapping):
        lon = _safe_float(location.get("lng") or location.get("lon") or location.get("longitude"))

    result = {
        "lat": lat,
        "lon": lon,
    }
    if isinstance(location, Mapping):
        result["address"] = location.get("address")
        result["city"] = location.get("locality")
        result["country"] = location.get("country")
    return result


def _load_cache(cache_path: Path) -> dict[str, dict[str, Any]]:
    if not cache_path.exists():
        return {}
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for venue_id, location in data.items():
        if isinstance(venue_id, str) and isinstance(location, dict):
            cache[venue_id] = location
    return cache


def _save_cache(cache_path: Path, cache: dict[str, dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
