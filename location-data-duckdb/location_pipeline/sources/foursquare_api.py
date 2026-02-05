from __future__ import annotations

from datetime import datetime

import requests

from .base import PlaceReviewRecord, SavedPlaceRecord, VisitRecord


def load_foursquare_api(
    oauth_token: str,
    api_version: str = "20240201",
    limit: int = 250,
) -> tuple[list[VisitRecord], list[SavedPlaceRecord], list[PlaceReviewRecord]]:
    """Load personal check-ins, saved lists, and tips via Foursquare/Swarm legacy OAuth endpoints.

    These endpoints may not be available for all accounts/apps. If an endpoint fails,
    this loader returns partial data from whatever succeeded.
    """
    visits = _fetch_checkins(oauth_token, api_version, limit)
    saved_places = _fetch_saved_places(oauth_token, api_version)
    reviews = _fetch_tips(oauth_token, api_version)
    return visits, saved_places, reviews


def _fetch_checkins(oauth_token: str, api_version: str, limit: int) -> list[VisitRecord]:
    data = _get(
        "https://api.foursquare.com/v2/users/self/checkins",
        oauth_token,
        api_version,
        {"limit": limit},
    )
    items = (((data or {}).get("response") or {}).get("checkins") or {}).get("items") or []
    results: list[VisitRecord] = []
    for item in items:
        venue = item.get("venue", {})
        loc = venue.get("location", {})
        results.append(
            VisitRecord(
                visit_id=str(item.get("id") or f"foursquare-api-{len(results)}"),
                source_name="foursquare_api",
                started_at=_from_unix(item.get("createdAt")),
                ended_at=None,
                lat=_safe_float(loc.get("lat")),
                lon=_safe_float(loc.get("lng")),
                place_name=venue.get("name"),
                place_id=venue.get("id"),
                list_name=None,
                confidence=None,
                payload=item,
            )
        )
    return results


def _fetch_saved_places(oauth_token: str, api_version: str) -> list[SavedPlaceRecord]:
    data = _get("https://api.foursquare.com/v2/users/self/lists", oauth_token, api_version)
    lists = (((data or {}).get("response") or {}).get("lists") or {}).get("groups") or []
    records: list[SavedPlaceRecord] = []

    for group in lists:
        for user_list in group.get("items", []):
            list_name = user_list.get("name")
            list_id = user_list.get("id")
            list_data = _get(
                f"https://api.foursquare.com/v2/lists/{list_id}",
                oauth_token,
                api_version,
            )
            entries = ((((list_data or {}).get("response") or {}).get("list") or {}).get("listItems") or {}).get("items") or []
            for entry in entries:
                venue = (entry.get("venue") or {})
                loc = venue.get("location", {})
                records.append(
                    SavedPlaceRecord(
                        saved_id=str(entry.get("id") or f"foursquare-saved-{len(records)}"),
                        source_name="foursquare_api",
                        saved_at=_from_unix(entry.get("createdAt")),
                        place_name=venue.get("name"),
                        place_id=venue.get("id"),
                        lat=_safe_float(loc.get("lat")),
                        lon=_safe_float(loc.get("lng")),
                        list_name=list_name,
                        notes=(entry.get("note") or {}).get("text"),
                        payload=entry,
                    )
                )
    return records


def _fetch_tips(oauth_token: str, api_version: str) -> list[PlaceReviewRecord]:
    data = _get("https://api.foursquare.com/v2/users/self/tips", oauth_token, api_version)
    tips = (((data or {}).get("response") or {}).get("tips") or {}).get("items") or []
    results: list[PlaceReviewRecord] = []
    for tip in tips:
        venue = tip.get("venue", {})
        results.append(
            PlaceReviewRecord(
                review_id=str(tip.get("id") or f"foursquare-tip-{len(results)}"),
                source_name="foursquare_api",
                created_at=_from_unix(tip.get("createdAt")),
                place_name=venue.get("name"),
                place_id=venue.get("id"),
                rating=None,
                review_text=tip.get("text"),
                payload=tip,
            )
        )
    return results


def _get(url: str, oauth_token: str, api_version: str, extra_params: dict | None = None) -> dict | None:
    params = {"oauth_token": oauth_token, "v": api_version}
    if extra_params:
        params.update(extra_params)
    try:
        response = requests.get(url, params=params, timeout=30)
        if not response.ok:
            return None
        return response.json()
    except requests.RequestException:
        return None


def _from_unix(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(value))
    except (ValueError, TypeError):
        return None


def _safe_float(value: float | str | int | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
