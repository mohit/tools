from __future__ import annotations

import os
from datetime import datetime

import duckdb
import requests


def enrich_places(conn: duckdb.DuckDBPyConnection, api_key_env: str, max_rows_per_run: int) -> int:
    api_key = os.getenv(api_key_env)
    if not api_key:
        return 0

    rows = conn.execute(
        """
        select distinct place_id
        from visits
        where place_id is not null
          and place_id not in (select place_id from place_enrichment_google)
        limit ?
        """,
        [max_rows_per_run],
    ).fetchall()

    inserted = 0
    for (place_id,) in rows:
        payload = _fetch_place_details(place_id, api_key)
        if not payload:
            continue
        result = payload.get("result", {})
        conn.execute(
            """
            insert into place_enrichment_google (
                place_id, fetched_at, formatted_address, rating, user_ratings_total, primary_type, payload
            ) values (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                place_id,
                datetime.utcnow(),
                result.get("formatted_address"),
                result.get("rating"),
                result.get("user_ratings_total"),
                (result.get("types") or [None])[0],
                payload,
            ],
        )
        inserted += 1
    return inserted


def _fetch_place_details(place_id: str, api_key: str) -> dict | None:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    response = requests.get(
        url,
        params={
            "place_id": place_id,
            "fields": "place_id,name,formatted_address,rating,user_ratings_total,types,geometry",
            "key": api_key,
        },
        timeout=30,
    )
    if not response.ok:
        return None
    data = response.json()
    if data.get("status") not in {"OK", "ZERO_RESULTS"}:
        return None
    return data
