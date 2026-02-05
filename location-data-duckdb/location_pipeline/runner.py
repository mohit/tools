from __future__ import annotations

import os
from datetime import datetime
from uuid import uuid4

import duckdb

from .enrich.google_places import enrich_places
from .sources.foursquare_api import load_foursquare_api
from .sources.foursquare_export import load_foursquare_export
from .sources.google_takeout import load_google_takeout
from .sources.manual_csv import load_manual_csv


def run_source(conn: duckdb.DuckDBPyConnection, source_name: str, source_cfg: dict) -> tuple[int, int, int, int]:
    if source_name == "google_takeout":
        raw_events, visits = load_google_takeout(source_cfg["path"])
        _insert_raw_events(conn, raw_events)
        _insert_visits(conn, visits)
        return len(raw_events), len(visits), 0, 0

    if source_name == "foursquare_export":
        visits = load_foursquare_export(source_cfg["path"])
        _insert_visits(conn, visits)
        return 0, len(visits), 0, 0

    if source_name == "manual_csv":
        visits = load_manual_csv(source_cfg["path"])
        _insert_visits(conn, visits)
        return 0, len(visits), 0, 0

    if source_name == "foursquare_api":
        token = os.getenv(source_cfg.get("oauth_token_env", "FOURSQUARE_OAUTH_TOKEN"))
        if not token:
            return 0, 0, 0, 0
        visits, saved_places, reviews = load_foursquare_api(
            oauth_token=token,
            api_version=source_cfg.get("api_version", "20240201"),
            limit=int(source_cfg.get("checkins_limit", 250)),
        )
        _insert_visits(conn, visits)
        _insert_saved_places(conn, saved_places)
        _insert_reviews(conn, reviews)
        return 0, len(visits), len(saved_places), len(reviews)

    raise ValueError(f"Unsupported source: {source_name}")


def run_with_audit(conn: duckdb.DuckDBPyConnection, source_name: str, source_cfg: dict) -> tuple[int, int, int, int]:
    run_id = str(uuid4())
    started = datetime.utcnow()
    conn.execute(
        "insert into ingestion_runs (run_id, source_name, started_at, status, message) values (?, ?, ?, ?, ?)",
        [run_id, source_name, started, "running", None],
    )

    try:
        raw_count, visit_count, saved_count, review_count = run_source(conn, source_name, source_cfg)
        conn.execute(
            """
            update ingestion_runs
            set finished_at = ?, status = ?, message = ?
            where run_id = ?
            """,
            [
                datetime.utcnow(),
                "success",
                f"raw={raw_count}, visits={visit_count}, saved={saved_count}, reviews={review_count}",
                run_id,
            ],
        )
        _refresh_place_dim(conn)
        return raw_count, visit_count, saved_count, review_count
    except Exception as exc:
        conn.execute(
            """
            update ingestion_runs
            set finished_at = ?, status = ?, message = ?
            where run_id = ?
            """,
            [datetime.utcnow(), "failed", str(exc), run_id],
        )
        raise


def run_enrichment(conn: duckdb.DuckDBPyConnection, enrichment_cfg: dict) -> int:
    google_cfg = enrichment_cfg.get("google_places", {})
    if not google_cfg.get("enabled"):
        return 0
    return enrich_places(
        conn=conn,
        api_key_env=google_cfg.get("api_key_env", "GOOGLE_PLACES_API_KEY"),
        max_rows_per_run=int(google_cfg.get("max_rows_per_run", 200)),
    )


def _insert_raw_events(conn: duckdb.DuckDBPyConnection, records: list) -> None:
    if not records:
        return
    conn.executemany(
        """
        insert into raw_events (event_id, source_name, event_ts, lat, lon, place_id, payload)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        [[r.event_id, r.source_name, r.event_ts, r.lat, r.lon, r.place_id, r.payload] for r in records],
    )


def _insert_visits(conn: duckdb.DuckDBPyConnection, records: list) -> None:
    if not records:
        return
    conn.executemany(
        """
        insert into visits (
            visit_id, source_name, started_at, ended_at, lat, lon,
            place_name, place_id, list_name, confidence, payload
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            [
                r.visit_id,
                r.source_name,
                r.started_at,
                r.ended_at,
                r.lat,
                r.lon,
                r.place_name,
                r.place_id,
                r.list_name,
                r.confidence,
                r.payload,
            ]
            for r in records
        ],
    )


def _insert_saved_places(conn: duckdb.DuckDBPyConnection, records: list) -> None:
    if not records:
        return
    conn.executemany(
        """
        insert into saved_places (
            saved_id, source_name, saved_at, place_name, place_id,
            lat, lon, list_name, notes, payload
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            [
                r.saved_id,
                r.source_name,
                r.saved_at,
                r.place_name,
                r.place_id,
                r.lat,
                r.lon,
                r.list_name,
                r.notes,
                r.payload,
            ]
            for r in records
        ],
    )


def _insert_reviews(conn: duckdb.DuckDBPyConnection, records: list) -> None:
    if not records:
        return
    conn.executemany(
        """
        insert into place_reviews (
            review_id, source_name, created_at, place_name, place_id,
            rating, review_text, payload
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            [
                r.review_id,
                r.source_name,
                r.created_at,
                r.place_name,
                r.place_id,
                r.rating,
                r.review_text,
                r.payload,
            ]
            for r in records
        ],
    )


def _refresh_place_dim(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("delete from place_dim")
    conn.execute(
        """
        insert into place_dim
        with all_places as (
            select source_name, place_id, place_name, lat, lon, started_at as event_ts from visits
            union all
            select source_name, place_id, place_name, lat, lon, saved_at as event_ts from saved_places
        )
        select
            coalesce(place_id, concat('coord:', cast(round(lat, 5) as varchar), ',', cast(round(lon, 5) as varchar))) as place_key,
            source_name,
            place_id,
            any_value(place_name) as place_name,
            avg(lat) as lat,
            avg(lon) as lon,
            min(event_ts) as first_seen_at,
            max(event_ts) as last_seen_at
        from all_places
        where place_id is not null or (lat is not null and lon is not null)
        group by 1,2,3
        """
    )
