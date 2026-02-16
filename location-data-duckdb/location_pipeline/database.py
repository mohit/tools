from __future__ import annotations

import duckdb

DDL = """
create table if not exists ingestion_runs (
    run_id varchar,
    source_name varchar,
    started_at timestamp,
    finished_at timestamp,
    status varchar,
    message varchar
);

create table if not exists raw_events (
    event_id varchar,
    source_name varchar,
    event_ts timestamp,
    lat double,
    lon double,
    place_id varchar,
    payload json,
    ingested_at timestamp default current_timestamp
);

create table if not exists visits (
    visit_id varchar,
    source_name varchar,
    started_at timestamp,
    ended_at timestamp,
    lat double,
    lon double,
    place_name varchar,
    place_id varchar,
    list_name varchar,
    confidence double,
    payload json,
    ingested_at timestamp default current_timestamp
);

create table if not exists saved_places (
    saved_id varchar,
    source_name varchar,
    saved_at timestamp,
    place_name varchar,
    place_id varchar,
    lat double,
    lon double,
    list_name varchar,
    notes varchar,
    payload json,
    ingested_at timestamp default current_timestamp
);

create table if not exists place_reviews (
    review_id varchar,
    source_name varchar,
    created_at timestamp,
    place_name varchar,
    place_id varchar,
    rating double,
    review_text varchar,
    payload json,
    ingested_at timestamp default current_timestamp
);

create table if not exists place_dim (
    place_key varchar,
    source_name varchar,
    place_id varchar,
    place_name varchar,
    lat double,
    lon double,
    first_seen_at timestamp,
    last_seen_at timestamp
);

create table if not exists place_enrichment_google (
    place_id varchar,
    fetched_at timestamp,
    formatted_address varchar,
    rating double,
    user_ratings_total integer,
    primary_type varchar,
    payload json
);
"""


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL)
