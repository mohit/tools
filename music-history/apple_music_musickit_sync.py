import argparse
import json
import time
from pathlib import Path

import duckdb


DEFAULT_RAW_ROOT = Path.home() / "datalake.me" / "raw" / "apple-music" / "musickit"
DEFAULT_CURATED_ROOT = Path.home() / "datalake.me" / "curated" / "apple-music" / "recent-played"
BASE_URL = "https://api.music.apple.com/v1/me/recent/played/tracks"
MAX_PAGES = 5
PAGE_LIMIT = 10


def fetch_recent_tracks(developer_token: str, user_token: str) -> dict:
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError("requests is required for MusicKit sync. Install dependencies with `uv sync`.") from exc

    headers = {
        "Authorization": f"Bearer {developer_token}",
        "Music-User-Token": user_token,
    }

    all_rows = []
    next_url = f"{BASE_URL}?limit={PAGE_LIMIT}"
    page = 0

    while next_url and page < MAX_PAGES:
        response = requests.get(next_url, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()

        rows = payload.get("data", [])
        all_rows.extend(rows)

        next_url = payload.get("next")
        if next_url and next_url.startswith("/"):
            next_url = f"https://api.music.apple.com{next_url}"
        page += 1

    return {
        "fetched_at_utc": int(time.time()),
        "source": "musickit_recent_played",
        "max_items_expected": MAX_PAGES * PAGE_LIMIT,
        "data": all_rows,
    }


def write_raw_snapshot(raw_root: Path, payload: dict) -> Path:
    raw_root.mkdir(parents=True, exist_ok=True)
    out_path = raw_root / f"recent_played_{payload['fetched_at_utc']}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def upsert_curated(payload: dict, curated_root: Path) -> int:
    curated_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        ingested_at = payload["fetched_at_utc"]
        raw_json = json.dumps(payload["data"]).replace("'", "''")

        incoming_query = f"""
WITH src AS (
    SELECT *
    FROM json_each('{raw_json}')
), parsed AS (
    SELECT
        json_extract_string(value, '$.id') AS track_id,
        json_extract_string(value, '$.attributes.name') AS track,
        json_extract_string(value, '$.attributes.artistName') AS artist,
        json_extract_string(value, '$.attributes.albumName') AS album,
        json_extract_string(value, '$.attributes.playParams.id') AS play_params_id,
        json_extract_string(value, '$.attributes.url') AS track_url,
        CAST({ingested_at} AS BIGINT) AS ingested_at,
        to_timestamp(CAST({ingested_at} AS BIGINT)) AS fetched_at_utc,
        'musickit_recent_played' AS source,
        strftime(to_timestamp(CAST({ingested_at} AS BIGINT)), '%Y') AS year,
        strftime(to_timestamp(CAST({ingested_at} AS BIGINT)), '%m') AS month
    FROM src
)
SELECT *
FROM parsed
WHERE track_id IS NOT NULL
""".strip()

        has_existing = bool(list(curated_root.glob("year=*/month=*/*.parquet")))
        existing_glob = str(curated_root / "year=*" / "month=*" / "*.parquet").replace("'", "''")

        if has_existing:
            merged_query = f"""
WITH incoming AS ({incoming_query}),
existing AS (
    SELECT
        track_id,
        track,
        artist,
        album,
        play_params_id,
        track_url,
        ingested_at,
        fetched_at_utc,
        source,
        strftime(fetched_at_utc, '%Y') AS year,
        strftime(fetched_at_utc, '%m') AS month
    FROM read_parquet('{existing_glob}', hive_partitioning=TRUE)
), all_rows AS (
    SELECT * FROM existing
    UNION ALL
    SELECT * FROM incoming
), deduped AS (
    SELECT * EXCLUDE(rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY track_id, fetched_at_utc
                ORDER BY ingested_at DESC
            ) AS rn
        FROM all_rows
    )
    WHERE rn = 1
)
SELECT * FROM deduped
""".strip()
        else:
            merged_query = incoming_query

        tmp_root = curated_root.parent / f".tmp_recent_played_{int(time.time())}"
        if tmp_root.exists():
            import shutil

            shutil.rmtree(tmp_root)
        tmp_root.mkdir(parents=True, exist_ok=True)

        con.sql(
            f"""
COPY ({merged_query})
TO '{str(tmp_root).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (year, month))
"""
        )

        row_count = con.sql(
            f"SELECT COUNT(*) FROM read_parquet('{str(tmp_root / 'year=*' / 'month=*' / '*.parquet').replace("'", "''")}', hive_partitioning=TRUE)"
        ).fetchone()[0]

        import shutil

        if curated_root.exists():
            shutil.rmtree(curated_root)
        tmp_root.rename(curated_root)

        return row_count
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch MusicKit recent played tracks (max ~50) and store as supplemental snapshots. "
            "This does not provide full Apple Music listening history."
        )
    )
    parser.add_argument("--developer-token", required=True)
    parser.add_argument("--user-token", required=True)
    parser.add_argument("--raw-root", type=Path, default=Path(str(DEFAULT_RAW_ROOT)))
    parser.add_argument("--curated-root", type=Path, default=Path(str(DEFAULT_CURATED_ROOT)))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    payload = fetch_recent_tracks(
        developer_token=args.developer_token,
        user_token=args.user_token,
    )

    raw_path = write_raw_snapshot(raw_root=Path(args.raw_root).expanduser(), payload=payload)
    row_count = upsert_curated(payload=payload, curated_root=Path(args.curated_root).expanduser())

    print(f"Fetched {len(payload['data'])} recent played tracks (MusicKit cap is ~50).")
    print(f"Raw snapshot: {raw_path}")
    print(f"Curated rows: {row_count}")


if __name__ == "__main__":
    main()
