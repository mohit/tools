#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

import duckdb
import requests

import time

STRAVA_TOKEN_URL = "https://www.strava.com/oauth/token"
STRAVA_API_BASE = "https://www.strava.com/api/v3"
DEFAULT_TYPES = [
    "Ride",
    "VirtualRide",
    "GravelRide",
    "EBikeRide",
    "Walk",
    "Hike",
    "TrailRun",
    "Run",
]

MAX_RETRIES = 5
BASE_DELAY = 15  # seconds


def parse_date(value: str) -> int:
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Dates must be YYYY-MM-DD") from exc
    return int(parsed.replace(tzinfo=dt.timezone.utc).timestamp())


def load_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise SystemExit(f"Missing required env var: {var_name}")
    return value


def get_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    response = requests.post(
        STRAVA_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=60,
    )
    if response.status_code != 200:
        print(f"Error fetching token: {response.status_code} - {response.text}")
    response.raise_for_status()
    payload = response.json()
    return payload["access_token"]


def request_json(endpoint: str, token: str, params: dict | None = None) -> dict:
    url = f"{STRAVA_API_BASE}{endpoint}"
    
    for attempt in range(MAX_RETRIES):
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        
        if response.status_code == 429:
            # Rate limited - wait and retry
            delay = BASE_DELAY * (2 ** attempt)
            print(f"Rate limited. Waiting {delay}s before retry ({attempt + 1}/{MAX_RETRIES})...")
            time.sleep(delay)
            continue
        
        if response.status_code >= 400:
            print(
                f"Request failed ({response.status_code}) for {url}: {response.text}",
                file=sys.stderr,
            )
            response.raise_for_status()
        
        return response.json()
    
    # Exhausted retries
    raise SystemExit(f"Rate limit exceeded after {MAX_RETRIES} retries. Try again later.")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def write_ndjson(path: Path, payloads: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")


def append_ndjson(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def load_existing_activities(out_dir: Path) -> tuple[set[int], int | None]:
    """Load existing activity IDs and find the most recent activity timestamp.
    
    Returns:
        Tuple of (set of activity IDs, most recent start_date as Unix timestamp or None)
    """
    ndjson_path = out_dir / "activities.ndjson"
    existing_ids: set[int] = set()
    latest_timestamp: int | None = None
    
    if not ndjson_path.exists():
        return existing_ids, latest_timestamp
    
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                activity = json.loads(line)
                activity_id = activity.get("id")
                if activity_id:
                    existing_ids.add(activity_id)
                
                # Parse start_date to find the latest
                start_date_str = activity.get("start_date")
                if start_date_str:
                    # Parse ISO format like "2024-01-15T10:30:00Z"
                    parsed = dt.datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
                    ts = int(parsed.timestamp())
                    if latest_timestamp is None or ts > latest_timestamp:
                        latest_timestamp = ts
            except (json.JSONDecodeError, ValueError):
                continue
    
    return existing_ids, latest_timestamp


def fetch_athlete(token: str, out_dir: Path) -> int:
    athlete = request_json("/athlete", token)
    write_json(out_dir / "athlete.json", athlete)
    return athlete["id"]


def fetch_stats(token: str, athlete_id: int, out_dir: Path) -> None:
    stats = request_json(f"/athletes/{athlete_id}/stats", token)
    write_json(out_dir / "stats.json", stats)


def fetch_activities(
    token: str,
    out_dir: Path,
    types: set[str],
    after: int | None,
    before: int | None,
    per_page: int,
    max_pages: int,
) -> list[dict]:
    activities: list[dict] = []
    page = 1
    while page <= max_pages:
        params = {"page": page, "per_page": per_page}
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        batch = request_json("/athlete/activities", token, params)
        if not batch:
            break
        filtered = [activity for activity in batch if activity.get("type") in types]
        activities.extend(filtered)
        page += 1
    write_json(out_dir / "activities.json", activities)
    write_ndjson(out_dir / "activities.ndjson", activities)
    return activities


def fetch_activity_details(token: str, out_dir: Path, activity_id: int) -> None:
    activity = request_json(f"/activities/{activity_id}", token)
    write_json(out_dir / "activities" / f"{activity_id}.json", activity)
    append_ndjson(out_dir / "activity_details.ndjson", activity)


def fetch_activity_streams(token: str, out_dir: Path, activity_id: int) -> None:
    streams = request_json(
        f"/activities/{activity_id}/streams",
        token,
        {
            "keys": "time,distance,latlng,altitude,velocity_smooth,heartrate,cadence,watts,grade_smooth,temp,moving,grade_smooth,"
            "avg_grade_adjusted_speed",
            "key_by_type": "true",
        },
    )
    write_json(out_dir / "streams" / f"{activity_id}.json", streams)
    append_ndjson(out_dir / "activity_streams.ndjson", streams)


def export_parquet(out_dir: Path) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE OR REPLACE TABLE activities AS SELECT * FROM read_json_auto(?)",
        [str(out_dir / "activities.ndjson")],
    )
    con.execute(
        "CREATE OR REPLACE TABLE athlete AS SELECT * FROM read_json_auto(?)",
        [str(out_dir / "athlete.json")],
    )
    con.execute(
        "CREATE OR REPLACE TABLE stats AS SELECT * FROM read_json_auto(?)",
        [str(out_dir / "stats.json")],
    )
    if (out_dir / "activity_details.ndjson").exists():
        con.execute(
            "CREATE OR REPLACE TABLE activity_details AS SELECT * FROM read_json_auto(?)",
            [str(out_dir / "activity_details.ndjson")],
        )
    if (out_dir / "activity_streams.ndjson").exists():
        con.execute(
            "CREATE OR REPLACE TABLE activity_streams AS SELECT * FROM read_json_auto(?)",
            [str(out_dir / "activity_streams.ndjson")],
        )

    con.execute(
        "COPY activities TO ? (FORMAT 'parquet')",
        [str(out_dir / "activities.parquet")],
    )
    con.execute("COPY athlete TO ? (FORMAT 'parquet')", [str(out_dir / "athlete.parquet")])
    con.execute("COPY stats TO ? (FORMAT 'parquet')", [str(out_dir / "stats.parquet")])
    if (out_dir / "activity_details.ndjson").exists():
        con.execute(
            "COPY activity_details TO ? (FORMAT 'parquet')",
            [str(out_dir / "activity_details.parquet")],
        )
    if (out_dir / "activity_streams.ndjson").exists():
        con.execute(
            "COPY activity_streams TO ? (FORMAT 'parquet')",
            [str(out_dir / "activity_streams.parquet")],
        )


def parse_types(raw_types: str | None) -> set[str]:
    if not raw_types:
        return set(DEFAULT_TYPES)
    return {value.strip() for value in raw_types.split(",") if value.strip()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull Strava data locally")
    parser.add_argument("--out-dir", default="./strava-export")
    parser.add_argument("--types", default=None)
    parser.add_argument("--after", type=parse_date)
    parser.add_argument("--before", type=parse_date)
    parser.add_argument("--include-streams", action="store_true")
    parser.add_argument("--per-page", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Skip DuckDB parquet export for JSON outputs.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-fetch all activities, ignoring existing data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client_id = load_env("STRAVA_CLIENT_ID")
    client_secret = load_env("STRAVA_CLIENT_SECRET")
    refresh_token = load_env("STRAVA_REFRESH_TOKEN")

    access_token = get_access_token(client_id, client_secret, refresh_token)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Load existing activities for incremental sync
    existing_ids: set[int] = set()
    auto_after: int | None = None
    
    if not args.force:
        existing_ids, auto_after = load_existing_activities(out_dir)
        if existing_ids:
            print(f"Found {len(existing_ids)} existing activities. Fetching only new ones.")
            print("Use --force to re-fetch all activities.")
    
    # Use auto_after if no explicit --after was provided
    after_param = args.after
    if after_param is None and auto_after is not None and not args.force:
        after_param = auto_after
        print(f"Auto-setting --after to latest activity date.")
    
    # Only clear detail files if forcing full refresh
    if args.force:
        for ndjson_file in ("activity_details.ndjson", "activity_streams.ndjson"):
            ndjson_path = out_dir / ndjson_file
            if ndjson_path.exists():
                ndjson_path.unlink()

    athlete_id = fetch_athlete(access_token, out_dir)
    fetch_stats(access_token, athlete_id, out_dir)

    activity_types = parse_types(args.types)
    activities = fetch_activities(
        access_token,
        out_dir,
        activity_types,
        after_param,
        args.before,
        args.per_page,
        args.max_pages,
    )
    
    # Filter to only new activities
    new_activities = [a for a in activities if a["id"] not in existing_ids]
    skipped_count = len(activities) - len(new_activities)
    
    if skipped_count > 0:
        print(f"Skipping {skipped_count} already-fetched activities.")

    for activity in new_activities:
        activity_id = activity["id"]
        fetch_activity_details(access_token, out_dir, activity_id)
        if args.include_streams:
            fetch_activity_streams(access_token, out_dir, activity_id)

    if not args.skip_parquet:
        export_parquet(out_dir)

    print(f"Exported {len(new_activities)} new activities to {out_dir} ({skipped_count} skipped)")


if __name__ == "__main__":
    main()
