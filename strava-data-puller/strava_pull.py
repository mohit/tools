#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import shlex
import subprocess
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
REQUIRED_STRAVA_VARS = (
    "STRAVA_CLIENT_ID",
    "STRAVA_CLIENT_SECRET",
    "STRAVA_REFRESH_TOKEN",
)


def parse_date(value: str) -> int:
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Dates must be YYYY-MM-DD") from exc
    return int(parsed.replace(tzinfo=dt.timezone.utc).timestamp())


def parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[len("export ") :].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if (value.startswith('"') and value.endswith('"')) or (
                    value.startswith("'") and value.endswith("'")
                ):
                    value = value[1:-1]
                values[key] = value
    except OSError:
        return {}
    return values


def discover_env_files() -> list[Path]:
    candidate_paths: list[Path] = []
    explicit_env_file = os.getenv("STRAVA_ENV_FILE")
    if explicit_env_file:
        candidate_paths.append(Path(explicit_env_file).expanduser())

    script_dir = Path(__file__).resolve().parent
    cwd = Path.cwd()
    for path in (
        script_dir / ".env",
        cwd / ".env",
        Path.home() / "code" / "tools" / "strava-data-puller" / ".env",
    ):
        if path not in candidate_paths:
            candidate_paths.append(path)
    return candidate_paths


def load_keychain_secret(var_name: str) -> str | None:
    # macOS keychain fallback for unattended runs.
    lookups = (
        ("strava-data-puller", var_name),
        ("com.mohit.tools.strava-data-puller", var_name),
        (var_name, None),
    )

    for service, account in lookups:
        cmd = ["security", "find-generic-password", "-w", "-s", service]
        if account:
            cmd.extend(["-a", account])
        try:
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except FileNotFoundError:
            return None
        except subprocess.TimeoutExpired:
            continue
        if result.returncode == 0:
            secret = result.stdout.strip()
            if secret:
                return secret
    return None


def resolve_strava_credentials() -> tuple[dict[str, str], dict[str, str], list[Path]]:
    values: dict[str, str] = {}
    sources: dict[str, str] = {}

    for var_name in REQUIRED_STRAVA_VARS:
        env_value = os.getenv(var_name)
        if env_value:
            values[var_name] = env_value
            sources[var_name] = "environment"

    env_files = discover_env_files()
    for env_file in env_files:
        if all(var_name in values for var_name in REQUIRED_STRAVA_VARS):
            break
        if not env_file.exists() or not env_file.is_file() or not os.access(env_file, os.R_OK):
            continue
        env_values = parse_dotenv(env_file)
        for var_name in REQUIRED_STRAVA_VARS:
            if var_name in values:
                continue
            env_value = env_values.get(var_name)
            if env_value:
                values[var_name] = env_value
                sources[var_name] = f"dotenv:{env_file}"

    for var_name in REQUIRED_STRAVA_VARS:
        if var_name in values:
            continue
        keychain_value = load_keychain_secret(var_name)
        if keychain_value:
            values[var_name] = keychain_value
            sources[var_name] = "keychain"

    return values, sources, env_files


def format_missing_credentials_message(
    missing_vars: list[str], searched_env_files: list[Path]
) -> str:
    env_locations = ", ".join(shlex.quote(str(path)) for path in searched_env_files)
    missing = ", ".join(missing_vars)
    return (
        f"Missing Strava credentials: {missing}\n"
        "Credential lookup order: environment variables -> .env files -> macOS keychain.\n"
        f"Searched .env paths: {env_locations}\n"
        "To automate runs, add credentials to one of those .env files or keychain."
    )


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


def load_existing_activities(out_dir: Path) -> tuple[list[dict], int | None]:
    """Load existing activities and find the most recent activity timestamp.
    
    Returns:
        Tuple of (list of activity dicts, most recent start_date as Unix timestamp or None)
    """
    ndjson_path = out_dir / "activities.ndjson"
    existing_activities: list[dict] = []
    latest_timestamp: int | None = None
    
    if not ndjson_path.exists():
        return existing_activities, latest_timestamp
    
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                activity = json.loads(line)
                existing_activities.append(activity)
                
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
    
    return existing_activities, latest_timestamp


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
    # Note: We do NOT write to files here anymore, that happens in main() after merging
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
    parser.add_argument(
        "--check-credentials",
        action="store_true",
        help="Validate credential discovery and exit without calling the Strava API.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    credentials, sources, searched_env_files = resolve_strava_credentials()
    missing_vars = [var for var in REQUIRED_STRAVA_VARS if var not in credentials]
    if missing_vars:
        raise SystemExit(
            format_missing_credentials_message(missing_vars, searched_env_files)
        )

    if args.check_credentials:
        print("Strava credentials available for automated runs:")
        for var in REQUIRED_STRAVA_VARS:
            print(f"- {var}: {sources[var]}")
        return

    client_id = credentials["STRAVA_CLIENT_ID"]
    client_secret = credentials["STRAVA_CLIENT_SECRET"]
    refresh_token = credentials["STRAVA_REFRESH_TOKEN"]

    access_token = get_access_token(client_id, client_secret, refresh_token)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Load existing activities for incremental sync
    existing_activities: list[dict] = []
    auto_after: int | None = None
    
    if not args.force:
        existing_activities, auto_after = load_existing_activities(out_dir)
        if existing_activities:
            print(f"Found {len(existing_activities)} existing activities.")
            print("Use --force to re-fetch all activities.")
    
    # Use auto_after with 7-day buffer if no explicit --after was provided
    after_param = args.after
    if after_param is None and auto_after is not None and not args.force:
        # Buffer 7 days (7 * 24 * 60 * 60 seconds)
        buffer_seconds = 7 * 24 * 60 * 60
        after_param = auto_after - buffer_seconds
        print(f"Auto-setting --after to {after_param} (latest - 7 days) to catch late uploads/edits.")
    
    # Only clear detail files if forcing full refresh
    if args.force:
        for ndjson_file in ("activity_details.ndjson", "activity_streams.ndjson"):
            ndjson_path = out_dir / ndjson_file
            if ndjson_path.exists():
                ndjson_path.unlink()

    athlete_id = fetch_athlete(access_token, out_dir)
    fetch_stats(access_token, athlete_id, out_dir)

    activity_types = parse_types(args.types)
    fetched_activities = fetch_activities(
        access_token,
        out_dir,
        activity_types,
        after_param,
        args.before,
        args.per_page,
        args.max_pages,
    )
    
    # Merge strategy:
    # 1. Create a dict of all activities by ID (existing + fetched)
    #    Since we process existing first, then fetched, updates from fetched will overwrite existing
    activity_map = {a["id"]: a for a in existing_activities}
    
    # 2. Update/Add new fetched activities
    new_activity_ids = set()
    for activity in fetched_activities:
        # Only consider it "new" if we didn't have it before
        if activity["id"] not in activity_map:
            new_activity_ids.add(activity["id"])
        activity_map[activity["id"]] = activity
        
    # 3. Convert back to list and sort by start_date
    final_activities = list(activity_map.values())
    final_activities.sort(key=lambda x: x.get("start_date", ""), reverse=True)
    
    # 4. Write merged list to files
    write_json(out_dir / "activities.json", final_activities)
    write_ndjson(out_dir / "activities.ndjson", final_activities)

    fetched_count = len(fetched_activities)
    new_count = len(new_activity_ids)
    
    print(f"Fetched {fetched_count} records (overlapping). Found {new_count} truly new activities.")

    # Only fetch details for the TRULY new activities
    for activity_id in new_activity_ids:
        # Note: We can't easily get the 'activity' dict here without iterating map, 
        # but fetch_activity_details needs ID anyway.
        fetch_activity_details(access_token, out_dir, activity_id)
        if args.include_streams:
            fetch_activity_streams(access_token, out_dir, activity_id)

    if not args.skip_parquet:
        export_parquet(out_dir)

    print(f"Sync complete. Total library size: {len(final_activities)} activities.")


if __name__ == "__main__":
    main()
