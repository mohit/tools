#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import shlex
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

import duckdb
import requests

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
DEFAULT_AUTOMATION_ENV_FILE = (
    Path.home() / "code" / "tools" / "strava-data-puller" / ".env"
)


class StravaRateLimiter:
    """Conservative client-side limiter to stay under Strava API quotas."""

    def __init__(
        self,
        short_window_limit: int = 100,
        short_window_seconds: int = 15 * 60,
        daily_limit: int = 1000,
        safety_margin: int = 2,
    ) -> None:
        self.short_window_limit = max(1, short_window_limit - safety_margin)
        self.short_window_seconds = short_window_seconds
        self.daily_limit = max(1, daily_limit - safety_margin)
        self.short_window_requests: deque[float] = deque()
        self.daily_requests: deque[float] = deque()

    def _prune(self, now: float) -> None:
        while self.short_window_requests and now - self.short_window_requests[0] >= self.short_window_seconds:
            self.short_window_requests.popleft()
        while self.daily_requests and now - self.daily_requests[0] >= 24 * 60 * 60:
            self.daily_requests.popleft()

    def wait_for_slot(self) -> None:
        while True:
            now = time.time()
            self._prune(now)

            if len(self.daily_requests) >= self.daily_limit:
                raise SystemExit(
                    "Daily Strava API limit reached locally; retry after the daily window resets."
                )

            if len(self.short_window_requests) < self.short_window_limit:
                return

            sleep_for = self.short_window_requests[0] + self.short_window_seconds - now + 1
            if sleep_for > 0:
                print(
                    f"Approaching Strava 15-minute limit; sleeping {int(sleep_for)}s to stay under quota.",
                    file=sys.stderr,
                )
                time.sleep(sleep_for)

    def note_request(self) -> None:
        now = time.time()
        self.short_window_requests.append(now)
        self.daily_requests.append(now)


def is_readable_file(path: Path) -> bool:
    return path.exists() and path.is_file() and os.access(path, os.R_OK)


def parse_date(value: str) -> int:
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Dates must be YYYY-MM-DD") from exc
    return int(parsed.replace(tzinfo=dt.UTC).timestamp())


def parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not is_readable_file(path):
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
        DEFAULT_AUTOMATION_ENV_FILE,
    ):
        if path not in candidate_paths:
            candidate_paths.append(path)
    return candidate_paths


def write_credentials_env_file(path: Path, credentials: dict[str, str]) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f'{key}="{credentials[key]}"' for key in REQUIRED_STRAVA_VARS]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(0o600)


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
        if not is_readable_file(env_file):
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


def request_json(
    endpoint: str,
    token: str,
    params: dict[str, Any] | None = None,
    rate_limiter: StravaRateLimiter | None = None,
) -> Any:
    url = f"{STRAVA_API_BASE}{endpoint}"

    for attempt in range(MAX_RETRIES):
        if rate_limiter:
            rate_limiter.wait_for_slot()

        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )

        if rate_limiter:
            rate_limiter.note_request()

        if response.status_code == 429:
            delay = BASE_DELAY * (2**attempt)
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

    raise SystemExit(f"Rate limit exceeded after {MAX_RETRIES} retries. Try again later.")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def write_ndjson(path: Path, payloads: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")


def parse_activity_timestamp(activity: dict[str, Any]) -> int | None:
    start_date_str = activity.get("start_date")
    if not isinstance(start_date_str, str) or not start_date_str:
        return None
    try:
        parsed = dt.datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(parsed.timestamp())


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_existing_activities(out_dir: Path) -> tuple[list[dict[str, Any]], int | None]:
    ndjson_path = out_dir / "activities.ndjson"
    existing_activities: list[dict[str, Any]] = []
    latest_timestamp: int | None = None

    if not ndjson_path.exists():
        return existing_activities, latest_timestamp

    with ndjson_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                activity = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(activity, dict):
                continue

            existing_activities.append(activity)
            timestamp = parse_activity_timestamp(activity)
            if timestamp is not None and (latest_timestamp is None or timestamp > latest_timestamp):
                latest_timestamp = timestamp

    return existing_activities, latest_timestamp


def fetch_athlete(token: str, out_dir: Path, rate_limiter: StravaRateLimiter | None = None) -> int:
    athlete = request_json("/athlete", token, rate_limiter=rate_limiter)
    if not isinstance(athlete, dict) or "id" not in athlete:
        raise SystemExit("Unexpected athlete response from Strava API")
    write_json(out_dir / "athlete.json", athlete)
    return int(athlete["id"])


def fetch_stats(
    token: str,
    athlete_id: int,
    out_dir: Path,
    rate_limiter: StravaRateLimiter | None = None,
) -> None:
    stats = request_json(f"/athletes/{athlete_id}/stats", token, rate_limiter=rate_limiter)
    if not isinstance(stats, dict):
        raise SystemExit("Unexpected stats response from Strava API")
    write_json(out_dir / "stats.json", stats)


def fetch_activities(
    token: str,
    types: set[str],
    after: int | None,
    before: int | None,
    per_page: int,
    max_pages: int,
    rate_limiter: StravaRateLimiter | None = None,
) -> list[dict[str, Any]]:
    activities: list[dict[str, Any]] = []
    page = 1
    while page <= max_pages:
        params: dict[str, int] = {"page": page, "per_page": per_page}
        if after:
            params["after"] = after
        if before:
            params["before"] = before

        batch = request_json("/athlete/activities", token, params, rate_limiter=rate_limiter)
        if not isinstance(batch, list):
            raise SystemExit("Unexpected activities response from Strava API")
        if not batch:
            break

        filtered = [
            activity
            for activity in batch
            if isinstance(activity, dict) and activity.get("type") in types
        ]
        activities.extend(filtered)
        page += 1

    return activities


def fetch_activity_details(
    token: str,
    out_dir: Path,
    activity_id: int,
    rate_limiter: StravaRateLimiter | None = None,
) -> dict[str, Any]:
    activity = request_json(f"/activities/{activity_id}", token, rate_limiter=rate_limiter)
    if not isinstance(activity, dict):
        raise SystemExit(f"Unexpected activity detail response for {activity_id}")
    write_json(out_dir / "activities" / f"{activity_id}.json", activity)
    return activity


def fetch_activity_streams(
    token: str,
    out_dir: Path,
    activity_id: int,
    rate_limiter: StravaRateLimiter | None = None,
) -> dict[str, Any]:
    streams = request_json(
        f"/activities/{activity_id}/streams",
        token,
        {
            "keys": "time,distance,latlng,altitude,velocity_smooth,heartrate,cadence,watts,grade_smooth,temp,moving,grade_smooth,"
            "avg_grade_adjusted_speed",
            "key_by_type": "true",
        },
        rate_limiter=rate_limiter,
    )
    if not isinstance(streams, dict):
        raise SystemExit(f"Unexpected stream response for {activity_id}")
    write_json(out_dir / "streams" / f"{activity_id}.json", streams)
    return streams


def build_activity_details_ndjson(out_dir: Path, activity_ids: set[int] | None = None) -> int:
    details_dir = out_dir / "activities"
    detail_files = sorted(details_dir.glob("*.json"), key=lambda p: int(p.stem)) if details_dir.exists() else []
    records: list[dict[str, Any]] = []
    for path in detail_files:
        try:
            activity_id = int(path.stem)
        except ValueError:
            continue
        if activity_ids is not None and activity_id not in activity_ids:
            continue

        try:
            payload = load_json(path)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            records.append(payload)

    ndjson_path = out_dir / "activity_details.ndjson"
    if records:
        write_ndjson(ndjson_path, records)
    elif ndjson_path.exists():
        ndjson_path.unlink()
    return len(records)


def build_activity_streams_ndjson(out_dir: Path, activity_ids: set[int] | None = None) -> int:
    streams_dir = out_dir / "streams"
    stream_files = sorted(streams_dir.glob("*.json"), key=lambda p: int(p.stem)) if streams_dir.exists() else []
    records: list[dict[str, Any]] = []
    point_count_streams = ("time", "distance", "heartrate", "watts", "cadence")

    for path in stream_files:
        try:
            activity_id = int(path.stem)
        except ValueError:
            continue
        if activity_ids is not None and activity_id not in activity_ids:
            continue

        try:
            payload = load_json(path)
        except (OSError, json.JSONDecodeError):
            continue

        if not isinstance(payload, dict):
            continue

        record = {"activity_id": activity_id}
        record.update(payload)
        for stream_key in point_count_streams:
            stream_value = record.get(stream_key)
            if not isinstance(stream_value, dict):
                record[stream_key] = {"data": []}
                continue
            if not isinstance(stream_value.get("data"), list):
                stream_copy = dict(stream_value)
                stream_copy["data"] = []
                record[stream_key] = stream_copy
        records.append(record)

    ndjson_path = out_dir / "activity_streams.ndjson"
    if records:
        write_ndjson(ndjson_path, records)
    elif ndjson_path.exists():
        ndjson_path.unlink()
    return len(records)


def activity_in_scope(
    activity: dict[str, Any],
    types: set[str],
    after: int | None,
    before: int | None,
) -> bool:
    if activity.get("type") not in types:
        return False

    timestamp = parse_activity_timestamp(activity)
    if timestamp is None:
        return False

    if after is not None and timestamp < after:
        return False
    if before is not None and timestamp > before:
        return False
    return True


def detail_has_expected_fields(detail: dict[str, Any]) -> bool:
    has_laps_key = "laps" in detail
    has_split_key = "splits_metric" in detail or "splits_standard" in detail
    return has_laps_key and has_split_key


def streams_have_data(streams: dict[str, Any]) -> bool:
    if not streams:
        return False

    for stream in streams.values():
        if not isinstance(stream, dict):
            continue
        values = stream.get("data")
        if isinstance(values, list) and values:
            return True
    return False


def find_missing_detail_ids(
    activities: list[dict[str, Any]],
    out_dir: Path,
    types: set[str],
    after: int | None,
    before: int | None,
    include_streams: bool,
) -> list[tuple[int, list[str]]]:
    candidates: list[tuple[int, list[str]]] = []

    for activity in activities:
        if not activity_in_scope(activity, types, after, before):
            continue

        activity_id = activity.get("id")
        if not isinstance(activity_id, int):
            continue

        reasons: list[str] = []
        detail_path = out_dir / "activities" / f"{activity_id}.json"
        if not detail_path.exists():
            reasons.append("missing_detail_file")
        else:
            try:
                detail_payload = load_json(detail_path)
            except (OSError, json.JSONDecodeError):
                reasons.append("invalid_detail_file")
            else:
                if not isinstance(detail_payload, dict) or not detail_has_expected_fields(detail_payload):
                    reasons.append("missing_laps_or_splits")

        if include_streams:
            streams_path = out_dir / "streams" / f"{activity_id}.json"
            if not streams_path.exists():
                reasons.append("missing_streams_file")
            else:
                try:
                    streams_payload = load_json(streams_path)
                except (OSError, json.JSONDecodeError):
                    reasons.append("invalid_streams_file")
                else:
                    if not isinstance(streams_payload, dict) or not streams_have_data(streams_payload):
                        reasons.append("empty_streams")

        if reasons:
            candidates.append((activity_id, reasons))

    return candidates


def collect_in_scope_activity_ids(
    activities: list[dict[str, Any]],
    types: set[str],
    after: int | None,
    before: int | None,
) -> set[int]:
    activity_ids: set[int] = set()
    for activity in activities:
        if not activity_in_scope(activity, types, after, before):
            continue
        activity_id = activity.get("id")
        if isinstance(activity_id, int):
            activity_ids.add(activity_id)
    return activity_ids


def export_parquet(out_dir: Path) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE OR REPLACE TABLE activities_raw AS SELECT * FROM read_json_auto(?)",
        [str(out_dir / "activities.ndjson")],
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE activities AS
        SELECT
            *,
            TRY_CAST(distance AS DOUBLE) / 1000.0 AS distance_km,
            TRY_CAST(moving_time AS DOUBLE) / 60.0 AS moving_time_min,
            TRY_CAST(elapsed_time AS DOUBLE) / 3600.0 AS elapsed_time_hours,
            CASE
                WHEN TRY_CAST(moving_time AS DOUBLE) > 0
                THEN (TRY_CAST(distance AS DOUBLE) / 1000.0) / (TRY_CAST(moving_time AS DOUBLE) / 3600.0)
                ELSE NULL
            END AS average_speed_kph,
            CASE
                WHEN TRY_CAST(elapsed_time AS DOUBLE) > 0
                THEN TRY_CAST(moving_time AS DOUBLE) / TRY_CAST(elapsed_time AS DOUBLE)
                ELSE NULL
            END AS moving_ratio,
            CASE
                WHEN TRY_CAST(distance AS DOUBLE) > 0
                THEN TRY_CAST(total_elevation_gain AS DOUBLE) / (TRY_CAST(distance AS DOUBLE) / 1000.0)
                ELSE NULL
            END AS elevation_gain_per_km
        FROM activities_raw
        """
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
            "CREATE OR REPLACE TABLE activity_details_raw AS SELECT * FROM read_json_auto(?)",
            [str(out_dir / "activity_details.ndjson")],
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE activity_details AS
            SELECT
                *,
                COALESCE(array_length(laps), 0) AS lap_count,
                COALESCE(array_length(splits_standard), 0) AS splits_standard_count,
                COALESCE(array_length(splits_metric), 0) AS splits_metric_count
            FROM activity_details_raw
            """
        )

    if (out_dir / "activity_streams.ndjson").exists():
        con.execute(
            "CREATE OR REPLACE TABLE activity_streams_raw AS SELECT * FROM read_json_auto(?)",
            [str(out_dir / "activity_streams.ndjson")],
        )
        stream_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info('activity_streams_raw')").fetchall()
            if len(row) > 1 and isinstance(row[1], str)
        }
        stream_point_definitions = [
            ("time", "time_points"),
            ("distance", "distance_points"),
            ("heartrate", "heartrate_points"),
            ("watts", "power_points"),
            ("cadence", "cadence_points"),
        ]
        point_count_projection = ",\n                ".join(
            f"COALESCE(array_length({stream_key}.data), 0) AS {point_name}"
            if stream_key in stream_columns
            else f"0 AS {point_name}"
            for stream_key, point_name in stream_point_definitions
        )
        con.execute(
            """
            CREATE OR REPLACE TABLE activity_streams AS
            SELECT
                *,
                """
            + point_count_projection
            + """
            FROM activity_streams_raw
            """
        )

    con.execute("COPY activities TO ? (FORMAT 'parquet')", [str(out_dir / "activities.parquet")])
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
    parser.add_argument("--backfill-details", action="store_true")
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
    parser.add_argument(
        "--install-credentials",
        action="store_true",
        help=(
            "Write discovered credentials to a stable .env file for automated runs and "
            "exit without calling the Strava API."
        ),
    )
    parser.add_argument(
        "--credentials-file",
        default=str(DEFAULT_AUTOMATION_ENV_FILE),
        help=(
            "Path to write with --install-credentials "
            "(default: ~/code/tools/strava-data-puller/.env)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    credentials, sources, searched_env_files = resolve_strava_credentials()
    missing_vars = [var for var in REQUIRED_STRAVA_VARS if var not in credentials]
    if missing_vars:
        raise SystemExit(format_missing_credentials_message(missing_vars, searched_env_files))

    if args.install_credentials:
        credentials_file = Path(args.credentials_file).expanduser()
        write_credentials_env_file(credentials_file, credentials)
        print(f"Wrote Strava credentials to {credentials_file}")
        print("Run --check-credentials to confirm automation discovery.")
        return

    if args.check_credentials:
        print("Strava credentials available for automated runs:")
        for var in REQUIRED_STRAVA_VARS:
            print(f"- {var}: {sources[var]}")
        return

    client_id = credentials["STRAVA_CLIENT_ID"]
    client_secret = credentials["STRAVA_CLIENT_SECRET"]
    refresh_token = credentials["STRAVA_REFRESH_TOKEN"]

    access_token = get_access_token(client_id, client_secret, refresh_token)
    rate_limiter = StravaRateLimiter()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    existing_activities: list[dict[str, Any]] = []
    auto_after: int | None = None

    if not args.force:
        existing_activities, auto_after = load_existing_activities(out_dir)
        if existing_activities:
            print(f"Found {len(existing_activities)} existing activities.")
            print("Use --force to re-fetch all activities.")

    after_param = args.after
    if after_param is None and auto_after is not None and not args.force:
        buffer_seconds = 7 * 24 * 60 * 60
        after_param = auto_after - buffer_seconds
        print(f"Auto-setting --after to {after_param} (latest - 7 days) to catch late uploads/edits.")

    if args.force:
        for ndjson_file in ("activity_details.ndjson", "activity_streams.ndjson"):
            ndjson_path = out_dir / ndjson_file
            if ndjson_path.exists():
                ndjson_path.unlink()

    athlete_id = fetch_athlete(access_token, out_dir, rate_limiter=rate_limiter)
    fetch_stats(access_token, athlete_id, out_dir, rate_limiter=rate_limiter)

    activity_types = parse_types(args.types)
    fetched_activities = fetch_activities(
        access_token,
        activity_types,
        after_param,
        args.before,
        args.per_page,
        args.max_pages,
        rate_limiter=rate_limiter,
    )

    activity_map: dict[int, dict[str, Any]] = {}
    for activity in existing_activities:
        activity_id = activity.get("id")
        if isinstance(activity_id, int):
            activity_map[activity_id] = activity

    new_activity_ids: set[int] = set()
    for activity in fetched_activities:
        activity_id = activity.get("id")
        if not isinstance(activity_id, int):
            continue
        if activity_id not in activity_map:
            new_activity_ids.add(activity_id)
        activity_map[activity_id] = activity

    final_activities = list(activity_map.values())
    final_activities.sort(key=lambda x: x.get("start_date", ""), reverse=True)

    write_json(out_dir / "activities.json", final_activities)
    write_ndjson(out_dir / "activities.ndjson", final_activities)

    fetched_count = len(fetched_activities)
    new_count = len(new_activity_ids)
    print(f"Fetched {fetched_count} records (overlapping). Found {new_count} truly new activities.")

    details_or_streams_updated = False

    include_streams_for_new = args.include_streams or args.backfill_details
    for activity_id in sorted(new_activity_ids):
        fetch_activity_details(access_token, out_dir, activity_id, rate_limiter=rate_limiter)
        details_or_streams_updated = True
        if include_streams_for_new:
            fetch_activity_streams(access_token, out_dir, activity_id, rate_limiter=rate_limiter)

    if args.backfill_details:
        candidates = find_missing_detail_ids(
            final_activities,
            out_dir,
            activity_types,
            args.after,
            args.before,
            include_streams=True,
        )
        print(f"Backfill scan complete. {len(candidates)} activities missing detail/stream data.")

        for activity_id, reasons in candidates:
            reason_text = ", ".join(reasons)
            print(f"Backfilling activity {activity_id} ({reason_text})")
            fetch_activity_details(access_token, out_dir, activity_id, rate_limiter=rate_limiter)
            fetch_activity_streams(access_token, out_dir, activity_id, rate_limiter=rate_limiter)
            details_or_streams_updated = True

    if details_or_streams_updated:
        current_activity_ids = collect_in_scope_activity_ids(
            final_activities,
            activity_types,
            args.after,
            args.before,
        )
        detail_rows = build_activity_details_ndjson(out_dir, current_activity_ids)
        stream_rows = build_activity_streams_ndjson(out_dir, current_activity_ids)
        print(f"Rebuilt detail indexes: {detail_rows} detail records, {stream_rows} stream records.")

    if not args.skip_parquet:
        export_parquet(out_dir)

    print(f"Sync complete. Total library size: {len(final_activities)} activities.")


if __name__ == "__main__":
    main()
