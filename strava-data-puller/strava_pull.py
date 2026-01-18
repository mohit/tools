#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

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
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["access_token"]


def request_json(endpoint: str, token: str, params: dict | None = None) -> dict:
    url = f"{STRAVA_API_BASE}{endpoint}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )
    if response.status_code >= 400:
        print(
            f"Request failed ({response.status_code}) for {url}: {response.text}",
            file=sys.stderr,
        )
        response.raise_for_status()
    return response.json()


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


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
    return activities


def fetch_activity_details(token: str, out_dir: Path, activity_id: int) -> None:
    activity = request_json(f"/activities/{activity_id}", token)
    write_json(out_dir / "activities" / f"{activity_id}.json", activity)


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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client_id = load_env("STRAVA_CLIENT_ID")
    client_secret = load_env("STRAVA_CLIENT_SECRET")
    refresh_token = load_env("STRAVA_REFRESH_TOKEN")

    access_token = get_access_token(client_id, client_secret, refresh_token)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    athlete_id = fetch_athlete(access_token, out_dir)
    fetch_stats(access_token, athlete_id, out_dir)

    activity_types = parse_types(args.types)
    activities = fetch_activities(
        access_token,
        out_dir,
        activity_types,
        args.after,
        args.before,
        args.per_page,
        args.max_pages,
    )

    for activity in activities:
        activity_id = activity["id"]
        fetch_activity_details(access_token, out_dir, activity_id)
        if args.include_streams:
            fetch_activity_streams(access_token, out_dir, activity_id)

    print(f"Exported {len(activities)} activities to {out_dir}")


if __name__ == "__main__":
    main()
