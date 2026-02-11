#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

API_URL = "https://ws.audioscrobbler.com/2.0/"

DEFAULT_RAW_ROOT = Path(
    os.environ.get(
        "DATALAKE_RAW_ROOT",
        "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports",
    )
)
DEFAULT_OUTPUT_DIR = DEFAULT_RAW_ROOT / "lastfm" / "scrobbles"


class LastfmIngestError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Last.fm scrobbles and merge them into month-partitioned JSONL files."
        )
    )
    parser.add_argument(
        "--since",
        nargs="?",
        const="auto",
        default=None,
        help=(
            "Incremental mode. Use '--since' for auto-detect from existing JSONL, "
            "or provide a unix timestamp / ISO datetime. Omit for full-history fetch."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for month-partitioned JSONL output (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--user",
        default=os.environ.get("LASTFM_USER"),
        help="Last.fm username (default: LASTFM_USER env var)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("LASTFM_API_KEY"),
        help="Last.fm API key (default: LASTFM_API_KEY env var)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Items per API request (max 200, default: 200)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30)",
    )
    return parser.parse_args()


def iter_jsonl_files(root: Path):
    if not root.exists():
        return
    for path in sorted(root.rglob("*.jsonl")):
        if path.is_file():
            yield path


def parse_uts_from_row(row: dict[str, Any]) -> int | None:
    if "uts" in row:
        try:
            return int(row["uts"])
        except (TypeError, ValueError):
            return None

    date_block = row.get("date")
    if isinstance(date_block, dict) and "uts" in date_block:
        try:
            return int(date_block["uts"])
        except (TypeError, ValueError):
            return None

    return None


def find_latest_uts_in_jsonl(root: Path) -> int | None:
    latest: int | None = None
    for jsonl_path in iter_jsonl_files(root):
        with jsonl_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                uts = parse_uts_from_row(row)
                if uts is None:
                    continue
                if latest is None or uts > latest:
                    latest = uts
    return latest


def parse_since_value(since: str | None, output_dir: Path) -> int | None:
    if since is None:
        return None
    if since == "auto":
        return find_latest_uts_in_jsonl(output_dir)

    try:
        return int(since)
    except ValueError:
        pass

    iso_candidate = since.rstrip("Z")
    try:
        parsed = dt.datetime.fromisoformat(iso_candidate)
    except ValueError as exc:
        raise LastfmIngestError(
            "--since must be 'auto', a unix timestamp, or ISO datetime"
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)

    return int(parsed.timestamp())


def fetch_recent_tracks(
    *,
    user: str,
    api_key: str,
    from_uts: int | None,
    page_size: int,
    timeout: int,
) -> list[dict[str, Any]]:
    if not user:
        raise LastfmIngestError("Missing Last.fm user. Set LASTFM_USER or pass --user.")
    if not api_key:
        raise LastfmIngestError(
            "Missing Last.fm API key. Set LASTFM_API_KEY or pass --api-key."
        )

    rows: list[dict[str, Any]] = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        params: dict[str, Any] = {
            "method": "user.getRecentTracks",
            "user": user,
            "api_key": api_key,
            "format": "json",
            "limit": min(max(page_size, 1), 200),
            "page": page,
        }
        if from_uts is not None:
            params["from"] = from_uts

        response = requests.get(API_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()

        recent_tracks = payload.get("recenttracks", {})
        tracks = recent_tracks.get("track", [])
        attr = recent_tracks.get("@attr", {})

        try:
            total_pages = int(attr.get("totalPages", "1"))
        except ValueError:
            total_pages = 1

        if isinstance(tracks, dict):
            tracks = [tracks]

        for track in tracks:
            # Ignore the synthetic currently-playing row.
            if track.get("@attr", {}).get("nowplaying") == "true":
                continue

            uts = parse_uts_from_row(track)
            if uts is None:
                continue

            row = {
                "uts": uts,
                "artist": (track.get("artist") or {}).get("#text"),
                "track": track.get("name"),
                "album": (track.get("album") or {}).get("#text") or None,
                "mbid": track.get("mbid") or None,
            }
            rows.append(row)

        page += 1

    return rows


def month_partition_path(output_dir: Path, uts: int) -> Path:
    ts = dt.datetime.fromtimestamp(uts, tz=dt.timezone.utc)
    return output_dir / f"year={ts.year:04d}" / f"month={ts.month:02d}" / "scrobbles.jsonl"


def scrobble_identity(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (row.get("uts"), row.get("artist"), row.get("track"), row.get("album"))


def merge_into_monthly_jsonl(rows: list[dict[str, Any]], output_dir: Path) -> dict[str, int]:
    if not rows:
        return {"inserted": 0, "deduped": 0, "files_touched": 0}

    by_file: dict[Path, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        uts = parse_uts_from_row(row)
        if uts is None:
            continue
        row["uts"] = uts
        by_file[month_partition_path(output_dir, uts)].append(row)

    inserted = 0
    deduped = 0

    for file_path, pending_rows in by_file.items():
        existing_keys: set[tuple[Any, Any, Any, Any]] = set()

        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    existing_keys.add(scrobble_identity(row))

        unique_new_rows: list[dict[str, Any]] = []
        for row in pending_rows:
            key = scrobble_identity(row)
            if key in existing_keys:
                deduped += 1
                continue
            existing_keys.add(key)
            unique_new_rows.append(row)

        if not unique_new_rows:
            continue

        unique_new_rows.sort(key=lambda r: int(r["uts"]))
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("a", encoding="utf-8") as fh:
            for row in unique_new_rows:
                fh.write(json.dumps(row, ensure_ascii=True) + "\n")
                inserted += 1

    return {"inserted": inserted, "deduped": deduped, "files_touched": len(by_file)}


def main() -> None:
    args = parse_args()

    since_uts = parse_since_value(args.since, args.output_dir)
    rows = fetch_recent_tracks(
        user=args.user,
        api_key=args.api_key,
        from_uts=since_uts,
        page_size=args.page_size,
        timeout=args.timeout,
    )

    if since_uts is not None:
        # Last.fm 'from' may include the boundary timestamp.
        rows = [row for row in rows if int(row.get("uts", 0)) > since_uts]

    summary = merge_into_monthly_jsonl(rows, args.output_dir)

    print(f"Fetched: {len(rows)}")
    print(f"Inserted: {summary['inserted']}")
    print(f"Deduped: {summary['deduped']}")
    print(f"Files touched: {summary['files_touched']}")


if __name__ == "__main__":
    main()
