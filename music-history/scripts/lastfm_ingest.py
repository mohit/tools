#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

API = "https://ws.audioscrobbler.com/2.0/"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 5

DEFAULT_RAW_ROOT = Path(
    "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports"
)
DEFAULT_CURATED_ROOT = Path(
    "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated"
)
STATE_DIR = Path.home() / ".local" / "share" / "datalake"
STATE_FILE = STATE_DIR / "lastfm_last_uts.txt"
CHECKPOINT_FILE = STATE_DIR / "lastfm_ingest_checkpoint.json"
SEEN_KEYS_CACHE: dict[tuple[Path, int], set[tuple[Any, Any, Any, Any]]] = {}


def parse_date(value: str) -> int:
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Dates must be YYYY-MM-DD") from exc
    return int(parsed.replace(tzinfo=dt.timezone.utc).timestamp())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Last.fm scrobbles into raw JSONL and parquet")
    parser.add_argument(
        "--from",
        dest="from_uts",
        type=int,
        help="Unix timestamp (UTC seconds) to fetch from",
    )
    parser.add_argument(
        "--since",
        type=parse_date,
        help="Fetch from this UTC date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoint and start fresh for the selected range",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional page limit for debugging",
    )
    return parser.parse_args()


def load_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise SystemExit(f"Missing required env var: {var_name}")
    return value


def load_last_uts(state_file: Path = STATE_FILE) -> int:
    if state_file.exists():
        try:
            return int(state_file.read_text().strip())
        except ValueError:
            pass
    return int(time.time()) - 30 * 24 * 3600


def save_last_uts(value: int, state_file: Path = STATE_FILE) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(str(value))


def load_checkpoint(checkpoint_file: Path = CHECKPOINT_FILE) -> dict[str, Any] | None:
    if not checkpoint_file.exists():
        return None
    with checkpoint_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_checkpoint(
    from_uts: int,
    next_page: int,
    run_id: int,
    max_uts_seen: int | None,
    checkpoint_file: Path = CHECKPOINT_FILE,
) -> None:
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "from_uts": from_uts,
        "next_page": next_page,
        "run_id": run_id,
        "max_uts_seen": max_uts_seen,
        "saved_at": int(time.time()),
    }
    with checkpoint_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)


def clear_checkpoint(checkpoint_file: Path = CHECKPOINT_FILE) -> None:
    if checkpoint_file.exists():
        checkpoint_file.unlink()


def request_recent_tracks(
    user: str,
    api_key: str,
    from_uts: int,
    page: int,
    max_retries: int = MAX_RETRIES,
    base_delay_seconds: int = BASE_DELAY_SECONDS,
) -> dict[str, Any]:
    params = {
        "method": "user.getRecentTracks",
        "user": user,
        "api_key": api_key,
        "format": "json",
        "from": from_uts,
        "limit": 200,
        "page": page,
    }

    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = requests.get(API, params=params, timeout=30)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt == max_retries - 1:
                break
            delay = base_delay_seconds * (2**attempt)
            print(
                f"Transient network error on page {page}: {exc}. "
                f"Retrying in {delay}s ({attempt + 1}/{max_retries})..."
            )
            time.sleep(delay)
            continue

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == max_retries - 1:
                response.raise_for_status()
            delay = base_delay_seconds * (2**attempt)
            print(
                f"Transient HTTP {response.status_code} on page {page}. "
                f"Retrying in {delay}s ({attempt + 1}/{max_retries})..."
            )
            time.sleep(delay)
            continue

        response.raise_for_status()
        payload = response.json()
        if payload.get("error"):
            raise SystemExit(f"Last.fm API error {payload.get('error')}: {payload.get('message')}")
        return payload

    raise SystemExit(f"Failed to fetch page {page} after {max_retries} retries: {last_error}")


def normalize(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        if "@attr" in item and item["@attr"].get("nowplaying") == "true":
            continue
        date = item.get("date")
        if not date or "uts" not in date:
            continue
        uts = int(date["uts"])
        rows.append(
            {
                "uts": uts,
                "played_at_utc": pd.to_datetime(uts, unit="s", utc=True),
                "artist": item.get("artist", {}).get("#text"),
                "track": item.get("name"),
                "album": item.get("album", {}).get("#text") or None,
                "mbid_track": item.get("mbid") or None,
                "source": "lastfm",
            }
        )
    return rows


def write_raw_page(raw_root: Path, run_id: int, page: int, rows: list[dict[str, Any]]) -> Path:
    raw_root.mkdir(parents=True, exist_ok=True)
    raw_path = raw_root / f"recent_{run_id}_page_{page:04d}.jsonl"
    with raw_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str, sort_keys=True))
            handle.write("\n")
    return raw_path


def scrobble_key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (
        row.get("uts"),
        row.get("artist"),
        row.get("track"),
        row.get("album"),
    )


def dedupe_rows(
    rows: list[dict[str, Any]],
    seen_keys: set[tuple[Any, Any, Any, Any]],
) -> list[dict[str, Any]]:
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        key = scrobble_key(row)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_rows.append(row)
    return unique_rows


def load_seen_keys_for_run(
    curated_root: Path,
    run_id: int,
) -> set[tuple[Any, Any, Any, Any]]:
    seen_keys: set[tuple[Any, Any, Any, Any]] = set()
    pattern = f"scrobbles_{run_id}_p*.parquet"
    for parquet_file in curated_root.rglob(pattern):
        table = pq.read_table(parquet_file, columns=["uts", "artist", "track", "album"])
        for row in table.to_pylist():
            seen_keys.add(scrobble_key(row))
    return seen_keys


def append_parquet_partitions(
    curated_root: Path,
    run_id: int,
    page: int,
    rows: list[dict[str, Any]],
    seen_keys: set[tuple[Any, Any, Any, Any]] | None = None,
) -> int:
    if not rows:
        return 0

    dedupe_keys = seen_keys
    if dedupe_keys is None:
        cache_key = (curated_root.resolve(), run_id)
        if cache_key not in SEEN_KEYS_CACHE:
            SEEN_KEYS_CACHE[cache_key] = load_seen_keys_for_run(curated_root=curated_root, run_id=run_id)
        dedupe_keys = SEEN_KEYS_CACHE[cache_key]

    rows = dedupe_rows(rows=rows, seen_keys=dedupe_keys)
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    if df.empty:
        return 0

    df["year"] = df["played_at_utc"].dt.year.astype(int)
    df["month"] = df["played_at_utc"].dt.month.astype(int)

    written = 0
    for (year, month), group in df.groupby(["year", "month"]):
        part_dir = curated_root / f"year={year:04d}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / f"scrobbles_{run_id}_p{page:04d}.parquet"
        table = pa.Table.from_pandas(group.drop(columns=["year", "month"]), preserve_index=False)
        pq.write_table(table, out_file)
        written += len(group)
    return written


def resolve_start(
    args: argparse.Namespace,
    checkpoint: dict[str, Any] | None,
    fallback_from_uts: int,
) -> tuple[int, int, int, int | None]:
    if args.from_uts is not None and args.since is not None:
        raise SystemExit("Use only one of --from or --since.")

    explicit_from = args.from_uts if args.from_uts is not None else args.since
    if explicit_from is not None:
        return explicit_from, 1, int(time.time()), None

    if checkpoint and not args.no_resume:
        try:
            return (
                int(checkpoint["from_uts"]),
                int(checkpoint.get("next_page", 1)),
                int(checkpoint.get("run_id", int(time.time()))),
                checkpoint.get("max_uts_seen"),
            )
        except (TypeError, ValueError, KeyError):
            pass

    return fallback_from_uts, 1, int(time.time()), None


def main() -> None:
    args = parse_args()
    user = load_env("LASTFM_USER")
    api_key = load_env("LASTFM_API_KEY")
    raw_root = Path(os.getenv("DATALAKE_RAW_ROOT", str(DEFAULT_RAW_ROOT))) / "lastfm"
    curated_root = Path(os.getenv("DATALAKE_CURATED_ROOT", str(DEFAULT_CURATED_ROOT))) / "lastfm" / "scrobbles"

    state_from_uts = load_last_uts()
    checkpoint = None if args.no_resume else load_checkpoint()
    from_uts, page, run_id, max_uts_seen = resolve_start(args, checkpoint, state_from_uts)
    print(
        f"Starting Last.fm ingest: from_uts={from_uts}, start_page={page}, "
        f"run_id={run_id}, resume={'yes' if checkpoint and not args.no_resume else 'no'}"
    )

    pages_processed = 0
    rows_written = 0

    while True:
        payload = request_recent_tracks(user=user, api_key=api_key, from_uts=from_uts, page=page)
        recent = payload.get("recenttracks", {})
        tracks = recent.get("track", [])
        rows = normalize(tracks if isinstance(tracks, list) else [])

        if not rows:
            print(f"No rows on page {page}; finishing.")
            break

        write_raw_page(raw_root=raw_root, run_id=run_id, page=page, rows=rows)
        rows_written += append_parquet_partitions(
            curated_root=curated_root,
            run_id=run_id,
            page=page,
            rows=rows,
        )
        page_max = max(row["uts"] for row in rows)
        max_uts_seen = page_max if max_uts_seen is None else max(max_uts_seen, page_max)

        next_page = page + 1
        save_checkpoint(
            from_uts=from_uts,
            next_page=next_page,
            run_id=run_id,
            max_uts_seen=max_uts_seen,
        )

        pages_processed += 1
        attr = recent.get("@attr", {})
        total_pages = int(attr.get("totalPages", "1"))
        print(f"Processed page {page}/{total_pages}: {len(rows)} rows")

        if args.max_pages is not None and pages_processed >= args.max_pages:
            print(f"Reached --max-pages={args.max_pages}; stopping early.")
            return

        if page >= total_pages:
            break
        page = next_page

    if max_uts_seen is None:
        clear_checkpoint()
        print("No new rows found.")
        return

    save_last_uts(max_uts_seen)
    clear_checkpoint()
    print(
        f"Ingest complete: pages={pages_processed}, parquet_rows={rows_written}, "
        f"last_uts={max_uts_seen}"
    )


if __name__ == "__main__":
    main()
