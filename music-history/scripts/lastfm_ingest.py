#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
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
    os.environ.get("DATALAKE_RAW_ROOT", str(Path.home() / "datalake.me/raw"))
)
DEFAULT_CURATED_ROOT = Path(
    os.environ.get("DATALAKE_CURATED_ROOT", str(Path.home() / "datalake.me/curated"))
)
STATE_DIR = Path.home() / ".local" / "share" / "datalake"
STATE_FILE = STATE_DIR / "lastfm_last_uts.txt"
CHECKPOINT_FILE = STATE_DIR / "lastfm_ingest_checkpoint.json"


def parse_since(value: str) -> int:
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Invalid --since value. Use YYYY-MM-DD or ISO-8601 UTC like 2026-01-01T00:00:00Z."
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    else:
        parsed = parsed.astimezone(dt.UTC)
    return int(parsed.timestamp())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Last.fm scrobbles into raw JSONL and parquet")
    parser.add_argument(
        "--from",
        "--from-uts",
        dest="from_uts",
        type=int,
        help="Unix timestamp (UTC seconds) to fetch from",
    )
    parser.add_argument(
        "--since",
        type=parse_since,
        help="Fetch from this UTC timestamp (YYYY-MM-DD or ISO-8601)",
    )
    parser.add_argument(
        "--full-refetch",
        action="store_true",
        help="Ignore state/raw history and fetch full Last.fm history",
    )
    parser.add_argument(
        "--user",
        default=None,
        help="Last.fm username (overrides LASTFM_USER)",
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


def resolve_user(explicit_user: str | None = None, default_user: str | None = None) -> str:
    if explicit_user:
        return explicit_user
    from_env = os.getenv("LASTFM_USER")
    if from_env:
        return from_env
    if default_user:
        return default_user
    raise SystemExit("Missing Last.fm user. Set LASTFM_USER or pass --user.")


def load_last_uts(state_file: Path = STATE_FILE) -> int | None:
    if state_file.exists():
        try:
            return int(state_file.read_text().strip())
        except ValueError:
            pass
    return None


def save_last_uts(value: int, state_file: Path = STATE_FILE) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(str(value))


def load_checkpoint(checkpoint_file: Path = CHECKPOINT_FILE) -> dict[str, Any] | None:
    if not checkpoint_file.exists():
        return None
    with checkpoint_file.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_checkpoint(
    from_uts: int | None,
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


def extract_uts(row: dict[str, Any]) -> int | None:
    uts = row.get("uts")
    if uts is not None:
        try:
            return int(uts)
        except (TypeError, ValueError):
            return None

    date = row.get("date") or {}
    nested_uts = date.get("uts")
    if nested_uts is not None:
        try:
            return int(nested_uts)
        except (TypeError, ValueError):
            return None

    return None


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def load_last_uts_from_raw(raw_root: Path) -> int | None:
    if not raw_root.exists():
        return None

    latest: int | None = None
    for path in raw_root.glob("scrobbles_*.jsonl"):
        for row in iter_jsonl(path):
            uts = extract_uts(row)
            if uts is None:
                continue
            if latest is None or uts > latest:
                latest = uts
    return latest


def _normalize_text(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("#text")
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def row_key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (
        int(row["uts"]),
        _normalize_text(row.get("artist")),
        _normalize_text(row.get("track") or row.get("name")),
        _normalize_text(row.get("album")),
    )


def month_file_for_uts(raw_root: Path, uts: int) -> Path:
    parsed = pd.to_datetime(uts, unit="s", utc=True)
    return raw_root / f"scrobbles_{parsed.year:04d}-{parsed.month:02d}.jsonl"


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    played_at = out.get("played_at_utc")
    if isinstance(played_at, pd.Timestamp):
        out["played_at_utc"] = played_at.isoformat()
    return out


def merge_raw_monthly_jsonl(rows: list[dict[str, Any]], raw_root: Path) -> int:
    if not rows:
        return 0

    raw_root.mkdir(parents=True, exist_ok=True)

    by_month: dict[Path, list[dict[str, Any]]] = collections.defaultdict(list)
    for row in rows:
        by_month[month_file_for_uts(raw_root, int(row["uts"]))].append(_serialize_row(row))

    updated_files = 0
    for month_file, month_rows in by_month.items():
        merged: dict[tuple[Any, Any, Any, Any], dict[str, Any]] = {}

        if month_file.exists():
            for existing in iter_jsonl(month_file):
                existing_uts = extract_uts(existing)
                if existing_uts is None:
                    continue
                existing_for_key = dict(existing)
                existing_for_key["uts"] = existing_uts
                existing_for_key["artist"] = _normalize_text(existing.get("artist"))
                existing_for_key["track"] = _normalize_text(existing.get("track") or existing.get("name"))
                existing_for_key["album"] = _normalize_text(existing.get("album"))
                merged[row_key(existing_for_key)] = existing

        for row in month_rows:
            merged[row_key(row)] = row

        sorted_rows = sorted(merged.values(), key=lambda r: int(extract_uts(r) or 0))
        tmp_path = month_file.with_suffix(".jsonl.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            for row in sorted_rows:
                handle.write(json.dumps(row, ensure_ascii=False))
                handle.write("\n")
        tmp_path.replace(month_file)
        updated_files += 1

    return updated_files


def determine_from_uts(raw_root: Path, state_file: Path = STATE_FILE) -> int | None:
    raw_last = load_last_uts_from_raw(raw_root)
    state_last = load_last_uts(state_file)

    candidates = [value for value in (raw_last, state_last) if value is not None]
    if not candidates:
        return None
    return max(candidates) + 1


def request_recent_tracks(
    user: str,
    api_key: str,
    from_uts: int | None,
    page: int,
    max_retries: int = MAX_RETRIES,
    base_delay_seconds: int = BASE_DELAY_SECONDS,
) -> dict[str, Any]:
    params = {
        "method": "user.getRecentTracks",
        "user": user,
        "api_key": api_key,
        "format": "json",
        "limit": 200,
        "page": page,
    }
    if from_uts is not None:
        params["from"] = from_uts

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

    # Fallback ensures cross-page dedupe still works for callers that do not
    # carry an in-memory seen_keys set across page writes.
    effective_seen_keys = seen_keys
    if effective_seen_keys is None:
        effective_seen_keys = load_seen_keys_for_run(curated_root=curated_root, run_id=run_id)

    deduped_rows = dedupe_rows(rows=rows, seen_keys=effective_seen_keys)
    if not deduped_rows:
        return 0

    df = pd.DataFrame(deduped_rows)
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
    fallback_from_uts: int | None,
) -> tuple[int | None, int, int, int | None]:
    if args.from_uts is not None and args.since is not None:
        raise SystemExit("Use only one of --from/--from-uts or --since.")

    if args.full_refetch:
        return None, 1, int(time.time()), None

    explicit_from = args.from_uts if args.from_uts is not None else args.since
    if explicit_from is not None:
        return explicit_from, 1, int(time.time()), None

    if checkpoint and not args.no_resume:
        try:
            return (
                int(checkpoint["from_uts"]) if checkpoint.get("from_uts") is not None else None,
                int(checkpoint.get("next_page", 1)),
                int(checkpoint.get("run_id", int(time.time()))),
                checkpoint.get("max_uts_seen"),
            )
        except (TypeError, ValueError, KeyError):
            pass

    return fallback_from_uts, 1, int(time.time()), None


def main() -> None:
    args = parse_args()
    user = resolve_user(explicit_user=args.user, default_user=os.getenv("LASTFM_USER_DEFAULT"))
    api_key = load_env("LASTFM_API_KEY")
    raw_root = Path(os.getenv("DATALAKE_RAW_ROOT", str(DEFAULT_RAW_ROOT))) / "lastfm"
    curated_root = Path(os.getenv("DATALAKE_CURATED_ROOT", str(DEFAULT_CURATED_ROOT))) / "lastfm" / "scrobbles"

    state_from_uts = determine_from_uts(raw_root=raw_root)
    checkpoint = None if args.no_resume else load_checkpoint()
    from_uts, page, run_id, max_uts_seen = resolve_start(args, checkpoint, state_from_uts)
    seen_keys = load_seen_keys_for_run(curated_root=curated_root, run_id=run_id)
    print(
        f"Starting Last.fm ingest: user={user}, from_uts={from_uts}, start_page={page}, "
        f"run_id={run_id}, resume={'yes' if checkpoint and not args.no_resume else 'no'}"
    )

    pages_processed = 0
    rows_written = 0
    raw_files_updated = 0

    while True:
        payload = request_recent_tracks(user=user, api_key=api_key, from_uts=from_uts, page=page)
        recent = payload.get("recenttracks", {})
        tracks = recent.get("track", [])
        rows = normalize(tracks if isinstance(tracks, list) else [])

        if not rows:
            print(f"No rows on page {page}; finishing.")
            break

        page_rows = rows
        raw_files_updated += merge_raw_monthly_jsonl(rows=page_rows, raw_root=raw_root)
        page_rows_written = append_parquet_partitions(
            curated_root=curated_root,
            run_id=run_id,
            page=page,
            rows=page_rows,
            seen_keys=seen_keys,
        )
        rows_written += page_rows_written
        page_max = max(row["uts"] for row in page_rows)
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
        print(
            f"Processed page {page}/{total_pages}: fetched={len(page_rows)} "
            f"deduped={page_rows_written}"
        )

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
        f"Ingest complete: pages={pages_processed}, parquet_rows={rows_written}, raw_month_files={raw_files_updated}, "
        f"last_uts={max_uts_seen}"
    )


if __name__ == "__main__":
    main()
