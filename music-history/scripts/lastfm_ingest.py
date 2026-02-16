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
=======
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

API = "https://ws.audioscrobbler.com/2.0/"
DEFAULT_TIMEOUT_CONNECT = 10
DEFAULT_TIMEOUT_READ = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_LIMIT = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Last.fm scrobbles into datalake")
    parser.add_argument("--from-uts", type=int, default=None, help="Fetch scrobbles strictly after this unix timestamp")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Fetch scrobbles since this datetime (ISO-8601, e.g. 2026-01-31T00:00:00Z)",
    )
    parser.add_argument(
        "--full-refetch",
        action="store_true",
        help="Ignore state and fetch full history from uts=0",
    )
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_TIMEOUT_CONNECT)
    parser.add_argument("--read-timeout", type=float, default=DEFAULT_TIMEOUT_READ)
    parser.add_argument("--backoff-base", type=float, default=DEFAULT_BACKOFF_BASE_SECONDS)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    return parser.parse_args()


def parse_since_to_uts(since_value: str) -> int:
    candidate = since_value.strip()
    if candidate.isdigit():
        return int(candidate)

    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def load_last_uts(state_file: Path) -> int | None:
    if not state_file.exists():
        return None

    raw = state_file.read_text().strip()
    if not raw:
        return None

    try:
        return int(raw)
    except ValueError:
        return None


def save_last_uts(state_file: Path, value: int) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(str(value))


def latest_uts_from_curated(curated_root: Path) -> int | None:
    if not curated_root.exists():
        return None

    max_uts: int | None = None
    for parquet_file in curated_root.rglob("*.parquet"):
        table = pq.read_table(parquet_file, columns=["uts"])
        if table.num_rows == 0:
            continue
        column_max = table.column("uts").to_pandas().max()
        if pd.isna(column_max):
            continue
        value = int(column_max)
        if max_uts is None or value > max_uts:
            max_uts = value
    return max_uts


def resolve_from_uts(
    cli_from_uts: int | None,
    cli_since: str | None,
    full_refetch: bool,
    state_file: Path,
    curated_root: Path,
) -> int:
    if full_refetch:
        return 0
    if cli_from_uts is not None:
        # Last.fm `from` is inclusive; +1 preserves strict "after this timestamp" CLI semantics.
        return cli_from_uts + 1
    if cli_since is not None:
        return parse_since_to_uts(cli_since)

    state_uts = load_last_uts(state_file)
    curated_uts = latest_uts_from_curated(curated_root)
    latest_known = max([v for v in [state_uts, curated_uts] if v is not None], default=None)

    if latest_known is None:
        return int(time.time()) - (30 * 24 * 3600)

    # Last.fm `from` is inclusive. Add 1 second to avoid re-fetching the last known scrobble.
    return latest_known + 1


def normalize(items: list[dict]) -> list[dict]:
    out: list[dict] = []
    for item in items:
        if "@attr" in item and item["@attr"].get("nowplaying") == "true":
            continue

        date_info = item.get("date")
        if not date_info or "uts" not in date_info:
            continue

        uts = int(date_info["uts"])
        out.append(
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
    return out


def fetch_page_with_retries(
    session: requests.Session,
    user: str,
    api_key: str,
    from_uts: int,
    page: int,
    limit: int,
    connect_timeout: float,
    read_timeout: float,
    max_retries: int,
    backoff_base: float,
) -> dict:
    params = {
        "method": "user.getRecentTracks",
        "user": user,
        "api_key": api_key,
        "format": "json",
        "from": from_uts,
        "limit": limit,
        "page": page,
    }

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(API, params=params, timeout=(connect_timeout, read_timeout))
            response.raise_for_status()
            return response.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, requests.JSONDecodeError) as exc:
            last_error = exc
            if attempt == max_retries:
                break

            # Exponential backoff with jitter to avoid synchronized retries.
            sleep_seconds = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
            print(
                f"Request failed on page {page} attempt {attempt}/{max_retries}: {exc}. "
                f"Retrying in {sleep_seconds:.2f}s..."
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch Last.fm page={page} after {max_retries} attempts") from last_error


def write_raw_jsonl(raw_dir: Path, rows: list[dict]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_path = raw_dir / f"recent_{int(time.time())}.jsonl"
    with out_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str) + "\n")


def append_parquet_partitions(df: pd.DataFrame, curated_root: Path) -> None:
    if df.empty:
        return

    df = df.copy()
    df["year"] = df["played_at_utc"].dt.year.astype(int)
    df["month"] = df["played_at_utc"].dt.month.astype(int)

    for (year, month), group in df.groupby(["year", "month"]):
        part_dir = curated_root / f"year={year:04d}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / f"scrobbles_{int(time.time())}.parquet"
        table = pa.Table.from_pandas(group.drop(columns=["year", "month"]), preserve_index=False)
        pq.write_table(table, out_file)


def run() -> int:
    args = parse_args()

    user = os.environ["LASTFM_USER"]
    api_key = os.environ["LASTFM_API_KEY"]

    raw_root = Path(
        os.environ.get(
            "DATALAKE_RAW_ROOT",
            "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports",
        )
    )
    curated_root = Path(
        os.environ.get(
            "DATALAKE_CURATED_ROOT",
            "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated",
        )
    )

    raw_dir = raw_root / "lastfm"
    curated_dir = curated_root / "lastfm" / "scrobbles"

    state_dir = Path.home() / ".local" / "share" / "datalake"
    state_file = state_dir / "lastfm_last_uts.txt"

    from_uts = resolve_from_uts(args.from_uts, args.since, args.full_refetch, state_file, curated_dir)
    print(f"Starting Last.fm fetch from uts={from_uts} (full_refetch={args.full_refetch})")

    all_rows: list[dict] = []
    page = 1

    with requests.Session() as session:
        while True:
            payload = fetch_page_with_retries(
                session=session,
                user=user,
                api_key=api_key,
                from_uts=from_uts,
                page=page,
                limit=args.limit,
                connect_timeout=args.connect_timeout,
                read_timeout=args.read_timeout,
                max_retries=args.max_retries,
                backoff_base=args.backoff_base,
            )
            recent = payload.get("recenttracks", {})
            tracks = recent.get("track", [])
            rows = normalize(tracks)

            if not rows:
                break

            all_rows.extend(rows)

            attr = recent.get("@attr", {})
            total_pages = int(attr.get("totalPages", "1"))
            print(f"Fetched page {page}/{total_pages}, rows={len(rows)}")

            if page >= total_pages:
                break
            page += 1

    if not all_rows:
        print("No new scrobbles found.")
        return 0

    write_raw_jsonl(raw_dir, all_rows)

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["uts", "artist", "track", "album"])
    append_parquet_partitions(df, curated_dir)

    max_uts = int(df["uts"].max())
    save_last_uts(state_file, max_uts)
    print(f"Ingested {len(df)} deduplicated scrobbles. Updated state to uts={max_uts}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
