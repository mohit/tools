#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
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
STALENESS_STATE_FILE = STATE_DIR / "lastfm_staleness.json"

# Days without a new scrobble before the ingest is declared stale and exits non-zero.
STALE_THRESHOLD_DAYS: int = 7

# Catalog YAML that downstream tools read for dataset metadata.
CATALOG_FILE = Path(
    os.environ.get(
        "LASTFM_CATALOG_FILE",
        str(Path.home() / "datalake.me" / "catalog" / "lastfm.yaml"),
    )
)


def parse_date(value: str) -> int:
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Dates must be YYYY-MM-DD") from exc
    return int(parsed.replace(tzinfo=dt.UTC).timestamp())


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
        type=parse_date,
        help="Fetch from this UTC date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--full-refetch",
        action="store_true",
        help="Ignore state/checkpoint and fetch full history from unix timestamp 0",
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


def load_last_uts_if_valid(state_file: Path = STATE_FILE) -> int | None:
    if not state_file.exists():
        return None
    try:
        return int(state_file.read_text().strip())
    except ValueError:
        return None


def load_last_uts(state_file: Path = STATE_FILE) -> int:
    """Return the saved last-ingested Unix timestamp, defaulting to 0.

    **First-run / missing-state behaviour:** when the state file does not exist
    or contains an invalid value, this function returns ``0`` (Unix epoch).
    Callers (specifically ``main()``) treat epoch-0 as "start from the very
    beginning", which means a **full-history backfill** will be triggered on the
    first run on a clean machine.  If prior curated parquet data exists and no
    interrupted multi-page run is detected, ``main()`` narrows the start cursor
    to the latest curated timestamp instead.  Pass ``--since``/``--from-uts``
    or ``--full-refetch`` to take explicit control of the start point.
    """
    loaded = load_last_uts_if_valid(state_file=state_file)
    return loaded if loaded is not None else 0


def save_last_uts(value: int, state_file: Path = STATE_FILE) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(str(value))


def load_persisted_uts(state_file: Path = STATE_FILE) -> int | None:
    """Return the persisted last-scrobble UTS, or *None* when no state file exists yet.

    Unlike :func:`load_last_uts`, this never synthesises a fallback value, so
    callers can distinguish "never seen a scrobble" from "last scrobble was N
    days ago".
    """
    if state_file.exists():
        try:
            return int(state_file.read_text().strip())
        except ValueError:
            pass
    return None


def load_staleness_state(
    staleness_file: Path = STALENESS_STATE_FILE,
) -> dict[str, Any]:
    """Load the persisted staleness state dict.

    Returns ``{"stale": False, "stale_since": None}`` if the file is absent
    or unreadable.
    """
    if staleness_file.exists():
        try:
            with staleness_file.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (json.JSONDecodeError, OSError):
            pass
    return {"stale": False, "stale_since": None}


def save_staleness_state(
    state: dict[str, Any],
    staleness_file: Path = STALENESS_STATE_FILE,
) -> None:
    staleness_file.parent.mkdir(parents=True, exist_ok=True)
    with staleness_file.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, sort_keys=True)


def update_catalog_staleness(
    stale: bool,
    stale_since: str | None,
    catalog_file: Path = CATALOG_FILE,
) -> None:
    """Upsert ``stale`` and ``stale_since`` fields in the Last.fm YAML catalog.

    Operates on raw text so PyYAML is not required as a dependency.  Only
    top-level scalar lines that begin with ``stale:`` or ``stale_since:`` are
    touched; the rest of the file is preserved byte-for-byte.
    If the file does not exist the function returns silently.
    """
    if not catalog_file.exists():
        return

    stale_val = "true" if stale else "false"
    stale_since_val = f'"{stale_since}"' if stale_since else "null"

    lines = catalog_file.read_text(encoding="utf-8").splitlines(keepends=True)
    out: list[str] = []
    stale_written = False
    stale_since_written = False

    for line in lines:
        if line.startswith("stale_since:"):
            out.append(f"stale_since: {stale_since_val}\n")
            stale_since_written = True
        elif line.startswith("stale:"):
            out.append(f"stale: {stale_val}\n")
            stale_written = True
        else:
            out.append(line)

    # If neither field existed yet, insert them before the ``fields:`` block
    # (or at the very end of the file when no such block exists).
    if not stale_written or not stale_since_written:
        insert_idx = len(out)
        for i, line in enumerate(out):
            if line.startswith("fields:"):
                insert_idx = i
                break
        new_lines: list[str] = []
        if not stale_written:
            new_lines.append(f"stale: {stale_val}\n")
        if not stale_since_written:
            new_lines.append(f"stale_since: {stale_since_val}\n")
        out[insert_idx:insert_idx] = new_lines

    catalog_file.write_text("".join(out), encoding="utf-8")


def load_checkpoint(checkpoint_file: Path = CHECKPOINT_FILE) -> dict[str, Any] | None:
    if not checkpoint_file.exists():
        return None
    try:
        with checkpoint_file.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return loaded if isinstance(loaded, dict) else None


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


def is_retryable_status_code(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or 500 <= status_code <= 599


def backoff_delay_seconds(attempt: int, base_delay_seconds: int) -> int:
    return base_delay_seconds * (2**attempt)


def status_code_from_exception(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int):
            return status_code

    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code

    return None


def is_retryable_exception(exc: Exception) -> bool:
    status_code = status_code_from_exception(exc)
    if status_code is not None:
        return is_retryable_status_code(status_code)

    return isinstance(
        exc,
        (
            requests.Timeout,
            requests.ConnectionError,
            urllib.error.URLError,
            TimeoutError,
        ),
    )


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
        except Exception as exc:
            last_error = exc
            status_code = status_code_from_exception(exc)
            if not is_retryable_exception(exc):
                if status_code is None:
                    raise
                raise SystemExit(
                    f"Last.fm request failed with non-retryable HTTP {status_code}"
                ) from exc
            if attempt == max_retries - 1:
                break
            delay = backoff_delay_seconds(attempt, base_delay_seconds)
            if status_code is not None:
                print(
                    f"Transient HTTP {status_code} on page {page}. "
                    f"Retrying in {delay}s ({attempt + 1}/{max_retries})..."
                )
            else:
                print(
                    f"Transient network error on page {page}: {exc}. "
                    f"Retrying in {delay}s ({attempt + 1}/{max_retries})..."
                )
            time.sleep(delay)
            continue

        if is_retryable_status_code(response.status_code):
            last_error = RuntimeError(f"HTTP {response.status_code}")
            if attempt == max_retries - 1:
                break
            delay = backoff_delay_seconds(attempt, base_delay_seconds)
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


def detect_latest_curated_uts(curated_root: Path) -> int | None:
    """Scan all curated parquet files and return the maximum 'uts' value seen.

    Uses ``pyarrow.compute.max`` so only the 'uts' column is read into memory;
    the full row data is never materialised.  This keeps RAM bounded even when
    the curated history folder is large.
    """
    if not curated_root.exists():
        return None

    latest_uts: int | None = None
    for parquet_file in curated_root.rglob("*.parquet"):
        try:
            table = pq.read_table(parquet_file, columns=["uts"])
        except Exception as e:  # noqa: BLE001
            print(f"WARNING detect_latest_curated_uts: skipping corrupt parquet {parquet_file}: {e}")
            continue
        if table.num_rows == 0:
            continue
        column = table.column("uts")
        if column.null_count == column.length():
            continue
        file_max_scalar = pc.max(column)
        if file_max_scalar.is_valid:
            file_max = file_max_scalar.as_py()
            latest_uts = file_max if latest_uts is None else max(latest_uts, file_max)
    return latest_uts


CURATED_RUN_FILE_RE = re.compile(r"^scrobbles_(?P<run_id>\d+)_p(?P<page>\d+)$")


def has_paginated_curated_output(curated_root: Path) -> bool:
    if not curated_root.exists():
        return False

    for parquet_file in curated_root.rglob("scrobbles_*_p*.parquet"):
        match = CURATED_RUN_FILE_RE.match(parquet_file.stem)
        if not match:
            continue
        if int(match.group("page")) > 1:
            return True
    return False


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
    fallback_from_uts: int,
) -> tuple[int, int, int, int | None]:
    if args.from_uts is not None and args.since is not None:
        raise SystemExit("Use only one of --from or --since.")
    if args.full_refetch and (args.from_uts is not None or args.since is not None):
        raise SystemExit("--full-refetch cannot be combined with --from/--since.")
    if args.full_refetch:
        return 0, 1, int(time.time()), None

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

    # Keep the raw persisted value separately so staleness detection can
    # distinguish "first ever run" from "last scrobble was N days ago".
    persisted_uts = load_persisted_uts()
    checkpoint = None if args.no_resume else load_checkpoint()
    loaded_state_uts = load_last_uts_if_valid()
    # Intentional: when the state file is missing or corrupted, fall back to unix epoch 0
    # (full-history backfill) rather than an arbitrary recent window (e.g. now minus 30 days).
    # A missing/corrupted state file means we have no reliable cursor, so starting from 0
    # guarantees no historical gaps. The curated-scan block below narrows this down to an
    # incremental start when prior curated data already exists.
    state_from_uts = loaded_state_uts if loaded_state_uts is not None else 0
    has_explicit_start = args.full_refetch or args.from_uts is not None or args.since is not None
    # Performance gate: detect_latest_curated_uts() recursively scans every curated parquet
    # file to find the maximum 'uts' value. That scan is only useful when the state file is
    # absent or unusable (loaded_state_uts is None). Normal incremental runs — where the
    # state file is present and valid — must skip this block entirely so routine syncs stay
    # fast regardless of how large the curated history has grown.
    if loaded_state_uts is None and checkpoint is None and not has_explicit_start:
        if has_paginated_curated_output(curated_root=curated_root):
            # Paginated curated output exists but state file is absent — a prior history run
            # completed some pages but left no cursor. Resuming from the latest curated UTS
            # would silently skip the remaining pages of that interrupted run. Keep the
            # fallback at 0 so the full-history path handles deduplication safely instead.
            state_from_uts = 0
        else:
            latest_curated_uts = detect_latest_curated_uts(curated_root=curated_root)
            if latest_curated_uts is not None:
                # No multi-page interrupted run detected; curated data exists but state is
                # missing. Use the latest scrobble timestamp from curated parquet as the
                # incremental start to avoid a redundant full re-fetch.
                state_from_uts = latest_curated_uts

    from_uts, page, run_id, max_uts_seen = resolve_start(args, checkpoint, state_from_uts)
    seen_keys = load_seen_keys_for_run(curated_root=curated_root, run_id=run_id)
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

        page_rows = rows
        write_raw_page(raw_root=raw_root, run_id=run_id, page=page, rows=page_rows)
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

        # --- Staleness detection ---
        # Only fire when we have a real persisted timestamp (i.e. we have
        # ingested at least once before) and the gap exceeds the threshold.
        if persisted_uts is not None:
            gap_days = (int(time.time()) - persisted_uts) / 86400.0
            if gap_days >= STALE_THRESHOLD_DAYS:
                # Persist stale_since on first detection; keep it on subsequent runs.
                stale_state = load_staleness_state()
                if not stale_state.get("stale"):
                    today = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%d")
                    stale_state = {"stale": True, "stale_since": today}
                    save_staleness_state(stale_state)
                stale_since = stale_state["stale_since"]
                update_catalog_staleness(stale=True, stale_since=stale_since)
                last_date = dt.datetime.fromtimestamp(
                    persisted_uts, tz=dt.UTC
                ).strftime("%Y-%m-%d")
                gap_int = int(gap_days)
                print(
                    f"ERROR: No new Last.fm scrobbles detected for {gap_int} days "
                    f"(last scrobble: {last_date}, threshold: {STALE_THRESHOLD_DAYS}d). "
                    "Check that the scrobbling source is still connected at "
                    "https://www.last.fm/settings/applications",
                    file=sys.stderr,
                )
                raise SystemExit(1)
        return

    # Advance by 1 second so the next incremental run starts strictly after
    # the boundary scrobble, preventing it from being re-fetched and written
    # into a new Parquet file (which would silently accumulate duplicates).
    save_last_uts(max_uts_seen + 1)
    # Only clear prior staleness when max_uts_seen is strictly newer than the
    # last persisted timestamp. An ad-hoc replay (--from/--since earlier than
    # the saved timestamp) may return rows whose max_uts_seen <= persisted_uts,
    # which does not prove that scrobbling has resumed.
    stale_state = load_staleness_state()
    if stale_state.get("stale") and (persisted_uts is None or max_uts_seen > persisted_uts):
        save_staleness_state({"stale": False, "stale_since": None})
        update_catalog_staleness(stale=False, stale_since=None)
    clear_checkpoint()
    print(
        f"Ingest complete: pages={pages_processed}, parquet_rows={rows_written}, "
        f"last_uts={max_uts_seen} (saved as {max_uts_seen + 1})"
    )


if __name__ == "__main__":
    main()
