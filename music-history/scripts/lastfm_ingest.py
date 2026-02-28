#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
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
RAW_PAGE_FILE_PATTERN = re.compile(
    r"^scrobbles_run-(?P<run_id>\d+)_from-(?P<from_uts>full|\d+)_p(?P<page>\d{4})(?:_a(?P<attempt>\d{4}))?\.jsonl$"
)
RAW_RUN_MARKER_PATTERN = re.compile(
    r"^scrobbles_run-(?P<run_id>\d+)_from-(?P<from_uts>full|\d+)_(?P<kind>started|completed)\.json$"
)

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
    parsed = (
        parsed.replace(tzinfo=dt.UTC)
        if parsed.tzinfo is None
        else parsed.astimezone(dt.UTC)
    )
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
    try:
        with checkpoint_file.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError):
        return None


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


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    played_at = out.get("played_at_utc")
    if isinstance(played_at, pd.Timestamp):
        out["played_at_utc"] = played_at.isoformat()
    return out


def _from_uts_token(from_uts: int | None) -> str:
    return "full" if from_uts is None else str(int(from_uts))


def raw_page_file_for_run(raw_root: Path, run_id: int, from_uts: int | None, page: int) -> Path:
    from_token = "full" if from_uts is None else str(int(from_uts))
    return raw_root / f"scrobbles_run-{run_id}_from-{from_token}_p{page:04d}.jsonl"


def _raw_page_file_candidate_for_attempt(
    raw_root: Path,
    run_id: int,
    from_uts: int | None,
    page: int,
    attempt: int,
) -> Path:
    if attempt == 0:
        return raw_page_file_for_run(raw_root=raw_root, run_id=run_id, from_uts=from_uts, page=page)
    from_token = _from_uts_token(from_uts)
    return raw_root / f"scrobbles_run-{run_id}_from-{from_token}_p{page:04d}_a{attempt:04d}.jsonl"


def _parquet_file_candidate_for_attempt(
    part_dir: Path,
    run_id: int,
    page: int,
    attempt: int,
) -> Path:
    if attempt == 0:
        return part_dir / f"scrobbles_{run_id}_p{page:04d}.parquet"
    return part_dir / f"scrobbles_{run_id}_p{page:04d}_a{attempt:04d}.parquet"


def run_marker_file_for_run(raw_root: Path, run_id: int, from_uts: int | None, kind: str) -> Path:
    from_token = _from_uts_token(from_uts)
    return raw_root / f"scrobbles_run-{run_id}_from-{from_token}_{kind}.json"


def write_run_marker(
    raw_root: Path,
    run_id: int,
    from_uts: int | None,
    kind: str,
    payload: dict[str, Any],
) -> None:
    raw_root.mkdir(parents=True, exist_ok=True)
    marker_path = run_marker_file_for_run(
        raw_root=raw_root,
        run_id=run_id,
        from_uts=from_uts,
        kind=kind,
    )
    if marker_path.exists():
        return
    with marker_path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")


def append_raw_page_jsonl(
    rows: list[dict[str, Any]],
    raw_root: Path,
    run_id: int,
    from_uts: int | None,
    page: int,
) -> int:
    if not rows:
        return 0

    raw_root.mkdir(parents=True, exist_ok=True)
    temp_file = raw_root / f".scrobbles_tmp_{run_id}_{page}_{os.getpid()}_{int(time.time() * 1000)}.jsonl"

    with temp_file.open("x", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_serialize_row(row), ensure_ascii=False))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    attempt = 0
    while True:
        raw_page_file = _raw_page_file_candidate_for_attempt(
            raw_root=raw_root,
            run_id=run_id,
            from_uts=from_uts,
            page=page,
            attempt=attempt,
        )
        try:
            # Publish without overwrite. If a path already exists, write a new attempt file.
            os.link(temp_file, raw_page_file)
            break
        except FileExistsError:
            attempt += 1

    temp_file.unlink(missing_ok=True)

    return 1


def _state_next_from_uts(state_file: Path) -> int | None:
    state_last = load_last_uts(state_file)
    if state_last is None:
        return None
    return state_last + 1


def _list_pages_for_from_uts(raw_root: Path, from_uts: int | None) -> list[int]:
    from_token = _from_uts_token(from_uts)
    pages: list[int] = []
    for path in raw_root.glob(f"scrobbles_run-*_from-{from_token}_p*.jsonl"):
        match = RAW_PAGE_FILE_PATTERN.match(path.name)
        if not match:
            continue
        has_valid_uts = False
        for row in iter_jsonl(path):
            if extract_uts(row) is not None:
                has_valid_uts = True
                break
        if not has_valid_uts:
            continue
        pages.append(int(match.group("page")))
    return pages


def _next_missing_page(pages: list[int]) -> int:
    if not pages:
        return 1
    existing = set(pages)
    candidate = 1
    while candidate in existing:
        candidate += 1
    return candidate


def _max_uts_for_from_uts(raw_root: Path, from_uts: int | None) -> int | None:
    from_token = _from_uts_token(from_uts)
    latest: int | None = None
    for path in raw_root.glob(f"scrobbles_run-*_from-{from_token}_p*.jsonl"):
        if not RAW_PAGE_FILE_PATTERN.match(path.name):
            continue
        for row in iter_jsonl(path):
            uts = extract_uts(row)
            if uts is None:
                continue
            latest = uts if latest is None else max(latest, uts)
    return latest


def _list_run_ids_for_from_uts(raw_root: Path, from_uts: int | None) -> list[int]:
    from_token = _from_uts_token(from_uts)
    run_ids: set[int] = set()
    for path in raw_root.glob(f"scrobbles_run-*_from-{from_token}_p*.jsonl"):
        match = RAW_PAGE_FILE_PATTERN.match(path.name)
        if not match:
            continue
        has_valid_uts = False
        for row in iter_jsonl(path):
            if extract_uts(row) is not None:
                has_valid_uts = True
                break
        if not has_valid_uts:
            continue
        run_ids.add(int(match.group("run_id")))
    return sorted(run_ids)


def _latest_run_id_with_pages_for_from_uts(
    raw_root: Path,
    from_uts: int | None,
    eligible_run_ids: set[int] | None = None,
) -> int | None:
    from_token = _from_uts_token(from_uts)
    latest: tuple[float, int] | None = None

    for path in raw_root.glob(f"scrobbles_run-*_from-{from_token}_p*.jsonl"):
        match = RAW_PAGE_FILE_PATTERN.match(path.name)
        if not match:
            continue

        has_valid_uts = False
        for row in iter_jsonl(path):
            if extract_uts(row) is not None:
                has_valid_uts = True
                break
        if not has_valid_uts:
            continue

        run_id = int(match.group("run_id"))
        if eligible_run_ids is not None and run_id not in eligible_run_ids:
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue

        if latest is None or mtime > latest[0] or (mtime == latest[0] and run_id > latest[1]):
            latest = (mtime, run_id)

    return None if latest is None else latest[1]


def _list_incomplete_marked_runs(raw_root: Path) -> list[int | None]:
    if not raw_root.exists():
        return []

    started: set[tuple[int, int | None]] = set()
    completed: set[tuple[int, int | None]] = set()

    for path in raw_root.glob("scrobbles_run-*_from-*_*.json"):
        match = RAW_RUN_MARKER_PATTERN.match(path.name)
        if not match:
            continue
        run_id = int(match.group("run_id"))
        from_token = match.group("from_uts")
        from_uts = None if from_token == "full" else int(from_token)
        kind = match.group("kind")
        key = (run_id, from_uts)
        if kind == "started":
            started.add(key)
        elif kind == "completed":
            completed.add(key)

    incomplete_from_uts = [from_uts for run_id, from_uts in started if (run_id, from_uts) not in completed]
    return incomplete_from_uts


def infer_resume_from_raw(
    raw_root: Path,
) -> tuple[int | None, int, int, int | None] | None:
    if not raw_root.exists():
        return None

    started: list[tuple[int, int | None]] = []
    completed: set[tuple[int, int | None]] = set()
    page_runs: set[tuple[int, int | None]] = set()
    for path in raw_root.glob("scrobbles_run-*_from-*_*.json"):
        match = RAW_RUN_MARKER_PATTERN.match(path.name)
        if not match:
            continue
        run_id = int(match.group("run_id"))
        from_token = match.group("from_uts")
        from_uts = None if from_token == "full" else int(from_token)
        key = (run_id, from_uts)
        if match.group("kind") == "started":
            started.append(key)
        else:
            completed.add(key)

    for path in raw_root.glob("scrobbles_run-*_from-*_p*.jsonl"):
        match = RAW_PAGE_FILE_PATTERN.match(path.name)
        if not match:
            continue
        run_id = int(match.group("run_id"))
        from_token = match.group("from_uts")
        from_uts = None if from_token == "full" else int(from_token)
        page_runs.add((run_id, from_uts))

    incomplete_runs = [key for key in started if key not in completed]
    legacy_unmarked_runs = [key for key in page_runs if key not in completed and key not in started]

    # Always include legacy unmarked page runs, even when started markers exist.
    # This avoids skipping historical backfills if an interrupted full run exists
    # without marker files alongside other incomplete runs.
    candidate_pool = list({*incomplete_runs, *legacy_unmarked_runs})

    if not candidate_pool:
        return None

    # Pick the safest historical lower bound to avoid skipping unfinished backfills.
    from_candidates = {from_uts for _run_id, from_uts in candidate_pool}
    if None in from_candidates:
        from_uts = None
    else:
        from_uts = min(int(value) for value in from_candidates if value is not None)

    matching_runs = [key for key in candidate_pool if key[1] == from_uts]
    eligible_run_ids = {run_id for run_id, _from_uts in matching_runs}
    latest_run_id = _latest_run_id_with_pages_for_from_uts(
        raw_root=raw_root,
        from_uts=from_uts,
        eligible_run_ids=eligible_run_ids,
    )
    if latest_run_id is not None:
        run_id = latest_run_id
    else:
        run_id = max(matching_runs, key=lambda key: key[0])[0]

    # Compute the missing page using all raw files for the chosen from_uts range,
    # not just one run_id. This avoids max(page)+1 style skips after interrupted
    # no-checkpoint restarts that created multiple run_ids for the same backfill.
    pages = _list_pages_for_from_uts(raw_root=raw_root, from_uts=from_uts)
    next_page = _next_missing_page(pages)
    max_uts_seen = _max_uts_for_from_uts(raw_root=raw_root, from_uts=from_uts)
    return from_uts, next_page, run_id, max_uts_seen


def _safe_restart_from_uts(from_uts_values: list[int | None]) -> int | None:
    if not from_uts_values:
        return None
    if any(value is None for value in from_uts_values):
        return None
    return min(int(value) for value in from_uts_values if value is not None)


def _resume_is_safer(
    candidate: tuple[int | None, int, int, int | None],
    baseline: tuple[int | None, int, int, int | None],
) -> bool:
    candidate_from_uts, candidate_next_page, _candidate_run_id, _candidate_max_uts_seen = candidate
    baseline_from_uts, baseline_next_page, _baseline_run_id, _baseline_max_uts_seen = baseline

    if candidate_from_uts is None and baseline_from_uts is not None:
        return True
    if candidate_from_uts is not None and baseline_from_uts is None:
        return False
    if candidate_from_uts is not None and baseline_from_uts is not None:
        if candidate_from_uts < baseline_from_uts:
            return True
        if candidate_from_uts > baseline_from_uts:
            return False

    # Same from_uts: earlier missing page is safer than a later checkpoint page.
    return candidate_next_page < baseline_next_page


def determine_from_uts(raw_root: Path, state_file: Path = STATE_FILE) -> int | None:
    incomplete_marked_runs = _list_incomplete_marked_runs(raw_root=raw_root)
    if incomplete_marked_runs:
        return _safe_restart_from_uts(incomplete_marked_runs)

    state_next = _state_next_from_uts(state_file=state_file)
    inferred_resume = infer_resume_from_raw(raw_root=raw_root)
    inferred_from_uts = None if inferred_resume is None else inferred_resume[0]

    if state_next is None:
        return inferred_from_uts

    if inferred_resume is None:
        return state_next

    if inferred_from_uts is None:
        return None

    return min(state_next, inferred_from_uts)


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
        table = pa.Table.from_pandas(group.drop(columns=["year", "month"]), preserve_index=False)
        attempt = 0
        while True:
            out_file = _parquet_file_candidate_for_attempt(
                part_dir=part_dir,
                run_id=run_id,
                page=page,
                attempt=attempt,
            )
            try:
                # Keep curated page partitions immutable, mirroring raw page dump behavior.
                with out_file.open("xb") as handle:
                    pq.write_table(table, handle)
                break
            except FileExistsError:
                attempt += 1
        written += len(group)
    return written


def resolve_start(
    args: argparse.Namespace,
    checkpoint: dict[str, Any] | None,
    fallback_from_uts: int | None,
    raw_root: Path,
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
            checkpoint_from_uts = (
                int(checkpoint["from_uts"]) if checkpoint.get("from_uts") is not None else None
            )
            checkpoint_next_page = int(checkpoint.get("next_page", 1))
            checkpoint_run_id = int(checkpoint.get("run_id", int(time.time())))
            checkpoint_max_uts_seen = checkpoint.get("max_uts_seen")
            if checkpoint_max_uts_seen is not None:
                checkpoint_max_uts_seen = int(checkpoint_max_uts_seen)

            pages_for_from_uts = _list_pages_for_from_uts(
                raw_root=raw_root,
                from_uts=checkpoint_from_uts,
            )
            if pages_for_from_uts:
                # Always resume from the first missing page to avoid max(page)+1 skips.
                safe_next_page = _next_missing_page(pages_for_from_uts)
                checkpoint_next_page = max(1, min(checkpoint_next_page, safe_next_page))
                run_ids_for_from_uts = _list_run_ids_for_from_uts(
                    raw_root=raw_root,
                    from_uts=checkpoint_from_uts,
                )
                if run_ids_for_from_uts and checkpoint_run_id not in run_ids_for_from_uts:
                    latest_run_id = _latest_run_id_with_pages_for_from_uts(
                        raw_root=raw_root,
                        from_uts=checkpoint_from_uts,
                    )
                    if latest_run_id is not None:
                        checkpoint_run_id = latest_run_id
                if checkpoint_max_uts_seen is None:
                    checkpoint_max_uts_seen = _max_uts_for_from_uts(
                        raw_root=raw_root,
                        from_uts=checkpoint_from_uts,
                    )
                checkpoint_resume = (
                    checkpoint_from_uts,
                    checkpoint_next_page,
                    checkpoint_run_id,
                    checkpoint_max_uts_seen,
                )
                inferred_resume = infer_resume_from_raw(raw_root=raw_root)
                if inferred_resume is not None and _resume_is_safer(
                    candidate=inferred_resume,
                    baseline=checkpoint_resume,
                ):
                    return inferred_resume
                return checkpoint_resume

            # A checkpoint without matching raw pages is likely stale/corrupt.
            # Prefer raw-page inference so unfinished backfills are not skipped.
            inferred_resume = infer_resume_from_raw(raw_root=raw_root)
            if inferred_resume is not None:
                return inferred_resume

            # If no raw evidence supports this checkpoint range, do not trust it.
            # Falling back to it can skip historical pages after interrupted runs.
            return fallback_from_uts, 1, int(time.time()), None
        except (TypeError, ValueError, KeyError):
            pass

    if not args.no_resume:
        inferred_resume = infer_resume_from_raw(raw_root=raw_root)
        if inferred_resume is not None:
            return inferred_resume

    return fallback_from_uts, 1, int(time.time()), None


def main() -> None:
    args = parse_args()
    user = resolve_user(explicit_user=args.user, default_user=os.getenv("LASTFM_USER_DEFAULT"))
    api_key = load_env("LASTFM_API_KEY")
    raw_root = Path(os.getenv("DATALAKE_RAW_ROOT", str(DEFAULT_RAW_ROOT))) / "lastfm"
    curated_root = Path(os.getenv("DATALAKE_CURATED_ROOT", str(DEFAULT_CURATED_ROOT))) / "lastfm" / "scrobbles"

    state_from_uts = determine_from_uts(raw_root=raw_root)
    checkpoint = None if args.no_resume else load_checkpoint()
    from_uts, page, run_id, max_uts_seen = resolve_start(
        args=args,
        checkpoint=checkpoint,
        fallback_from_uts=state_from_uts,
        raw_root=raw_root,
    )
    seen_keys = load_seen_keys_for_run(curated_root=curated_root, run_id=run_id)
    write_run_marker(
        raw_root=raw_root,
        run_id=run_id,
        from_uts=from_uts,
        kind="started",
        payload={
            "from_uts": from_uts,
            "run_id": run_id,
            "started_at": int(time.time()),
            "user": user,
        },
    )
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
        raw_files_updated += append_raw_page_jsonl(
            rows=page_rows,
            raw_root=raw_root,
            run_id=run_id,
            from_uts=from_uts,
            page=page,
        )
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
        write_run_marker(
            raw_root=raw_root,
            run_id=run_id,
            from_uts=from_uts,
            kind="completed",
            payload={
                "completed_at": int(time.time()),
                "from_uts": from_uts,
                "last_uts": None,
                "pages_processed": pages_processed,
                "parquet_rows": rows_written,
                "raw_page_files": raw_files_updated,
                "run_id": run_id,
                "user": user,
            },
        )
        clear_checkpoint()
        print("No new rows found.")
        return

    save_last_uts(max_uts_seen)
    write_run_marker(
        raw_root=raw_root,
        run_id=run_id,
        from_uts=from_uts,
        kind="completed",
        payload={
            "completed_at": int(time.time()),
            "from_uts": from_uts,
            "last_uts": max_uts_seen,
            "pages_processed": pages_processed,
            "parquet_rows": rows_written,
            "raw_page_files": raw_files_updated,
            "run_id": run_id,
            "user": user,
        },
    )
    clear_checkpoint()
    print(
        f"Ingest complete: pages={pages_processed}, parquet_rows={rows_written}, raw_page_files={raw_files_updated}, "
        f"last_uts={max_uts_seen}"
    )


if __name__ == "__main__":
    main()
