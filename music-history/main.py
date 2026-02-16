import json
import os
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

API = "https://ws.audioscrobbler.com/2.0/"

RAW_ROOT = Path(
    os.environ.get(
        "DATALAKE_RAW_ROOT",
        "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports",
    )
)
CURATED_ROOT = Path(
    os.environ.get(
        "DATALAKE_CURATED_ROOT",
        "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated",
    )
)

RAW = RAW_ROOT / "lastfm"
CURATED = CURATED_ROOT / "lastfm" / "scrobbles"

STATE_DIR = Path.home() / ".local" / "share" / "datalake"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "lastfm_last_uts.txt"


def get_credentials() -> tuple[str, str]:
    user = os.environ.get("LASTFM_USER")
    key = os.environ.get("LASTFM_API_KEY")
    if not user or not key:
        raise RuntimeError("Set LASTFM_USER and LASTFM_API_KEY before running ingestion.")
    return user, key


def _normalize_text(value) -> str:
    if isinstance(value, dict):
        value = value.get("#text")
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def row_key(row: dict) -> tuple:
    return (
        int(row["uts"]),
        _normalize_text(row.get("artist")),
        _normalize_text(row.get("track") or row.get("name")),
        _normalize_text(row.get("album")),
    )


def load_last_uts_from_state() -> int | None:
    if not STATE_FILE.exists():
        return None
    try:
        return int(STATE_FILE.read_text().strip())
    except (TypeError, ValueError):
        return None


def save_last_uts(v: int) -> None:
    STATE_FILE.write_text(str(v))


def extract_uts(row: dict) -> int | None:
    uts = row.get("uts")
    if uts is not None:
        try:
            return int(uts)
        except (TypeError, ValueError):
            return None

    # Support raw Last.fm API-shaped rows if present in older files.
    date = row.get("date") or {}
    nested_uts = date.get("uts")
    if nested_uts is not None:
        try:
            return int(nested_uts)
        except (TypeError, ValueError):
            return None

    return None


def iter_jsonl(path: Path):
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def load_last_uts_from_raw(raw_dir: Path = RAW) -> int | None:
    if not raw_dir.exists():
        return None

    latest = None
    for path in raw_dir.glob("*.jsonl"):
        for row in iter_jsonl(path):
            uts = extract_uts(row)
            if uts is None:
                continue
            if latest is None or uts > latest:
                latest = uts
    return latest


def determine_from_uts(raw_dir: Path = RAW) -> int | None:
    raw_last = load_last_uts_from_raw(raw_dir)
    if raw_last is not None:
        return raw_last + 1

    state_last = load_last_uts_from_state()
    if state_last is not None:
        return state_last + 1

    # No history found: fetch full history.
    return None


def fetch_page(from_uts: int | None, page: int) -> dict:
    user, key = get_credentials()
    params = {
        "method": "user.getRecentTracks",
        "user": user,
        "api_key": key,
        "format": "json",
        "limit": 200,
        "page": page,
    }
    if from_uts is not None:
        params["from"] = from_uts

    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize(items):
    out = []
    for it in items:
        if "@attr" in it and it["@attr"].get("nowplaying") == "true":
            continue
        uts = int(it["date"]["uts"])
        out.append(
            {
                "uts": uts,
                "played_at_utc": pd.to_datetime(uts, unit="s", utc=True),
                "artist": it["artist"]["#text"],
                "track": it["name"],
                "album": it.get("album", {}).get("#text") or None,
                "mbid_track": it.get("mbid") or None,
                "source": "lastfm",
            }
        )
    return out


def month_file_for_uts(raw_dir: Path, uts: int) -> Path:
    dt = pd.to_datetime(uts, unit="s", utc=True)
    return raw_dir / f"scrobbles_{dt.year:04d}-{dt.month:02d}.jsonl"


def _serialize_row(row: dict) -> dict:
    out = dict(row)
    played_at = out.get("played_at_utc")
    if isinstance(played_at, pd.Timestamp):
        out["played_at_utc"] = played_at.isoformat()
    return out


def merge_raw_monthly_jsonl(rows: list[dict], raw_dir: Path = RAW) -> None:
    if not rows:
        return

    raw_dir.mkdir(parents=True, exist_ok=True)

    by_month: dict[Path, list[dict]] = defaultdict(list)
    for row in rows:
        by_month[month_file_for_uts(raw_dir, int(row["uts"]))].append(_serialize_row(row))

    for month_file, month_rows in by_month.items():
        merged: dict[tuple, dict] = {}

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

        sorted_rows = sorted(merged.values(), key=lambda r: int(r.get("uts", 0)))

        tmp_path = month_file.with_suffix(".jsonl.tmp")
        with tmp_path.open("w") as f:
            for row in sorted_rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_path.replace(month_file)


def append_parquet_partitions(df: pd.DataFrame):
    if df.empty:
        return
    df["year"] = df["played_at_utc"].dt.year.astype(int)
    df["month"] = df["played_at_utc"].dt.month.astype(int)

    for (y, m), g in df.groupby(["year", "month"]):
        part_dir = CURATED / f"year={y:04d}" / f"month={m:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / f"scrobbles_{int(time.time())}.parquet"
        table = pa.Table.from_pandas(g.drop(columns=["year", "month"]), preserve_index=False)
        pq.write_table(table, out_file)


def main():
    from_uts = determine_from_uts()
    all_rows = []
    page = 1

    while True:
        payload = fetch_page(from_uts, page)
        recent = payload.get("recenttracks", {})
        tracks = recent.get("track", [])
        rows = normalize(tracks)
        if not rows:
            break
        all_rows.extend(rows)

        attr = recent.get("@attr", {})
        total_pages = int(attr.get("totalPages", "1"))
        if page >= total_pages:
            break
        page += 1

    if not all_rows:
        return

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["uts", "artist", "track", "album"])

    merge_raw_monthly_jsonl(df.to_dict(orient="records"))
    append_parquet_partitions(df)

    save_last_uts(int(df["uts"].max()))


if __name__ == "__main__":
    main()
