import os, json, time
from pathlib import Path
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

API = "https://ws.audioscrobbler.com/2.0/"

USER = os.environ["LASTFM_USER"]
KEY = os.environ["LASTFM_API_KEY"]

RAW_ROOT = Path(os.environ.get(
    "DATALAKE_RAW_ROOT",
    "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports"
))
CURATED_ROOT = Path(os.environ.get(
    "DATALAKE_CURATED_ROOT",
    "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated"
))

RAW = RAW_ROOT / "lastfm"
CURATED = CURATED_ROOT / "lastfm" / "scrobbles"

STATE_DIR = Path.home() / ".local" / "share" / "datalake"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "lastfm_last_uts.txt"

def load_last_uts() -> int:
    if STATE_FILE.exists():
        return int(STATE_FILE.read_text().strip())
    return int(time.time()) - 30 * 24 * 3600  # bootstrap 30d

def save_last_uts(v: int) -> None:
    STATE_FILE.write_text(str(v))

def fetch_page(from_uts: int, page: int) -> dict:
    params = {
        "method": "user.getRecentTracks",
        "user": USER,
        "api_key": KEY,
        "format": "json",
        "from": from_uts,
        "limit": 200,
        "page": page,
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize(items):
    out = []
    for it in items:
        if "@attr" in it and it["@attr"].get("nowplaying") == "true":
            continue
        uts = int(it["date"]["uts"])
        out.append({
            "uts": uts,
            "played_at_utc": pd.to_datetime(uts, unit="s", utc=True),
            "artist": it["artist"]["#text"],
            "track": it["name"],
            "album": it.get("album", {}).get("#text") or None,
            "mbid_track": it.get("mbid") or None,
            "source": "lastfm",
        })
    return out

def write_raw_jsonl(rows):
    RAW.mkdir(parents=True, exist_ok=True)
    p = RAW / f"recent_{int(time.time())}.jsonl"
    with p.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")

def append_parquet_partitions(df: pd.DataFrame):
    if df.empty:
        return
    df["year"] = df["played_at_utc"].dt.year.astype(int)
    df["month"] = df["played_at_utc"].dt.month.astype(int)

    for (y, m), g in df.groupby(["year", "month"]):
        part_dir = CURATED / f"year={y:04d}" / f"month={m:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_file = part_dir / f"scrobbles_{int(time.time())}.parquet"
        table = pa.Table.from_pandas(g.drop(columns=["year", "month"])), preserve_index=False)
        pq.write_table(table, out_file)

def main():
    from_uts = load_last_uts()
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

    write_raw_jsonl(all_rows)

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["uts", "artist", "track", "album"])
    append_parquet_partitions(df)

    save_last_uts(int(df["uts"].max()))

if __name__ == "__main__":
    main()