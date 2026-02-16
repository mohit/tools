# Music History Ingestion

This project ingests Last.fm scrobble data.
=======
This tool ingests music listening data (Last.fm + Apple Music), processes it, and stores it in parquet.


## Setup

1. Environment variables:
- `LASTFM_USER`: Last.fm username.
- `LASTFM_API_KEY`: Last.fm API key.
- `DATALAKE_RAW_ROOT` (optional): defaults to `/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports`.
- `DATALAKE_CURATED_ROOT` (optional): defaults to `/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated`.
- `APPLE_MUSIC_DEVELOPER_TOKEN` (optional): MusicKit developer token for recent-played snapshots.
- `APPLE_MUSIC_USER_TOKEN` (optional): MusicKit user token for recent-played snapshots.

2. Install dependencies:

```bash
uv sync
```

## Last.fm usage

=======
This project ingests music listening history and writes analytics-friendly Parquet data.
It currently includes:

- Last.fm scrobble ingestion via API.
- Apple Music library/play history export + ingestion.

## Setup

1.  **Environment Variables (Last.fm API workflow):**
    Set the following environment variables:
    *   `LASTFM_USER`: Your Last.fm username.
    *   `LASTFM_API_KEY`: Your Last.fm API key. You can get one [here](https://www.last.fm/api/account/create).
    *   `DATALAKE_RAW_ROOT`: (Optional) The root directory for raw data. Defaults to `/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports`.
    *   `DATALAKE_CURATED_ROOT`: (Optional) Kept for backward compatibility with older scripts.

    Example (add to your `.bashrc`, `.zshrc`, or similar):
    ```bash
    export LASTFM_USER="your_username"
    export LASTFM_API_KEY="your_api_key"
    export DATALAKE_RAW_ROOT="/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports"
    ```

2.  **Create Directories:**
    Ensure the necessary directories exist:
    ```bash
    ICLOUD="/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs"
    RAW_ROOT="$ICLOUD/Data Exports"

    mkdir -p "$RAW_ROOT/lastfm/scrobbles"
    ```

3.  **Install Dependencies:**
    This project uses `uv` for dependency management.
    ```bash
    uv sync
    ```

### Last.fm incremental pull

Use `scripts/lastfm_ingest.py` to fetch Last.fm scrobbles and write month-partitioned JSONL files:

```bash
python scripts/lastfm_ingest.py
```

This writes files like:

```text
$DATALAKE_RAW_ROOT/lastfm/scrobbles/year=2026/month=02/scrobbles.jsonl
```

### Incremental mode (`--since`)

For daily runs, use incremental mode so the script only fetches new scrobbles:

```bash
# Auto-detect latest uts from existing JSONL and fetch only newer scrobbles
python scripts/lastfm_ingest.py --since
```

You can also pass a specific boundary:

```bash
# Unix timestamp
python scripts/lastfm_ingest.py --since 1738713600
=======
```bash
python scripts/lastfm_ingest.py
```

## Apple Music Privacy Export Freshness Check

If you're ingesting Apple Music privacy export data from:
`~/datalake.me/raw/apple-music/Apple Music - Track Play History.csv`,
run the freshness validator before ingestion:

```bash
python check_apple_music_privacy_export.py
```

Useful flags:

```bash
python check_apple_music_privacy_export.py --csv-path "~/datalake.me/raw/apple-music/Apple Music - Track Play History.csv" --max-age-days 45
```

## Apple Music Export Freshness Guard

Apple Music ingestion now checks `apple_music_export_metadata.json` before loading data.

Run the check manually:
```bash
python check_apple_music_export.py
```

Current tracked state (issue #36):
- `latest_play_date`: `2023-11-09`
- `last_export_date`: `2023-11-10`
- detected stale on `2026-02-14`

If the check fails, request a new Apple Music export from `privacy.apple.com`, replace the raw data file, then update `apple_music_export_metadata.json` with the new `last_export_date` and `latest_play_date`.

## Example Query
=======
Incremental mode is the default. If no arguments are provided, the script starts from the latest known timestamp (state file and curated parquet max `uts`) and only fetches new scrobbles.

Optional flags:

```bash
# Start from a specific unix timestamp
python scripts/lastfm_ingest.py --from-uts 1735689600

# Start from an ISO timestamp (UTC)
python scripts/lastfm_ingest.py --since 2026-01-01T00:00:00Z

# Force full historical re-fetch
python scripts/lastfm_ingest.py --full-refetch
```

### Apple Music local export from Music.app

Export Apple Music listening/library metadata into JSONL:
```bash
python export_apple_music.py
```

Ingest that JSONL into curated Parquet and refresh the DuckDB view:
```bash
python ingest_apple_music.py
```

### Apple Music privacy export freshness reminder (Issue #15)

The Apple Music dataset in the data lake became stale and requires a new privacy export request.

Known stale snapshot:
- Location: `~/datalake.me/raw/apple-music/`
- Rows: `88,949`
- Data range: `2015-07-02` to `2023-11-09`
- Latest play date: `2023-11-05`
- Source export date: `~2023-11-10`
- Last ingested: `2026-02-07`
- Apple Health status: healthy (`latest=2026-01-20`, 19 days old as of 2026-02-08)

Run this reminder check:
```bash
python remind_apple_music_reexport.py
```

## Apple Music usage

MusicKit does not provide full historical listening history. Use the hybrid flow:
- Quarterly: privacy.apple.com export (full history)
- Daily/optional: MusicKit recent played snapshot (max ~50 tracks)

### 0. One-command sync (recommended for automation)

```bash
python apple_music_sync.py --json
```

Behavior:
- Processes latest Play Activity CSV into curated parquet when available.
- Returns freshness exit codes (`0` fresh, `1` warning, `2` critical, `3` missing CSV).
- Automatically runs MusicKit supplemental sync when token env vars are set.

### 1. Manual export helper (privacy.apple.com)

Open privacy portal and request/export data:

```bash
python apple_music_export_helper.py --open-browser
```

After Apple sends the zip, extract `Play Activity` CSV into raw folder:

```bash
python apple_music_export_helper.py --extract
# or specify zip
python apple_music_export_helper.py --extract --zip-file ~/Downloads/privacy-export.zip
```

Default raw destination:
- `$DATALAKE_RAW_ROOT/apple-music/<YYYYMMDD>/...Play Activity...csv`
- If `DATALAKE_RAW_ROOT` is unset, defaults to `/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/apple-music/...`

### 2. Process CSV to curated parquet

```bash
python apple_music_processor.py
```

Optional explicit paths:

```bash
python apple_music_processor.py \
  --csv-file "$DATALAKE_RAW_ROOT/apple-music/20260210/Apple_Music_Play_Activity.csv" \
  --curated-root "$DATALAKE_CURATED_ROOT/apple-music/play-activity"
```

### 3. Freshness monitoring

```bash
python apple_music_monitor.py --json
```

Exit codes:
- `0`: fresh
- `1`: warning (default >= 30 days stale)
- `2`: critical (default >= 90 days stale)
- `3`: missing CSV (file absent/renamed)

### 4. Optional MusicKit supplement (recent played only)

```bash
python apple_music_musickit_sync.py
```

Or pass tokens directly:

```bash
python apple_music_musickit_sync.py \
  --developer-token "$APPLE_MUSIC_DEVELOPER_TOKEN" \
  --user-token "$APPLE_MUSIC_USER_TOKEN"
```

This stores a raw JSON snapshot plus curated parquet at:
- `$DATALAKE_RAW_ROOT/apple-music/musickit/`
- `$DATALAKE_CURATED_ROOT/apple-music/recent-played/`

If stale, request a new Apple data export at `https://privacy.apple.com/`.
Apple usually takes a few days before the export is ready.

## Example query

# ISO datetime (UTC if timezone omitted)
python scripts/lastfm_ingest.py --since 2025-02-05T00:00:00Z
```

Incremental mode behavior:
- Scans existing `*.jsonl` under output dir to find latest scrobble timestamp.
- Passes Last.fm API `from` parameter to limit fetched history.
- Deduplicates and appends only new rows into month partitions.
