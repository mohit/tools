# Music History Ingestion

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
    *   `DATALAKE_CURATED_ROOT`: (Optional) The root directory for curated data. Defaults to `/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated`.

    Example (add to your `.bashrc`, `.zshrc`, or similar):
    ```bash
    export LASTFM_USER="your_username"
    export LASTFM_API_KEY="your_api_key"
    export DATALAKE_RAW_ROOT="/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports"
    export DATALAKE_CURATED_ROOT="/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated"
    ```

2.  **Create Directories:**
    Ensure the necessary directories exist. You can use the following commands:
    ```bash
    ICLOUD="/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs"
    RAW_ROOT="$ICLOUD/Data Exports"
    DATALAKE_ROOT="$ICLOUD/Data Exports/datalake"
    CURATED_ROOT="$DATALAKE_ROOT/curated"
    CATALOG_ROOT="$DATALAKE_ROOT/catalog"
    CODE_ROOT="/Users/mohit/Documents/code/datalake.me" # This project's root

    mkdir -p "$CURATED_ROOT/lastfm/scrobbles" \
             "$RAW_ROOT/lastfm" \
             "$CATALOG_ROOT"
    ```

3.  **Install Dependencies:**
    This project uses `uv` for dependency management. Run the following command to install dependencies:
    ```bash
    uv sync
    ```

### Last.fm incremental pull

```bash
python scripts/lastfm_ingest.py
```

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

```sql
SELECT artist, COUNT(*) plays
FROM read_parquet('/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated/lastfm/scrobbles/year=*/month=*/*.parquet')
WHERE played_at_utc >= now() - INTERVAL 30 DAY
GROUP BY artist
ORDER BY plays DESC
LIMIT 50;
```
