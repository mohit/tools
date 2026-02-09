# Music History Ingestion

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

## Usage

### Last.fm incremental pull

Run the `main.py` script to fetch and process Last.fm scrobbles:
```bash
python main.py
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

If stale, request a new Apple data export at `https://privacy.apple.com/`.
Apple usually takes a few days before the export is ready.

## Example Query

Once data is processed into Parquet files, you can query it using tools like DuckDB:

```sql
SELECT artist, COUNT(*) plays
FROM read_parquet('/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated/lastfm/scrobbles/year=*/month=*/*.parquet')
WHERE played_at_utc >= now() - INTERVAL 30 DAY
GROUP BY artist
ORDER BY plays DESC
LIMIT 50;
```
