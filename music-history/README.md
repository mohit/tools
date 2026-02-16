# Last.fm Data Ingestion

This project ingests Last.fm scrobble data, processes it, and stores it in Parquet format.

## Setup

1.  **Environment Variables:**
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

Run the `main.py` script to fetch and process Last.fm scrobbles:
```bash
python main.py
```

Incremental behavior:
- On each run, the script reads existing `*.jsonl` files under `.../raw/lastfm/` and finds the latest `uts`.
- It calls `user.getRecentTracks` with `from=<latest_uts+1>` so only new scrobbles are fetched.
- If no prior raw data exists, it performs a full-history backfill.
- Raw data is merged into monthly files: `scrobbles_YYYY-MM.jsonl` (deduped/upserted by `uts+artist+track+album`).

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
