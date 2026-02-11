# Last.fm Data Ingestion

This project ingests Last.fm scrobble data.

## Setup

1.  **Environment Variables:**
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

## Usage

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

# ISO datetime (UTC if timezone omitted)
python scripts/lastfm_ingest.py --since 2025-02-05T00:00:00Z
```

Incremental mode behavior:
- Scans existing `*.jsonl` under output dir to find latest scrobble timestamp.
- Passes Last.fm API `from` parameter to limit fetched history.
- Deduplicates and appends only new rows into month partitions.
