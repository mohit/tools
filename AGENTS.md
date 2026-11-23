# AGENTS.md — mohit/tools

## What this repo is
Personal data tools — ingest, transform, and reflect on health/fitness/music/location data. Each top-level folder is a standalone tool. Data is consolidated into a personal datalake (raw → curated parquet) on MoErMedia.

## Repo structure
```
amazon-orders-sync/      # Amazon orders/items/returns: Chrome+Safari extension (JS) → Flask+DuckDB ingest server on Tailscale
datalake-paths/          # Verified DATALAKE_ROOT locations + inode-safe atomic writes (stdlib)
anthropic-usage-tracker/ # Anthropic API usage tracking → parquet (Python, DuckDB)
apple-health-export/     # Apple Health XML → parquet (Python, DuckDB)
firefox-2fa-autofill/    # Browser extension (JS, WebExtension manifest)
google-takeout-focused/  # Google Takeout → location/search/music parquet (Python, DuckDB)
location-data-duckdb/    # Location/checkin data → DuckDB pipeline (Python)
music-history/           # Last.fm + Apple Music ingestion (Python, parquet)
personal-assistant-ios/  # iOS app (Swift)
personal-data-reflection/ # Health dashboard — Flask API + DuckDB (Python, uv)
scripts/                 # Standalone utility scripts (gcal_backup.py)
strava-data-puller/      # Strava API → raw JSON + curated parquet (Python, DuckDB)
```

## Tech stack
- **Language:** Python 3.11+ (most tools), JS (browser extension), Swift (iOS)
- **Data:** DuckDB for transforms, Parquet for storage, JSON for raw API dumps
- **Package management:** uv (for tools with pyproject.toml), pip otherwise
- **Web:** Flask (personal-data-reflection dashboard)
- **No monorepo build system** — each tool is independent

## Datalake conventions
- **Raw data:** `/Volumes/MoErMedia/datalake.me/raw/<source>/`
- **Curated data:** `/Volumes/MoErMedia/datalake.me/curated/<source>/` (parquet)
- **State files:** `/Volumes/MoErMedia/datalake.me/state/<tool>/` (last sync timestamps, etc.)
- **Databases / reports:** `databases/<tool>/`, `artifacts/<tool>/` under the same root.
- Resolve every path through `datalake-paths/` (`DATALAKE_ROOT`); entry points must call its verification before any request or write so a missing or wrong disk fails first. Never default to `~/datalake.me`, iCloud, `./data` or `~/.local/share`.
- Read `data-storage/README.md` before storage changes. Preserve immutable hard-linked payloads; publish replacements atomically with `datalake_paths.atomic_output`/`publish_file`, never by rewriting a file in place.
- moer-mini is the sole production writer. Scheduled/background jobs stay disabled until step 5 of the migration.
- Raw = immutable API dumps. Curated = cleaned, typed, deduplicated parquet.

## Coding conventions
- Standalone scripts with argparse CLIs — no frameworks beyond Flask
- Use `pathlib.Path` not string paths
- Environment variables for credentials (never hardcode)
- DuckDB for any data transforms — don't write pandas-heavy ETL
- Each tool has its own README with run instructions

## Testing
- `personal-data-reflection/` has pytest tests in `tests/`
- `location-data-duckdb/` has tests in `tests/`
- `amazon-orders-sync/` has `extension/test/` (`npm test`, `npm run e2e`) and `server/tests/` (pytest)
- Other tools: add tests if making non-trivial changes
- Run with: `cd <tool> && python -m pytest tests/`

## When fixing issues
- Read the tool's README first
- Check if there's existing state files/data you need to understand
- Don't break the raw → curated pipeline — raw is immutable
- If an issue requires API credentials, check env vars (STRAVA_*, LASTFM_*, etc.)
- Prefer incremental fixes over rewrites
- Add/update tests for behavioral changes

## PR guidelines
- One issue per PR, branch name: `fix/issue-{N}`
- Commit messages: reference the issue number
- Don't modify tools outside the scope of the issue
- If you find adjacent problems, file a new issue instead of scope-creeping
