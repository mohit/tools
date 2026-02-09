# AGENTS.md — mohit/tools

## What this repo is
Personal data tools — ingest, transform, and reflect on health/fitness/music/location data. Each top-level folder is a standalone tool. Data flows into a personal datalake (raw → curated parquet) on iCloud.

## Repo structure
```
apple-health-export/     # Apple Health XML → parquet (Python, DuckDB)
firefox-2fa-autofill/    # Browser extension (JS, WebExtension manifest)
location-data-duckdb/    # Location/checkin data → DuckDB pipeline (Python)
music-history/           # Last.fm + Apple Music ingestion (Python, parquet)
personal-assistant-ios/  # iOS app (Swift)
personal-data-reflection/ # Health dashboard — Flask API + DuckDB (Python, uv)
strava-data-puller/      # Strava API → raw JSON + curated parquet (Python, DuckDB)
```

## Tech stack
- **Language:** Python 3.11+ (most tools), JS (browser extension), Swift (iOS)
- **Data:** DuckDB for transforms, Parquet for storage, JSON for raw API dumps
- **Package management:** uv (for tools with pyproject.toml), pip otherwise
- **Web:** Flask (personal-data-reflection dashboard)
- **No monorepo build system** — each tool is independent

## Datalake conventions
- **Raw data:** `~/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/<source>/`
- **Curated data:** `.../datalake/curated/<source>/` (parquet)
- **State files:** `~/.local/share/datalake/` (last sync timestamps, etc.)
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
