# Tools for Mohit

A collection of personal data and productivity tools. Each tool lives in its own folder with a focused README.

## Tools

- `anthropic-usage-tracker/` — Track Anthropic API token usage and estimated cost into daily datalake snapshots
- `apple-health-export/` — Export and analyze Apple Health and Fitness data on macOS
- `firefox-2fa-autofill/` — Browser extension that auto-fills 2FA codes from Google Voice SMS
- `google-takeout-focused/` — Focused Google Takeout ingestion for location, search, and YouTube Music history
- `location-data-duckdb/` — Unify personal location history (Google, Foursquare, manual) into DuckDB
- `music-history/` — Last.fm + Apple Music ingestion into partitioned Parquet
- `personal-assistant-ios/` — iOS app with Apple Health, CloudKit sync, and Google services integration
- `personal-data-reflection/` — Health and fitness data dashboard (Flask + DuckDB)
- `strava-data-puller/` — Pull Strava activities into DuckDB and Parquet
- `scripts/gcal_backup.py` — Weekly Google Calendar backup via gog CLI

## Repository Layout

- Each tool is a top-level folder.
- Shared repo docs live at the root (`AGENTS.md`, `CONTRIBUTING.md`, `LICENSE`).
- Tool-specific docs live inside each tool folder.

## License

MIT. See `LICENSE`.
