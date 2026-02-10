# google-takeout-focused

Focused Google Takeout ingestion for issue #22, scoped to exactly:

- Location Timeline (`Location History (Timeline)`)
- Search History (`My Activity/Search`)
- YouTube Music History (`My Activity/YouTube and YouTube Music`, filtered to music activity)

This tool intentionally ignores Workspace and other Google products.

## Why this approach

Google Data Portability API is not practical for personal use in this repo's context. This tool automates the realistic workflow:

1. Request a selective Takeout export manually.
2. Drop zip files into a folder.
3. Run `sync` to process only new exports into curated parquet.

## Install

```bash
cd google-takeout-focused
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Commands

Print selective Takeout checklist:

```bash
google-takeout-focused guide
```

Process one zip/folder now:

```bash
google-takeout-focused process \
  --source ~/Downloads/takeout-2026-02-10T120000Z-001.zip \
  --raw-root ~/datalake.me/raw \
  --curated-root ~/datalake.me/curated
```

Quarterly (or scheduled) sync of all new exports in a drop folder:

```bash
google-takeout-focused sync \
  --takeout-dir ~/Downloads/takeout-drop \
  --raw-root ~/datalake.me/raw \
  --curated-root ~/datalake.me/curated
```

`sync` tracks processed exports in:

- `~/.local/share/datalake/google_takeout_focused_state.json`

## Output layout

- Raw archive copy:
  - `~/datalake.me/raw/google_takeout/archives/*.zip`
- Curated parquet datasets:
  - `~/datalake.me/curated/google_takeout/location_visits/year=YYYY/month=MM/*.parquet`
  - `~/datalake.me/curated/google_takeout/location_routes/year=YYYY/month=MM/*.parquet`
  - `~/datalake.me/curated/google_takeout/search_history/year=YYYY/month=MM/*.parquet`
  - `~/datalake.me/curated/google_takeout/youtube_music_history/year=YYYY/month=MM/*.parquet`

## Notes

- Location output includes both place visits and activity segments (routes) where available.
- Search output keeps parsed query plus full raw payload for replay.
- YouTube activity is filtered to music-specific events (`YouTube Music` / `music.youtube.com`).
