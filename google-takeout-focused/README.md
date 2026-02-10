# google-takeout-focused

Focused Google Takeout ingestion for exactly three personal analytics datasets:

- Location Timeline
- Search History
- YouTube Music History

It intentionally ignores Workspace and unrelated Google services.

## Scope

Included:
- `Location History (Timeline)` exports (records + semantic timeline)
- `Search` activity exports
- `YouTube and YouTube Music` history exports (music events only)

Excluded:
- Gmail, Drive, Docs, Photos, Videos, Chrome history, and all other Google services

## Setup

```bash
cd google-takeout-focused
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Quarterly workflow

1. Build a selective export request checklist:

```bash
google-takeout-focused takeout-checklist
```

2. Request/export from <https://takeout.google.com/> with only:
- `Location History (Timeline)`
- `Search`
- `YouTube and YouTube Music`

3. Extract the Takeout zip locally.

4. Analyze sample quality and coverage before ingest:

```bash
google-takeout-focused analyze \
  --input ~/Downloads/takeout-2026-02-10 \
  --report-path ~/datalake.me/catalog/google_takeout_analysis.json
```

5. Ingest raw + curated data with dedupe/merge:

```bash
google-takeout-focused ingest \
  --input ~/Downloads/takeout-2026-02-10 \
  --raw-root ~/datalake.me/raw \
  --curated-root ~/datalake.me/curated \
  --catalog-root ~/datalake.me/catalog
```

## Output layout

Raw snapshots (immutable):
- `~/datalake.me/raw/google-location/takeout_<snapshot_id>/...`
- `~/datalake.me/raw/google-search/takeout_<snapshot_id>/...`
- `~/datalake.me/raw/google-music/takeout_<snapshot_id>/...`

Curated parquet (deduplicated by deterministic `event_id`):
- `~/datalake.me/curated/google/location_timeline/year=*/month=*/*.parquet`
- `~/datalake.me/curated/google/search_history/year=*/month=*/*.parquet`
- `~/datalake.me/curated/google/youtube_music_history/year=*/month=*/*.parquet`

Catalog metadata:
- `~/datalake.me/catalog/google_takeout_focused.json`

## Notes

- `analyze` accepts either an extracted directory or a `.zip` archive.
- `ingest` requires an extracted directory so raw source files can be copied into immutable snapshot folders.
- The tool only parses the targeted Google services and ignores everything else in the archive.
