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

```bash
python main.py
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
- Returns freshness exit codes (`0` fresh, `1` warning, `2` critical/missing).
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
- `~/datalake.me/raw/apple-music/<YYYYMMDD>/...Play Activity...csv`

### 2. Process CSV to curated parquet

```bash
python apple_music_processor.py
```

Optional explicit paths:

```bash
python apple_music_processor.py \
  --csv-file ~/datalake.me/raw/apple-music/20260210/Apple_Music_Play_Activity.csv \
  --curated-root ~/datalake.me/curated/apple-music/play-activity
```

### 3. Freshness monitoring

```bash
python apple_music_monitor.py --json
```

Exit codes:
- `0`: fresh
- `1`: warning (default >= 30 days stale)
- `2`: critical (default >= 90 days stale)

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
- `~/datalake.me/raw/apple-music/musickit/`
- `~/datalake.me/curated/apple-music/recent-played/`

## Example query

```sql
SELECT artist, COUNT(*) plays
FROM read_parquet('/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated/lastfm/scrobbles/year=*/month=*/*.parquet')
WHERE played_at_utc >= now() - INTERVAL 30 DAY
GROUP BY artist
ORDER BY plays DESC
LIMIT 50;
```
