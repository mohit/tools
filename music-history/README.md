# Music History Ingestion

Ingests music listening data (Last.fm + Apple Music), processes it, and stores it in analytics-friendly Parquet.

## Sources

- **Last.fm** — API-based scrobble ingestion with incremental sync
- **Apple Music** — Privacy export (quarterly) + optional MusicKit recent-played snapshots

## Setup

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LASTFM_USER` | Yes (Last.fm) | — | Last.fm username |
| `LASTFM_API_KEY` | Yes (Last.fm) | — | [Get one here](https://www.last.fm/api/account/create) |
| `DATALAKE_RAW_ROOT` | No | `~/datalake.me/raw` | Root for raw data |
| `DATALAKE_CURATED_ROOT` | No | `~/datalake.me/curated` | Root for curated parquet |
| `DATALAKE_ROOT` | No | `~/datalake.me` | Used by legacy ingest scripts |
| `APPLE_MUSIC_DEVELOPER_TOKEN` | No | — | MusicKit developer token |
| `APPLE_MUSIC_USER_TOKEN` | No | — | MusicKit user token |

### Install Dependencies

```bash
uv sync
```

## Last.fm

### Incremental pull

```bash
python scripts/lastfm_ingest.py
```

Incremental by default:
- Uses `~/.local/share/datalake/lastfm_last_uts.txt` as the primary cursor.
- Falls back to scanning curated parquet for latest `uts` if the state file is missing/invalid.
- **Missing or corrupted state file triggers a full-history backfill** (from unix epoch 0) rather
  than an arbitrary recent window. If prior curated data exists without an interrupted multi-page
  run, the latest scrobble timestamp in parquet is used as the incremental start instead.
- Resumes interrupted runs from `~/.local/share/datalake/lastfm_ingest_checkpoint.json` unless `--no-resume` is set.
- Retries transient HTTP/network failures (including 5xx and timeouts) with exponential backoff.

### Options

```bash
# Start from a specific unix timestamp
python scripts/lastfm_ingest.py --from-uts 1735689600

# Start from a UTC date
python scripts/lastfm_ingest.py --since 2026-01-01

# Force full historical re-fetch
python scripts/lastfm_ingest.py --full-refetch

# Ignore a saved checkpoint and start selected range from page 1
python scripts/lastfm_ingest.py --no-resume
```

## Apple Music

MusicKit doesn't provide full historical listening history. Use the hybrid flow:
- **Quarterly**: privacy.apple.com export (full history)
- **Daily/optional**: MusicKit recent-played snapshot (max ~50 tracks)

### One-command sync (recommended for automation)

```bash
python apple_music_sync.py --json
```

Exit codes: `0` fresh, `1` warning, `2` critical, `3` missing CSV.

### Manual export helper (privacy.apple.com)

```bash
# Open privacy portal
python apple_music_export_helper.py --open-browser

# Extract Play Activity CSV from downloaded zip
python apple_music_export_helper.py --extract
python apple_music_export_helper.py --extract --zip-file ~/Downloads/privacy-export.zip
```

### Process CSV to curated parquet

```bash
python apple_music_processor.py
```

### Freshness monitoring

```bash
python apple_music_monitor.py --json
```

Exit codes: `0` fresh, `1` warning (≥30 days), `2` critical (≥90 days), `3` missing CSV.

### Export freshness guard

Ingestion checks `apple_music_export_metadata.json` before loading data:

```bash
python check_apple_music_export.py
python check_apple_music_privacy_export.py --max-age-days 45
```

### Optional MusicKit supplement

```bash
python apple_music_musickit_sync.py
```

Stores raw JSON + curated parquet at `$DATALAKE_RAW_ROOT/apple-music/musickit/`.

## Local export from Music.app

```bash
python export_apple_music.py      # Export to JSONL
python ingest_apple_music.py      # Ingest into curated parquet
```

## Example Query

```sql
SELECT artist, track, COUNT(*) as plays
FROM read_parquet('~/datalake.me/curated/lastfm/scrobbles/year=*/month=*/*.parquet', hive_partitioning=true)
GROUP BY artist, track
ORDER BY plays DESC
LIMIT 20;
```
