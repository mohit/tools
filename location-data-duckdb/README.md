# location-data-duckdb

Local-first tooling to unify personal location history into DuckDB from:

- Google Location History / Google Maps exports (Google Takeout)
- Foursquare / Swarm (API where accessible, export fallback)
- Manual CSV exports
- Optional Google Places API enrichment

## What changed vs the initial draft

You were right to push on reducing manual export work. The pipeline now supports **two acquisition pathways per source**:

1. **Direct API ingestion** where personal-data endpoints still work.
2. **Export ingestion fallback** where APIs are limited or unavailable.

For Foursquare/Swarm specifically, this tool now includes a `foursquare_api` adapter that attempts:

- `users/self/checkins` → visits
- `users/self/lists` + list details → saved places
- `users/self/tips` → reviews/tips

using OAuth token auth and API version pinning.

> Note: legacy personal endpoints can vary by app/account access. Keep `foursquare_export` enabled as a safety fallback.

---

## Feasibility by source (practical pathways)

| Source | API path | Export path | Best practical strategy |
|---|---|---|---|
| Google Location History timeline | Limited first-party API for user timeline backfill | ✅ Google Takeout | Use Takeout + scheduled ingest. |
| Google Maps saved places/lists | No broad user-level lists API | ✅ Google Takeout | Parse list artifacts from Takeout. |
| Foursquare/Swarm check-ins/saves/tips | ✅ Legacy OAuth endpoints may work | ✅ Account export | Try API first, fallback to export on endpoint/access gaps. |
| Apple location history | No official personal timeline API | ⚠️ Partial/manual export options | Treat as optional/future source. |
| Google Places metadata | ✅ Places API | N/A | Enrich canonical places in DB. |

---

## Why this schema

The schema is intentionally split so behavior is queryable by **intent**, not just place coordinates.

### Core entity tables

- `visits`: actual presence at a place (check-ins, timeline visits).
- `saved_places`: places you bookmarked/saved for future intent.
- `place_reviews`: your authored text/rating artifacts (tips/reviews), separated from visits.
- `raw_events`: immutable source-native payloads for replay/debug.

### Supporting tables

- `place_dim`: deduplicated place keys across visit/save events.
- `place_enrichment_google`: cached Google Places attributes.
- `ingestion_runs`: audit and operational tracking.

### Why this keeps behavior clean

By separating `visits`, `saved_places`, and `place_reviews`, you can answer questions without mixing semantics:

- “Where have I actually been?” → `visits`
- “What have I planned or bookmarked?” → `saved_places`
- “What did I comment on/review?” → `place_reviews`

And because all three can share a `place_id`, you can still correlate across behaviors when needed.

---

## Quickstart

```bash
cd location-data-duckdb
python -m venv .venv
source .venv/bin/activate
pip install -e .
cp config.example.yaml config.yaml
```

Initialize the DuckDB file:

```bash
location-pipeline init-db --db-path ./data/location.duckdb
```

Run all configured sources:

```bash
location-pipeline run-all --config ./config.yaml
```

Run one source only:

```bash
location-pipeline run-source --config ./config.yaml --source foursquare_api
```

---

## Scheduling pathways (to minimize manual work)

### 1) API polling (preferred where supported)

- Enable `foursquare_api`.
- Set `FOURSQUARE_OAUTH_TOKEN`.
- Run `location-pipeline run-all` daily via cron/systemd/launchd.

### 2) Export drop-zone automation

When API access is not available:

- Keep `foursquare_export` / `google_takeout` enabled.
- Save recurring exports into `./imports/...` watched folders.
- Schedule `run-all` so new files are picked up automatically.

### 3) Browser-assisted fallback (future)

If neither API nor exports are usable:

- Capture structured records from authenticated pages via extension.
- Emit NDJSON locally.
- Add a `browser_capture` adapter into `raw_events` + typed tables.

---

## Example cron

```cron
15 6 * * * cd /path/to/location-data-duckdb && /path/to/.venv/bin/location-pipeline run-all --config ./config.yaml >> ./logs/pipeline.log 2>&1
```

---

## Security and privacy

- Local-first: no telemetry.
- Keep API keys/tokens in environment variables.
- Treat `raw_events.payload` as sensitive personal data.
- Encrypt backups and avoid syncing raw exports to shared folders unless encrypted.
