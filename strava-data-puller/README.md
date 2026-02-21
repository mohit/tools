# Strava Data Puller

Pull your Strava data locally via the Strava API with a focus on cycling and walking/hiking activities. The tool uses your Strava API app credentials plus a refresh token to retrieve activity data, athlete profile details, and stats.

## Features
- Export activities for cycling, walking, hiking, and related types.
- Fetch per-activity detail and optional time-series streams (distance, elevation, cadence, etc.).
- Save athlete profile info and lifetime stats.
- Output JSON for easy reuse in other tools.
- Export transformed Parquet files via DuckDB for analytics workflows (derived pace/speed and detail coverage fields).

## Setup
1. Create a Strava API application: <https://www.strava.com/settings/api>.
2. Record your **Client ID** and **Client Secret**.
3. Generate a refresh token by completing an OAuth authorization flow for your app. (Many guides show how to do this via a one-time auth URL.)
4. Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

5. Configure credentials for automated runs.

Recommended: create `strava-data-puller/.env` (auto-discovered by `strava_pull.py`):

```bash
cp .env.example .env
# then edit .env with real values
```

Or install credentials into the default automation path (`~/code/tools/strava-data-puller/.env`) in one step:

```bash
STRAVA_CLIENT_ID="YOUR_CLIENT_ID" \
STRAVA_CLIENT_SECRET="YOUR_CLIENT_SECRET" \
STRAVA_REFRESH_TOKEN="YOUR_REFRESH_TOKEN" \
python strava_pull.py --install-credentials
```

Alternative: export in shell environment:

```bash
export STRAVA_CLIENT_ID="YOUR_CLIENT_ID"
export STRAVA_CLIENT_SECRET="YOUR_CLIENT_SECRET"
export STRAVA_REFRESH_TOKEN="YOUR_REFRESH_TOKEN"
```

Alternative (macOS Keychain):

```bash
security add-generic-password -U -s strava-data-puller -a STRAVA_CLIENT_ID -w "YOUR_CLIENT_ID"
security add-generic-password -U -s strava-data-puller -a STRAVA_CLIENT_SECRET -w "YOUR_CLIENT_SECRET"
security add-generic-password -U -s strava-data-puller -a STRAVA_REFRESH_TOKEN -w "YOUR_REFRESH_TOKEN"
```

6. Verify credential discovery before scheduling:

```bash
python strava_pull.py --check-credentials
```

## Usage

```bash
python strava_pull.py \
  --out-dir ./strava-export \
  --types Ride,VirtualRide,GravelRide,Walk,Hike \
  --after 2023-01-01 \
  --include-streams

# Repair historical pulls where per-activity detail/streams are missing
python strava_pull.py \
  --out-dir ./strava-export \
  --backfill-details
```

### Options
- `--out-dir`: Directory for exported JSON (default: `./strava-export`).
- `--types`: Comma-separated activity types to include.
- `--after` / `--before`: ISO-8601 dates (YYYY-MM-DD) to filter activities.
- `--include-streams`: Also fetch activity streams for detailed analysis.
- `--backfill-details`: Scan existing activities and fetch missing detail payloads (`/activities/{id}` + streams).
- `--per-page`: Number of activities per page (default: 200, max allowed by Strava).
- `--max-pages`: Safety cap on pagination pages.
- `--skip-parquet`: Skip DuckDB Parquet export (default exports Parquet files).
- `--install-credentials`: Write discovered credentials to a stable `.env` file and exit.
- `--credentials-file`: Override target path for `--install-credentials`.

### Output structure
```
strava-export/
  athlete.json
  athlete.parquet
  stats.json
  stats.parquet
  activities.json
  activities.ndjson
  activities.parquet
  activity_details.ndjson
  activity_details.parquet
  activities/
    <activity_id>.json
  streams/
    <activity_id>.json
  activity_streams.ndjson
  activity_streams.parquet

`activities.parquet`, `activity_details.parquet`, and `activity_streams.parquet` are curated/typed tables with derived fields (for example `distance_km`, `average_speed_kph`, `moving_ratio`, `lap_count`, stream point counts) rather than byte-identical raw copies.

## DuckDB Example Query

Once the export completes, you can query the Parquet files using DuckDB:

```sql
SELECT name, COUNT(*) AS total_activities
FROM read_parquet('strava-export/activities.parquet')
GROUP BY name
ORDER BY total_activities DESC;
```
```

## Notes
- Pro subscription unlocks additional metrics in the Strava UI, but API access is primarily governed by OAuth scopes and rate limits. You should be fine with a Pro plan as long as your app has the `read` and `activity:read_all` scopes.
- The script stores data locally only.

## Testing
Credential resolution tests:

```bash
python3 -m unittest discover -s tests -q
```

## Troubleshooting
- **Missing credentials**: The script checks this order: environment vars -> `.env` files (`strava-data-puller/.env`, current working directory `.env`, `~/code/tools/strava-data-puller/.env`, or `STRAVA_ENV_FILE`) -> macOS keychain.
- **Unauthorized**: Confirm your refresh token is valid and your app has the correct scopes.
- **Rate limit exceeded**: The script prints rate-limit headers. Re-run after the cooldown or lower your `--max-pages` and skip streams.
