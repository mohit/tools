# Strava Data Puller

Pull your Strava data locally via the Strava API with a focus on cycling and walking/hiking activities. The tool uses your Strava API app credentials plus a refresh token to retrieve activity data, athlete profile details, and stats.

## Features
- Export activities for cycling, walking, hiking, and related types.
- Fetch per-activity detail and optional time-series streams (distance, elevation, cadence, etc.).
- Save athlete profile info and lifetime stats.
- Output JSON for easy reuse in other tools.
- Export Parquet files via DuckDB for analytics workflows.

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

5. Set environment variables (or add them to a `.env` and export them in your shell):

```bash
export STRAVA_CLIENT_ID="YOUR_CLIENT_ID"
export STRAVA_CLIENT_SECRET="YOUR_CLIENT_SECRET"
export STRAVA_REFRESH_TOKEN="YOUR_REFRESH_TOKEN"
```

## Usage

```bash
python strava_pull.py \
  --out-dir ./strava-export \
  --types Ride,VirtualRide,GravelRide,Walk,Hike \
  --after 2023-01-01 \
  --include-streams
```

### Options
- `--out-dir`: Directory for exported JSON (default: `./strava-export`).
- `--types`: Comma-separated activity types to include.
- `--after` / `--before`: ISO-8601 dates (YYYY-MM-DD) to filter activities.
- `--include-streams`: Also fetch activity streams for detailed analysis.
- `--per-page`: Number of activities per page (default: 200, max allowed by Strava).
- `--max-pages`: Safety cap on pagination pages.
- `--skip-parquet`: Skip DuckDB Parquet export (default exports Parquet files).

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
No automated tests are included.

## Troubleshooting
- **Unauthorized**: Confirm your refresh token is valid and your app has the correct scopes.
- **Rate limit exceeded**: The script prints rate-limit headers. Re-run after the cooldown or lower your `--max-pages` and skip streams.
