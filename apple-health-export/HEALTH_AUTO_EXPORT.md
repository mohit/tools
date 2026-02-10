# Health Auto Export Integration

This document describes the REST endpoint and ingestion behavior used to integrate the iOS **Health Auto Export** app with this tool.

## Endpoint

- Method: `POST`
- Path: `/health-auto-export`
- Content-Type: `application/json`
- Optional auth header: `X-API-Key: <secret>`

Liveness endpoint:

- Method: `GET`
- Path: `/health`
- Response: `200 {"status":"ok"}`

## Run Server

```bash
export HEALTH_AUTO_EXPORT_API_KEY="replace-with-random-secret"

health-auto-export serve \
  --host 0.0.0.0 \
  --port 8787 \
  --raw-dir ~/datalake.me/raw/apple-health/health-auto-export \
  --curated-dir ~/datalake.me/datalake/curated/apple-health
```

## Payload Contract

The server accepts either shape below:

1. Canonical keys

```json
{
  "records": [
    {
      "type": "HKQuantityTypeIdentifierStepCount",
      "value": "123",
      "unit": "count",
      "startDate": "2026-02-10T07:00:00Z",
      "endDate": "2026-02-10T07:05:00Z",
      "sourceName": "iPhone"
    }
  ],
  "workouts": [
    {
      "workoutActivityType": "HKWorkoutActivityTypeRunning",
      "startDate": "2026-02-10T06:00:00Z",
      "endDate": "2026-02-10T06:30:00Z",
      "totalDistance": "5.0",
      "totalDistanceUnit": "km",
      "totalEnergyBurned": "350",
      "totalEnergyBurnedUnit": "kcal"
    }
  ]
}
```

2. Alternate keys (also accepted and normalized)

- `samples` instead of `records`
- `workoutSamples` instead of `workouts`
- `dataType` instead of `type`
- `start` / `end` instead of `startDate` / `endDate`

### Required fields

- Record: `type` (or `dataType`) and `startDate` (or `start`)
- Workout: `startDate` (or `start`)

## Storage Behavior

1. Raw immutable archive
- Every request is written to:
  - `~/datalake.me/raw/apple-health/health-auto-export/health_auto_export_<timestamp>_<rand>.json`

2. Curated parquet merge
- Records merged into:
  - `~/datalake.me/datalake/curated/apple-health/health_records.parquet`
- Workouts merged into:
  - `~/datalake.me/datalake/curated/apple-health/health_workouts.parquet`
- Dedup keys for records:
  - `type`, `startDate`, `endDate`, `value`, `sourceName`, `unit`
- Dedup keys for workouts:
  - `workoutActivityType`, `startDate`, `endDate`, `totalDistance`, `totalEnergyBurned`, `sourceName`

## Security Notes

- Keep `X-API-Key` enabled when server is reachable outside localhost.
- Prefer local network or VPN only; do not expose publicly without reverse proxy + TLS.
- Raw payloads are sensitive health data; keep directory permissions restricted.

## Quick Local Test

```bash
cat > /tmp/health-auto-export-sample.json <<'JSON'
{
  "records": [
    {
      "type": "HKQuantityTypeIdentifierHeartRate",
      "value": "62",
      "unit": "count/min",
      "startDate": "2026-02-10T08:00:00Z",
      "endDate": "2026-02-10T08:01:00Z"
    }
  ]
}
JSON

health-auto-export ingest-file --file /tmp/health-auto-export-sample.json
```

## Hybrid Mode Recommendation

- Daily: Health Auto Export incremental pushes to REST endpoint
- Quarterly: manual Health.app full XML export as a backstop
- Compare a sample range periodically to validate parity between incremental and full exports
