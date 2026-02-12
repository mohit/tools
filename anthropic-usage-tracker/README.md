# Anthropic Usage Tracker

Track Anthropic API token usage and estimated costs over time, with daily snapshots written to your local datalake.

## What it does

- Pulls usage from Anthropic Admin Usage API (`/v1/organizations/usage_report/messages`)
- Pulls cost totals from Anthropic Admin Cost API (`/v1/organizations/cost_report`) when available
- Breaks usage down by model and API key id (which can be mapped to agent/session labels)
- Writes raw API payload snapshots and curated daily Parquet outputs
- Prints a daily summary: total input/output tokens, estimated total cost, model-level breakdown

## Requirements

- Python 3.11+
- Anthropic **Admin API key** in `ANTHROPIC_ADMIN_API_KEY`

## Setup

```bash
cd anthropic-usage-tracker
uv sync
```

## Usage

Run for UTC "today" (default):

```bash
anthropic-usage-track
```

Run for a specific day:

```bash
anthropic-usage-track --date 2026-02-11
```

Point at a specific datalake root (default: `~/datalake.me`):

```bash
anthropic-usage-track --datalake-root ~/datalake.me
```

Map API key ids to agent/session names:

```bash
export ANTHROPIC_AGENT_MAP_JSON='{"key_abc":"opus-main","key_def":"sonnet-silt"}'
anthropic-usage-track
```

Override model pricing (USD per million tokens) with a JSON file:

```json
{
  "claude-opus-4-1": {"input_per_million": 15.0, "output_per_million": 75.0},
  "claude-sonnet-4-5": {"input_per_million": 3.0, "output_per_million": 15.0}
}
```

```bash
anthropic-usage-track --pricing-json ./anthropic_pricing.json
```

## Output layout

Under `<datalake-root>` (default `~/datalake.me`):

- Raw snapshots:
  - `raw/anthropic/usage_report/date=YYYY-MM-DD/usage_<timestamp>.json`
  - `raw/anthropic/cost_report/date=YYYY-MM-DD/cost_<timestamp>.json`
- Curated parquet snapshots:
  - `curated/anthropic/usage_daily/year=YYYY/month=MM/day=DD/usage_daily_<timestamp>.parquet`
  - `curated/anthropic/model_daily/year=YYYY/month=MM/day=DD/model_daily_<timestamp>.parquet`

## Cron example

```cron
15 7 * * * cd /path/to/anthropic-usage-tracker && /path/to/.venv/bin/anthropic-usage-track --date $(date -u +\%F) >> ./logs/anthropic_usage.log 2>&1
```

## Notes

- This tool uses Anthropic Admin APIs. If usage/cost endpoints are unavailable to your account, the script exits with an actionable error.
- Cost estimate is token-based from a pricing table; API-reported cost (when available) is also stored and printed for comparison.
