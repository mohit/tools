# Quarterly Data Reflection Features

This document describes the quarterly data management and reflection features added to the Personal Data Reflection Tool.

## Overview

The tool now supports quarterly-based data management, which is ideal for users who want to:
- Keep only the most recent quarter of health and activity data
- Generate quarterly reflection reports to review longer-term trends
- Archive or cleanup older data automatically
- Focus on recent patterns while maintaining historical archives

## Features

### 1. Quarterly Reports

Generate comprehensive quarterly reflection reports that include:
- **Executive Summary**: Key metrics averaged across the quarter
- **Highlights**: Major achievements and positive trends
- **Quarterly Trends**: Month-over-month progression analysis
- **Monthly Breakdown**: Comparison of each month within the quarter
- **Detailed Statistics**: Activity, sleep, workouts, and heart health metrics
- **Areas for Improvement**: Lowlights and challenges
- **Patterns Discovered**: Behavioral insights and correlations
- **Recommendations**: Actionable suggestions for the next quarter

#### CLI Usage

```bash
# Generate report for current quarter
reflect quarterly

# Generate report for specific quarter
reflect quarterly --quarter 2024-Q1

# Save report to file (markdown format)
reflect quarterly --quarter 2024-Q1 --output reports/q1-2024.md

# Save as text format
reflect quarterly --quarter 2024-Q1 --output reports/q1-2024.txt
```

#### API Endpoints

```bash
# Get current quarter data
GET /api/quarterly/current

# Get specific quarter data
GET /api/quarterly/<year>/<quarter>
# Example: GET /api/quarterly/2024/1

# Get quarterly report
GET /api/quarterly/report/<year>/<quarter>?format=markdown
# Example: GET /api/quarterly/report/2024/1?format=markdown
```

### 2. Data Retention Management

Keep your database lean by retaining only recent quarters of data.

#### Check Retention Statistics

View how much data you have and what quarter it spans:

```bash
reflect retention stats
```

Output example:
```
Data Retention Statistics:
============================================================
Oldest data: 2023-10-01
Newest data: 2024-01-24
Data span: 115 days

Current quarter: 2024-01-01 to 2024-03-31

Record counts:
  Total health metrics: 115
  Current quarter: 24
  Total workouts: 45
  Current quarter: 12
  Total Strava activities: 30
```

#### Cleanup Old Data

Remove data older than a specified number of quarters:

```bash
# Dry run (see what would be deleted without deleting)
reflect retention cleanup --quarters-to-keep 1

# Actually delete old data
reflect retention cleanup --quarters-to-keep 1 --confirm

# Keep 2 quarters of data
reflect retention cleanup --quarters-to-keep 2 --confirm
```

**Important**: By default, cleanup runs in dry-run mode. Use `--confirm` to actually delete data.

#### Archive Old Data

Before deleting, you can archive old data to a separate database:

```bash
# Archive data older than 1 quarter
reflect retention archive --quarters-to-keep 1

# Specify custom archive location
reflect retention archive --quarters-to-keep 1 --archive-path ./archives/2024-q1.duckdb
```

This creates a separate DuckDB file with all the old data, allowing you to:
- Keep your main database small and fast
- Maintain historical records for future reference
- Load archived data later if needed

### 3. Quarterly Insights

The insight generator has been enhanced to provide quarterly-level analysis:

- **Trend Detection**: Identify improving or declining patterns across months
  - Activity trends (steps increasing/decreasing)
  - Sleep quality changes
  - Workout consistency patterns

- **Aggregated Insights**: Consolidate monthly insights into quarterly themes
  - Deduplicated and prioritized by confidence score
  - Top 5 insights per category (highlights, lowlights, patterns, recommendations)

- **Monthly Comparison**: See how each month compared within the quarter
  - Average steps, sleep, and exercise per month
  - Days hitting 10K+ steps per month
  - Workout distribution across the quarter

## Configuration

Update `config.yaml` to set default retention behavior:

```yaml
# Data retention settings (quarterly data management)
data_retention:
  quarters_to_keep: 1  # Number of quarters to retain
  auto_cleanup: false  # Automatically cleanup old data on import
  archive_old_data: false  # Archive data before deletion
```

## Use Cases

### Use Case 1: Quarterly Review Routine

1. At the end of each quarter, generate a reflection report:
   ```bash
   reflect quarterly --output reports/2024-q1.md
   ```

2. Review the report to understand:
   - Your overall performance
   - Trends across the 3 months
   - What to focus on next quarter

3. Archive old data and keep only the current quarter:
   ```bash
   reflect retention archive --quarters-to-keep 1
   reflect retention cleanup --quarters-to-keep 1 --confirm
   ```

### Use Case 2: Focused Analysis

If you only care about recent data:

1. Set retention to 1 quarter in config.yaml
2. Run cleanup monthly or quarterly:
   ```bash
   reflect retention cleanup --quarters-to-keep 1 --confirm
   ```

3. Your database stays small and queries are fast
4. Dashboard shows only relevant recent data

### Use Case 3: Data Export & Backup

Create quarterly archives for long-term storage:

```bash
# End of Q1 2024
reflect retention archive --quarters-to-keep 1 --archive-path ./archives/2024-q1.duckdb

# End of Q2 2024
reflect retention archive --quarters-to-keep 1 --archive-path ./archives/2024-q2.duckdb
```

You now have separate database files for each quarter that can be:
- Backed up to cloud storage
- Analyzed independently
- Shared with health professionals

## Integration with Existing Features

All existing features continue to work with quarterly data:

- **Monthly reports** still work within the retained quarter(s)
- **Dashboard** shows data from all retained quarters
- **Correlation analysis** uses all available data
- **Pattern detection** works across retained time span

## Technical Details

### Quarter Calculation

- Q1: January 1 - March 31
- Q2: April 1 - June 30
- Q3: July 1 - September 30
- Q4: October 1 - December 31

### Data Tables Affected

Retention cleanup affects these tables:
- `health_metrics`: Daily health data
- `workouts`: Workout records
- `strava_activities`: Strava activity data
- `insights`: Generated insights
- `correlations`: Correlation analysis results
- `daily_summary`: Aggregated daily summary

### Archive Database Schema

Archived databases have the same schema as the main database, making it easy to:
- Query archived data using the same SQL
- Re-import if needed
- Merge archives using DuckDB's ATTACH DATABASE

## Best Practices

1. **Always dry-run first**: Use `reflect retention cleanup` without `--confirm` to see what will be deleted

2. **Archive before cleanup**: Create archives of old data before permanently deleting:
   ```bash
   reflect retention archive --quarters-to-keep 1
   reflect retention cleanup --quarters-to-keep 1 --confirm
   ```

3. **Generate reports before cleanup**: Create quarterly reports before removing old data:
   ```bash
   reflect quarterly --quarter 2023-Q4 --output reports/2023-q4.md
   reflect retention cleanup --quarters-to-keep 1 --confirm
   ```

4. **Regular schedule**: Set a calendar reminder for the end of each quarter to:
   - Generate quarterly report
   - Review insights and set goals
   - Archive and cleanup old data

5. **Backup archives**: Store quarterly archive files in cloud storage or external backup

## API Integration

For building custom dashboards or integrations:

### Get Current Quarter Overview
```javascript
fetch('/api/quarterly/current')
  .then(res => res.json())
  .then(data => {
    console.log(`Current quarter: Q${data.quarter} ${data.year}`);
    console.log(`Average steps: ${data.stats.activity.avg_steps}`);
    console.log(`Highlights: ${data.insights.highlights.length}`);
  });
```

### Check Data Retention Status
```javascript
fetch('/api/retention/stats')
  .then(res => res.json())
  .then(stats => {
    console.log(`Data spans ${stats.data_span_days} days`);
    console.log(`Current quarter has ${stats.current_quarter_health_metrics} records`);
  });
```

### Generate Quarterly Report
```javascript
fetch('/api/quarterly/report/2024/1?format=markdown')
  .then(res => res.json())
  .then(data => {
    // Display markdown report
    document.getElementById('report').innerHTML = marked(data.report);
  });
```

## Troubleshooting

### "No data available" error
- Check that you have imported health/Strava data
- Run `reflect retention stats` to see date range
- Ensure the quarter you're requesting has data

### Cleanup deleted too much data
- If you have archive files, you can restore from them
- Use `--quarters-to-keep 2` or higher if you want more history
- Always use dry-run mode first to verify what will be deleted

### Quarterly report is empty
- Ensure data exists for all 3 months of the quarter
- Some insights require minimum data thresholds
- Check that correlation analysis has run: `reflect analyze`

## Future Enhancements

Potential additions (not yet implemented):
- Automatic scheduled cleanup
- Year-over-year quarterly comparisons
- Quarterly goal setting and tracking
- Visual quarterly progress charts
- Email/notification integration for quarterly reviews
