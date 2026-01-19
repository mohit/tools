# Quick Start Guide

Get started with the Personal Data Reflection Tool in 5 minutes!

## Installation

```bash
cd personal-data-reflection
pip install -r requirements.txt
```

## Step 1: Import Your Data

### Import Apple Health Data

First, export your Apple Health data and parse it using the `apple-health-export` tool:

```bash
# Export and parse Apple Health data
cd ../apple-health-export
python health_export.py export
python health_parser.py export.zip --output health_records.csv

# Import into reflection tool
cd ../personal-data-reflection
python reflect.py import-health ../apple-health-export/health_records.csv
```

### Import Strava Data

Export your Strava data using the `strava-data-puller` tool:

```bash
# Pull and export Strava data
cd ../strava-data-puller
python strava_pull.py --out-dir strava-export

# Import into reflection tool
cd ../personal-data-reflection
python reflect.py import-strava ../strava-data-puller/strava-export/
```

## Step 2: Explore Your Data

### Start the Dashboard

Launch the interactive web dashboard:

```bash
python reflect.py serve
```

Then open http://localhost:5000 in your browser to explore:
- Monthly statistics and trends
- Personalized insights (highlights, lowlights, patterns)
- Correlation analysis
- Pattern detection

### Generate a Monthly Report

Create a markdown report for the current month:

```bash
python reflect.py report --output reports/current-month.md
```

Or for a specific month:

```bash
python reflect.py report --month 2024-01 --output reports/january-2024.md
```

### Run Correlation Analysis

Discover connections between your metrics:

```bash
python reflect.py analyze
```

This will show you correlations like:
- How sleep affects your daily activity
- Relationship between workouts and sleep quality
- Heart rate variability patterns

## Step 3: Review and Reflect

1. **Open the Dashboard**: Navigate through the different views
2. **Read Your Insights**: Review highlights, lowlights, and patterns
3. **Check Correlations**: Understand what behaviors are connected
4. **Generate a Report**: Create a monthly summary document
5. **Plan Action Items**: Pick 1-3 behavior changes to try based on recommendations

## Monthly Workflow

We recommend this monthly routine:

1. **Week 1**: Import fresh data from Apple Health and Strava
2. **Week 2**: Review dashboard, note any surprising patterns
3. **Week 3**: Run correlation analysis, generate monthly report
4. **Week 4**: Read the report, identify 1-3 action items for next month

## Example Insights You'll Discover

The tool automatically generates insights like:

**Highlights:**
- "15 Excellent Days - You had 15 days meeting all health goals"
- "7-Day Workout Streak - Your longest streak was 7 consecutive days"

**Patterns:**
- "Monday is Your Most Active Day - You average 12,000 steps on Mondays"
- "Sleep correlates with performance - More sleep = more daily activity"

**Recommendations:**
- "Prioritize Sleep for Better Activity - Aim for 7-8 hours to boost steps"
- "Stay Active on Weekends - Weekend activity is 30% lower than weekdays"

## Database Location

Your data is stored locally at:
```
./data/reflection.duckdb
```

You can query it directly using DuckDB:

```bash
duckdb data/reflection.duckdb
```

```sql
-- Example queries
SELECT date, steps, sleep_hours FROM health_metrics ORDER BY date DESC LIMIT 10;
SELECT workout_type, COUNT(*) FROM workouts GROUP BY workout_type;
```

## Customization

Edit `config.yaml` to customize:
- Metrics to track
- Correlation pairs to analyze
- Thresholds for good/bad days
- Goals and targets

## Troubleshooting

**No data showing up?**
- Check that you've imported data: `ls data/`
- Verify the database exists: `ls data/reflection.duckdb`
- Re-import with verbose output to see any errors

**Dashboard not loading?**
- Make sure port 5000 is not in use
- Try a different port: `python reflect.py serve --port 8080`

**Import errors?**
- Check the file path is correct
- Ensure CSV/JSON files are in the expected format
- Look at the error message for specific issues

## Next Steps

- Set up a monthly reminder to review your data
- Export reports and track them over time
- Experiment with different behavior changes based on insights
- Share anonymized insights with your healthcare provider

Happy reflecting!
