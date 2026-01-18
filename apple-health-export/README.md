# Apple Health Data Exporter

A Python-based tool for exporting and analyzing Apple Health and Fitness data on macOS.

## Features

- **Export Health Data** - Helper to trigger manual export from Health.app
- **Parse XML Export** - Parse the exported XML into usable formats
- **Convert to CSV** - Export health records and workouts to CSV
- **Filter Data** - Filter by type, date range, or other criteria
- **Summary Statistics** - Get overview of your health data

## Requirements

- macOS with Health.app
- Python 3.8 or later
- Full disk access permissions for Terminal/your Python environment (if needed)

## Installation

### Option 1: Quick Setup with uv (Recommended)

If you have [uv](https://github.com/astral-sh/uv) installed:

```bash
./install.sh
```

This will install the package in development mode with all dependencies.

### Option 2: Manual Setup

```bash
# Install development dependencies (optional, for testing)
pip3 install pytest pytest-cov

# The scripts use only Python standard library, so they work without installation
python3 health_export.py --help
python3 health_parser.py --help
```

### Option 3: Using Make

```bash
# Install with make
make install

# Run tests
make test

# Run tests with coverage
make test-cov
```

## Testing

The tool includes comprehensive tests. To run them:

```bash
# Using unittest (built-in, no dependencies)
python3 -m unittest discover -s tests -p "test_*.py" -v

# Using pytest (if installed)
pytest -v

# With coverage
pytest --cov

# Using make
make test
```

All 48 tests should pass on macOS and Linux (though export triggering only works on macOS).

## Quick Start

### 1. Export Your Health Data

First, export your health data from the Health app:

```bash
# This will open Health.app with instructions
python3 health_export.py export
```

Then manually in Health.app:
1. Click your profile icon (top right)
2. Scroll down and click "Export All Health Data"
3. Save the `export.zip` file (suggested: ~/Downloads)

### 2. Extract the Export

```bash
# Auto-find and extract the most recent export
python3 health_export.py extract

# Or specify the file explicitly
python3 health_export.py extract --file ~/Downloads/export.zip
```

### 3. Parse and Analyze

```bash
# Get a summary of your health data
python3 health_parser.py export.xml summary

# List all available data types
python3 health_parser.py export.xml list-types

# List all workout types
python3 health_parser.py export.xml list-workouts
```

### 4. Export to CSV

```bash
# Export all step count data to CSV
python3 health_parser.py export.xml export-records \
  --type HKQuantityTypeIdentifierStepCount \
  --output steps.csv

# Export all workouts
python3 health_parser.py export.xml export-workouts \
  --output workouts.csv

# Export specific workout type (e.g., running)
python3 health_parser.py export.xml export-workouts \
  --type HKWorkoutActivityTypeRunning \
  --output running_workouts.csv

# Export data for a specific date range
python3 health_parser.py export.xml export-records \
  --type HKQuantityTypeIdentifierHeartRate \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --output heart_rate_2024.csv
```

## Common Data Types

Here are some commonly used health data types:

### Activity & Fitness
- `HKQuantityTypeIdentifierStepCount` - Step count
- `HKQuantityTypeIdentifierDistanceWalkingRunning` - Walking + running distance
- `HKQuantityTypeIdentifierActiveEnergyBurned` - Active calories
- `HKQuantityTypeIdentifierBasalEnergyBurned` - Resting calories
- `HKQuantityTypeIdentifierFlightsClimbed` - Flights climbed
- `HKQuantityTypeIdentifierAppleExerciseTime` - Exercise minutes

### Heart
- `HKQuantityTypeIdentifierHeartRate` - Heart rate
- `HKQuantityTypeIdentifierRestingHeartRate` - Resting heart rate
- `HKQuantityTypeIdentifierWalkingHeartRateAverage` - Walking average heart rate
- `HKQuantityTypeIdentifierHeartRateVariabilitySDNN` - Heart rate variability

### Sleep
- `HKCategoryTypeIdentifierSleepAnalysis` - Sleep data

### Body Measurements
- `HKQuantityTypeIdentifierBodyMass` - Weight
- `HKQuantityTypeIdentifierHeight` - Height
- `HKQuantityTypeIdentifierBodyMassIndex` - BMI
- `HKQuantityTypeIdentifierBodyFatPercentage` - Body fat percentage

### Workout Types
- `HKWorkoutActivityTypeRunning` - Running
- `HKWorkoutActivityTypeWalking` - Walking
- `HKWorkoutActivityTypeCycling` - Cycling
- `HKWorkoutActivityTypeSwimming` - Swimming
- `HKWorkoutActivityTypeYoga` - Yoga
- `HKWorkoutActivityTypeStrengthTraining` - Strength training

## Usage Examples

### Example 1: Analyze Your Steps Over Time

```bash
# Export all step data
python3 health_parser.py export.xml export-records \
  --type HKQuantityTypeIdentifierStepCount \
  --output steps.csv

# Now you can analyze with any tool (Excel, Python pandas, R, etc.)
```

### Example 2: Export All 2024 Workouts

```bash
python3 health_parser.py export.xml export-workouts \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --output workouts_2024.csv
```

### Example 3: Get Heart Rate Data

```bash
python3 health_parser.py export.xml export-records \
  --type HKQuantityTypeIdentifierHeartRate \
  --output heart_rate.csv
```

### Example 4: Export Running Workouts Only

```bash
python3 health_parser.py export.xml export-workouts \
  --type HKWorkoutActivityTypeRunning \
  --output running.csv
```

## File Structure

```
apple-health-export/
├── README.md                # This file
├── health_export.py         # Export and extraction utilities
└── health_parser.py         # XML parsing and CSV export
```

## How It Works

### Export Process

1. **Manual Export**: Apple doesn't provide a direct API to export health data, so the export must be triggered manually through Health.app
2. **ZIP File**: Health.app creates a ZIP file containing:
   - `export.xml` - Main health data in XML format
   - `export_cda.xml` - Clinical Document Architecture format
   - `workout-routes/` - GPX files for workouts with routes

### Parsing

The parser reads the `export.xml` file which contains:
- `<Record>` elements for health metrics (steps, heart rate, etc.)
- `<Workout>` elements for workouts
- `<ActivitySummary>` elements for daily activity rings
- Metadata and source information

### Data Format

The XML structure looks like:

```xml
<Record type="HKQuantityTypeIdentifierStepCount"
        sourceName="iPhone"
        unit="count"
        value="1234"
        startDate="2024-01-15 10:00:00 -0800"
        endDate="2024-01-15 10:15:00 -0800"/>

<Workout workoutActivityType="HKWorkoutActivityTypeRunning"
         duration="30.5"
         durationUnit="min"
         totalDistance="5.0"
         totalDistanceUnit="km"
         totalEnergyBurned="250"
         totalEnergyBurnedUnit="kcal"
         startDate="2024-01-15 06:00:00 -0800"
         endDate="2024-01-15 06:30:30 -0800"/>
```

## Tips

### Large Exports

If you have years of health data, the XML file can be several GB in size. Parsing may take a few minutes. Consider:

- Filtering by date range to reduce output size
- Exporting specific data types rather than everything
- Using the summary command first to understand what data you have

### Data Privacy

Your health data is sensitive. This tool:

- Runs entirely locally on your Mac
- Does not send data anywhere
- Creates CSV files that you control
- All data stays on your computer

Remember to:
- Keep exported CSV files secure
- Delete exports when done analyzing
- Be careful sharing exported data

### Automation

You can create shell scripts to automate common exports:

```bash
#!/bin/bash
# export_weekly.sh - Export this week's activity data

EXPORT_XML="$HOME/Downloads/apple_health_export_*/export.xml"
TODAY=$(date +%Y-%m-%d)
WEEK_AGO=$(date -v-7d +%Y-%m-%d)

python3 health_parser.py $EXPORT_XML export-records \
  --type HKQuantityTypeIdentifierStepCount \
  --start-date $WEEK_AGO \
  --end-date $TODAY \
  --output "steps_week_${TODAY}.csv"
```

## Troubleshooting

### "Error: export.xml not found"

Make sure you've extracted the export.zip file first:

```bash
python3 health_export.py extract --file path/to/export.zip
```

### "Error: osascript not found"

You're not running on macOS. This tool requires macOS with Health.app.

### Large file parsing is slow

This is normal for large exports (several years of data). The parser shows progress every 10,000 records.

### Missing data in CSV

Some data types may not have the fields you expect. Use `list-types` and `summary` commands to see what's actually in your export.

## Advanced Usage

### Custom Analysis with Python

Once you have CSV files, you can analyze them with pandas:

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load step data
steps = pd.read_csv('steps.csv')
steps['startDate'] = pd.to_datetime(steps['startDate'])
steps['date'] = steps['startDate'].dt.date

# Daily step totals
daily_steps = steps.groupby('date')['value'].sum()

# Plot
daily_steps.plot(figsize=(15, 5), title='Daily Steps')
plt.ylabel('Steps')
plt.show()
```

## License

MIT License - see [LICENSE](../LICENSE)

## Privacy & Security

This tool is designed for personal use only. Your health data never leaves your computer. Be mindful of:

- File permissions on exported CSV files
- Where you store exported data
- Who has access to your computer
- Backup encryption if you back up these files

## Contributing

This is a personal tool, but improvements are welcome. See [CONTRIBUTING.md](../CONTRIBUTING.md).
