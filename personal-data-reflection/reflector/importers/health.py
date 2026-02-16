"""Apple Health data importer."""

import csv
import json
from datetime import date, datetime
from pathlib import Path

import duckdb


class HealthImporter:
    """Import Apple Health data into the reflection database."""

    # Mapping of Apple Health types to our metrics
    METRIC_MAPPING = {
        "HKQuantityTypeIdentifierStepCount": "steps",
        "HKQuantityTypeIdentifierDistanceWalkingRunning": "distance",
        "HKQuantityTypeIdentifierActiveEnergyBurned": "active_energy",
        "HKQuantityTypeIdentifierBasalEnergyBurned": "resting_energy",
        "HKQuantityTypeIdentifierAppleExerciseTime": "exercise_minutes",
        "HKQuantityTypeIdentifierFlightsClimbed": "flights_climbed",
        "HKQuantityTypeIdentifierRestingHeartRate": "resting_heart_rate",
        "HKQuantityTypeIdentifierWalkingHeartRateAverage": "walking_heart_rate",
        "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "hrv_sdnn",
        "HKQuantityTypeIdentifierBodyMass": "body_mass",
        "HKCategoryTypeIdentifierSleepAnalysis": "sleep",
    }

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize importer with database connection."""
        self.con = db_connection

    def import_from_csv(self, csv_path: Path) -> dict[str, int]:
        """Import Apple Health data from CSV file.

        Returns dict with counts of imported records.
        """
        csv_path = Path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Aggregate daily metrics
        daily_metrics: dict[date, dict] = {}
        workout_count = 0

        with open(csv_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record_type = row.get('type', '')

                if record_type in self.METRIC_MAPPING:
                    self._process_health_record(row, daily_metrics)
                elif 'Workout' in record_type:
                    self._process_workout(row)
                    workout_count += 1

        # Insert aggregated daily metrics
        self._insert_daily_metrics(daily_metrics)

        return {
            "health_metrics": len(daily_metrics),
            "workouts": workout_count
        }

    def import_from_json(self, json_path: Path) -> dict[str, int]:
        """Import Apple Health data from JSON file.

        Returns dict with counts of imported records.
        """
        json_path = Path(json_path)
        if not json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {json_path}")

        with open(json_path, encoding='utf-8') as f:
            data = json.load(f)

        daily_metrics: dict[date, dict] = {}
        workout_count = 0

        # Process records
        for record in data.get('records', []):
            record_type = record.get('type', '')
            if record_type in self.METRIC_MAPPING:
                self._process_health_record_json(record, daily_metrics)

        # Process workouts
        for workout in data.get('workouts', []):
            self._process_workout_json(workout)
            workout_count += 1

        # Insert aggregated daily metrics
        self._insert_daily_metrics(daily_metrics)

        return {
            "health_metrics": len(daily_metrics),
            "workouts": workout_count
        }

    def _process_health_record(self, row: dict, daily_metrics: dict):
        """Process a health record from CSV."""
        try:
            record_type = row['type']
            metric_name = self.METRIC_MAPPING.get(record_type)
            if not metric_name:
                return

            # Parse date and value
            start_date_str = row.get('startDate', '')
            if not start_date_str:
                return

            # Parse ISO datetime
            record_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).date()

            # Handle value based on metric type
            value = 0.0
            if metric_name == 'sleep':
                # Sleep value is a string category (e.g. Asleep, InBed)
                sleep_value = row.get('value', '')
                # Only count actual sleep, ignore "InBed" which overlaps
                if sleep_value in [
                    'HKCategoryValueSleepAnalysisAsleep',
                    'HKCategoryValueSleepAnalysisAsleepCore',
                    'HKCategoryValueSleepAnalysisAsleepDeep',
                    'HKCategoryValueSleepAnalysisAsleepREM'
                ]:
                    value = self._calculate_sleep_hours(row)
                else:
                    value = 0.0
            else:
                # Other metrics are numbers
                value = float(row.get('value', 0))

            # Convert units
            if metric_name == 'distance':
                # Convert meters to km
                if row.get('unit') == 'm':
                    value = value / 1000
            elif metric_name == 'exercise_minutes' and row.get('unit') == 'min':
                pass  # Already in minutes

            # Initialize daily metrics dict if needed
            if record_date not in daily_metrics:
                daily_metrics[record_date] = {}

            # Aggregate based on metric type
            if metric_name in ['steps', 'flights_climbed'] or metric_name in ['distance', 'active_energy', 'resting_energy', 'exercise_minutes']:
                # Sum these metrics
                daily_metrics[record_date][metric_name] = \
                    daily_metrics[record_date].get(metric_name, 0) + value
            elif metric_name == 'sleep':
                # Sum sleep durations
                daily_metrics[record_date]['sleep_hours'] = \
                    daily_metrics[record_date].get('sleep_hours', 0) + value
            else:
                # For heart rate, HRV, body mass - use latest value
                daily_metrics[record_date][metric_name] = value

        except (ValueError, KeyError):
            # Skip invalid records
            pass

    def _process_health_record_json(self, record: dict, daily_metrics: dict):
        """Process a health record from JSON."""
        try:
            record_type = record.get('type', '')
            metric_name = self.METRIC_MAPPING.get(record_type)
            if not metric_name:
                return

            start_date_str = record.get('startDate', '')
            if not start_date_str:
                return

            record_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).date()

            # Handle value based on metric type
            value = 0.0
            if metric_name == 'sleep':
                # Sleep value is a string category (e.g. Asleep, InBed)
                sleep_value = record.get('value', '')
                # Only count actual sleep, ignore "InBed" which overlaps
                if sleep_value in [
                    'HKCategoryValueSleepAnalysisAsleep',
                    'HKCategoryValueSleepAnalysisAsleepCore',
                    'HKCategoryValueSleepAnalysisAsleepDeep',
                    'HKCategoryValueSleepAnalysisAsleepREM'
                ]:
                    value = self._calculate_sleep_hours(record) # Re-use same helper since dict keys match
                else:
                    value = 0.0
            else:
                value = float(record.get('value', 0))

            # Initialize daily metrics dict if needed
            if record_date not in daily_metrics:
                daily_metrics[record_date] = {}

            # Similar aggregation logic as CSV
            if metric_name in ['steps', 'flights_climbed', 'distance', 'active_energy',
                             'resting_energy', 'exercise_minutes']:
                daily_metrics[record_date][metric_name] = \
                    daily_metrics[record_date].get(metric_name, 0) + value
            elif metric_name == 'sleep':
                daily_metrics[record_date]['sleep_hours'] = \
                    daily_metrics[record_date].get('sleep_hours', 0) + value
            else:
                daily_metrics[record_date][metric_name] = value

        except (ValueError, KeyError):
            pass

    def _calculate_sleep_hours(self, row: dict) -> float:
        """Calculate sleep hours from a sleep record."""
        try:
            start = datetime.fromisoformat(row['startDate'].replace('Z', '+00:00'))
            end = datetime.fromisoformat(row['endDate'].replace('Z', '+00:00'))
            duration = (end - start).total_seconds() / 3600  # Convert to hours
            return duration
        except (ValueError, KeyError):
            return 0.0

    def _process_workout(self, row: dict):
        """Process a workout record from CSV."""
        try:
            workout_type = row.get('workoutActivityType', 'Unknown')
            start_time = datetime.fromisoformat(row['startDate'].replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(row['endDate'].replace('Z', '+00:00'))

            duration_minutes = (end_time - start_time).total_seconds() / 60
            distance_km = float(row.get('totalDistance', 0))
            if row.get('totalDistanceUnit') == 'm':
                distance_km = distance_km / 1000

            calories = float(row.get('totalEnergyBurned', 0))

            # Generate workout ID
            workout_id = f"apple_health_{start_time.isoformat()}"

            self.con.execute("""
                INSERT OR REPLACE INTO workouts (
                    id, source, workout_type, start_time, end_time,
                    duration_minutes, distance_km, calories, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                workout_id,
                'apple_health',
                workout_type,
                start_time,
                end_time,
                duration_minutes,
                distance_km,
                calories,
                json.dumps(row)
            ])

        except (ValueError, KeyError):
            pass

    def _process_workout_json(self, workout: dict):
        """Process a workout record from JSON."""
        try:
            workout_type = workout.get('workoutActivityType', 'Unknown')
            start_time = datetime.fromisoformat(workout['startDate'].replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(workout['endDate'].replace('Z', '+00:00'))

            duration_minutes = (end_time - start_time).total_seconds() / 60
            distance_km = float(workout.get('totalDistance', 0))
            calories = float(workout.get('totalEnergyBurned', 0))

            workout_id = f"apple_health_{start_time.isoformat()}"

            self.con.execute("""
                INSERT OR REPLACE INTO workouts (
                    id, source, workout_type, start_time, end_time,
                    duration_minutes, distance_km, calories, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                workout_id,
                'apple_health',
                workout_type,
                start_time,
                end_time,
                duration_minutes,
                distance_km,
                calories,
                json.dumps(workout)
            ])

        except (ValueError, KeyError):
            pass

    def _insert_daily_metrics(self, daily_metrics: dict[date, dict]):
        """Insert aggregated daily metrics into the database."""
        for record_date, metrics in daily_metrics.items():
            # Determine sleep quality
            sleep_hours = metrics.get('sleep_hours', 0)
            if sleep_hours >= 7.5:
                sleep_quality = 'good'
            elif sleep_hours >= 6:
                sleep_quality = 'fair'
            else:
                sleep_quality = 'poor'

            self.con.execute("""
                INSERT OR REPLACE INTO health_metrics (
                    date, steps, distance_km, active_energy_kcal, resting_energy_kcal,
                    exercise_minutes, flights_climbed, resting_heart_rate,
                    walking_heart_rate, hrv_sdnn, sleep_hours, sleep_quality, body_mass_kg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                record_date,
                metrics.get('steps', 0),
                metrics.get('distance', 0),
                metrics.get('active_energy', 0),
                metrics.get('resting_energy', 0),
                metrics.get('exercise_minutes', 0),
                metrics.get('flights_climbed', 0),
                metrics.get('resting_heart_rate'),
                metrics.get('walking_heart_rate'),
                metrics.get('hrv_sdnn'),
                sleep_hours,
                sleep_quality if sleep_hours > 0 else None,
                metrics.get('body_mass')
            ])
