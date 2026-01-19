"""Strava data importer."""

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict
import duckdb


class StravaImporter:
    """Import Strava data into the reflection database."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize importer with database connection."""
        self.con = db_connection

    def import_from_directory(self, strava_dir: Path) -> Dict[str, int]:
        """Import Strava data from export directory.

        Expects directory structure from strava-data-puller tool.
        Returns dict with counts of imported records.
        """
        strava_dir = Path(strava_dir)
        if not strava_dir.exists():
            raise FileNotFoundError(f"Directory not found: {strava_dir}")

        counts = {
            "activities": 0,
            "workouts": 0,
        }

        # Import from Parquet files if they exist
        activities_parquet = strava_dir / "activities.parquet"
        if activities_parquet.exists():
            counts["activities"] = self._import_activities_parquet(activities_parquet)
            counts["workouts"] = counts["activities"]  # Each activity becomes a workout
        else:
            # Fall back to JSON/NDJSON
            activities_json = strava_dir / "activities.ndjson"
            if not activities_json.exists():
                activities_json = strava_dir / "activities.json"

            if activities_json.exists():
                counts["activities"] = self._import_activities_json(activities_json)
                counts["workouts"] = counts["activities"]

        return counts

    def _import_activities_parquet(self, parquet_path: Path) -> int:
        """Import activities from Parquet file."""
        # Read parquet into a temporary table
        self.con.execute(f"""
            CREATE OR REPLACE TEMP TABLE temp_strava AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        # Insert into strava_activities table
        self.con.execute("""
            INSERT OR REPLACE INTO strava_activities
            SELECT
                id,
                name,
                type,
                sport_type,
                start_date,
                start_date_local,
                distance,
                moving_time,
                elapsed_time,
                total_elevation_gain,
                average_speed,
                max_speed,
                average_heartrate,
                max_heartrate,
                average_watts,
                kilojoules,
                average_cadence,
                suffer_score,
                achievement_count,
                kudos_count,
                pr_count,
                to_json(*) as raw_data,
                CURRENT_TIMESTAMP as created_at
            FROM temp_strava
        """)

        # Also insert as workouts
        self.con.execute("""
            INSERT OR REPLACE INTO workouts
            SELECT
                'strava_' || CAST(id AS VARCHAR) as id,
                'strava' as source,
                type as workout_type,
                start_date_local as start_time,
                start_date_local + INTERVAL (elapsed_time || ' seconds')::INTERVAL as end_time,
                moving_time / 60.0 as duration_minutes,
                distance / 1000.0 as distance_km,
                total_elevation_gain as elevation_gain_m,
                kilojoules as calories,
                average_heartrate as avg_heart_rate,
                max_heartrate as max_heart_rate,
                CASE
                    WHEN distance > 0 AND moving_time > 0
                    THEN (moving_time / 60.0) / (distance / 1000.0)
                    ELSE NULL
                END as avg_pace_min_per_km,
                (distance / 1000.0) / (moving_time / 3600.0) as avg_speed_kmh,
                average_watts as avg_power_watts,
                to_json(*) as metadata,
                CURRENT_TIMESTAMP as created_at
            FROM temp_strava
            WHERE moving_time > 0
        """)

        count = self.con.execute("SELECT COUNT(*) FROM temp_strava").fetchone()[0]
        self.con.execute("DROP TABLE temp_strava")
        return count

    def _import_activities_json(self, json_path: Path) -> int:
        """Import activities from JSON or NDJSON file."""
        activities = []

        # Try to parse as NDJSON first
        if json_path.suffix == '.ndjson':
            with open(json_path, 'r') as f:
                for line in f:
                    if line.strip():
                        activities.append(json.loads(line))
        else:
            # Parse as regular JSON
            with open(json_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    activities = data
                else:
                    activities = [data]

        # Insert each activity
        for activity in activities:
            self._insert_activity(activity)

        return len(activities)

    def _insert_activity(self, activity: Dict):
        """Insert a single Strava activity."""
        try:
            # Insert into strava_activities
            self.con.execute("""
                INSERT OR REPLACE INTO strava_activities (
                    id, name, type, sport_type, start_date, start_date_local,
                    distance, moving_time, elapsed_time, total_elevation_gain,
                    average_speed, max_speed, average_heartrate, max_heartrate,
                    average_watts, kilojoules, average_cadence, suffer_score,
                    achievement_count, kudos_count, pr_count, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                activity.get('id'),
                activity.get('name'),
                activity.get('type'),
                activity.get('sport_type'),
                activity.get('start_date'),
                activity.get('start_date_local'),
                activity.get('distance'),
                activity.get('moving_time'),
                activity.get('elapsed_time'),
                activity.get('total_elevation_gain'),
                activity.get('average_speed'),
                activity.get('max_speed'),
                activity.get('average_heartrate'),
                activity.get('max_heartrate'),
                activity.get('average_watts'),
                activity.get('kilojoules'),
                activity.get('average_cadence'),
                activity.get('suffer_score'),
                activity.get('achievement_count'),
                activity.get('kudos_count'),
                activity.get('pr_count'),
                json.dumps(activity)
            ])

            # Insert as workout
            if activity.get('moving_time', 0) > 0:
                start_time = datetime.fromisoformat(
                    activity['start_date_local'].replace('Z', '+00:00')
                )
                end_time = start_time.replace(
                    microsecond=0
                ) + timedelta(seconds=activity['elapsed_time'])

                distance_km = activity.get('distance', 0) / 1000
                moving_time_min = activity.get('moving_time', 0) / 60
                avg_pace = moving_time_min / distance_km if distance_km > 0 else None
                avg_speed = distance_km / (activity.get('moving_time', 1) / 3600)

                self.con.execute("""
                    INSERT OR REPLACE INTO workouts (
                        id, source, workout_type, start_time, end_time,
                        duration_minutes, distance_km, elevation_gain_m, calories,
                        avg_heart_rate, max_heart_rate, avg_pace_min_per_km,
                        avg_speed_kmh, avg_power_watts, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    f"strava_{activity['id']}",
                    'strava',
                    activity.get('type'),
                    start_time,
                    end_time,
                    moving_time_min,
                    distance_km,
                    activity.get('total_elevation_gain'),
                    activity.get('kilojoules'),
                    activity.get('average_heartrate'),
                    activity.get('max_heartrate'),
                    avg_pace,
                    avg_speed,
                    activity.get('average_watts'),
                    json.dumps(activity)
                ])

        except Exception as e:
            print(f"Error importing activity {activity.get('id')}: {e}")
