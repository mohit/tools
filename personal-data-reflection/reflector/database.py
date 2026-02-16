"""DuckDB database schema and operations for personal data reflection."""

from datetime import datetime
from pathlib import Path

import duckdb


class ReflectionDB:
    """Manages the DuckDB database for personal reflection data."""

    def __init__(self, db_path: str = "./data/reflection.duckdb"):
        """Initialize database connection and create schema if needed."""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self._create_schema()

    def _create_schema(self):
        """Create database schema if it doesn't exist."""

        # Health metrics table - daily aggregated health data
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS health_metrics (
                date DATE PRIMARY KEY,
                steps INTEGER,
                distance_km DOUBLE,
                active_energy_kcal DOUBLE,
                resting_energy_kcal DOUBLE,
                exercise_minutes DOUBLE,
                flights_climbed INTEGER,
                resting_heart_rate DOUBLE,
                walking_heart_rate DOUBLE,
                hrv_sdnn DOUBLE,
                sleep_hours DOUBLE,
                sleep_quality VARCHAR,  -- 'good', 'fair', 'poor' based on duration
                body_mass_kg DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Workouts table - individual workout sessions from both sources
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS workouts (
                id VARCHAR PRIMARY KEY,
                source VARCHAR,  -- 'apple_health' or 'strava'
                workout_type VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_minutes DOUBLE,
                distance_km DOUBLE,
                elevation_gain_m DOUBLE,
                calories DOUBLE,
                avg_heart_rate DOUBLE,
                max_heart_rate DOUBLE,
                avg_pace_min_per_km DOUBLE,
                avg_speed_kmh DOUBLE,
                avg_power_watts DOUBLE,
                metadata JSON,  -- Additional metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Strava activities - detailed activity data
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS strava_activities (
                id BIGINT PRIMARY KEY,
                name VARCHAR,
                type VARCHAR,
                sport_type VARCHAR,
                start_date TIMESTAMP,
                start_date_local TIMESTAMP,
                distance DOUBLE,
                moving_time INTEGER,
                elapsed_time INTEGER,
                total_elevation_gain DOUBLE,
                average_speed DOUBLE,
                max_speed DOUBLE,
                average_heartrate DOUBLE,
                max_heartrate DOUBLE,
                average_watts DOUBLE,
                kilojoules DOUBLE,
                average_cadence DOUBLE,
                suffer_score DOUBLE,
                achievement_count INTEGER,
                kudos_count INTEGER,
                pr_count INTEGER,
                raw_data JSON,  -- Full activity JSON
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Daily summary - aggregated view of all data
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                date DATE PRIMARY KEY,
                total_steps INTEGER,
                total_distance_km DOUBLE,
                total_active_energy DOUBLE,
                workout_count INTEGER,
                workout_duration_minutes DOUBLE,
                sleep_hours DOUBLE,
                avg_resting_hr DOUBLE,
                avg_hrv DOUBLE,
                day_of_week INTEGER,  -- 0=Monday, 6=Sunday
                is_weekend BOOLEAN,
                mood_score DOUBLE,  -- Can be added manually
                energy_level VARCHAR,  -- 'high', 'medium', 'low'
                notes VARCHAR,  -- Manual notes
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Correlations table - computed correlations between metrics
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS correlations (
                metric_a VARCHAR,
                metric_b VARCHAR,
                period_start DATE,
                period_end DATE,
                correlation_coefficient DOUBLE,
                p_value DOUBLE,
                sample_size INTEGER,
                description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (metric_a, metric_b, period_start, period_end)
            )
        """)

        # Insights table - generated insights and patterns
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS insights (
                category VARCHAR,  -- 'highlight', 'lowlight', 'pattern', 'recommendation'
                title VARCHAR,
                description VARCHAR,
                metrics_involved JSON,  -- List of metrics involved
                confidence_score DOUBLE,  -- 0.0 to 1.0
                period_start DATE,
                period_end DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # User Goals configuration
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS goals (
                metric VARCHAR PRIMARY KEY,
                target_value DOUBLE,
                period VARCHAR, -- 'daily', 'weekly'
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for common queries
        self._create_indexes()

    def _create_indexes(self):
        """Create indexes for performance."""
        try:
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_workouts_start ON workouts(start_time)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_workouts_type ON workouts(workout_type)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_strava_date ON strava_activities(start_date_local)")
            self.con.execute("CREATE INDEX IF NOT EXISTS idx_insights_category ON insights(category)")
        except duckdb.CatalogException:
            # Indexes may already exist
            pass

    def get_date_range(self) -> tuple[datetime | None, datetime | None]:
        """Get the min and max dates in the database."""
        result = self.con.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date
            FROM health_metrics
        """).fetchone()
        return (result[0], result[1]) if result else (None, None)

    def get_daily_summary(self, start_date: str, end_date: str) -> list:
        """Get daily summary for a date range."""
        return self.con.execute("""
            SELECT * FROM daily_summary
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """, [start_date, end_date]).fetchall()

    def get_health_metrics(self, start_date: str, end_date: str) -> list:
        """Get health metrics for a date range."""
        return self.con.execute("""
            SELECT * FROM health_metrics
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """, [start_date, end_date]).fetchall()

    def get_workouts(self, start_date: str, end_date: str) -> list:
        """Get workouts for a date range."""
        return self.con.execute("""
            SELECT * FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
            ORDER BY start_time
        """, [start_date, end_date]).fetchall()

    def get_monthly_stats(self, year: int, month: int) -> dict:
        """Get aggregated statistics for a specific month."""
        result = self.con.execute("""
            WITH monthly_data AS (
                SELECT
                    date,
                    steps,
                    distance_km,
                    active_energy_kcal,
                    exercise_minutes,
                    sleep_hours,
                    resting_heart_rate,
                    hrv_sdnn
                FROM health_metrics
                WHERE YEAR(date) = ? AND MONTH(date) = ?
            )
            SELECT
                COUNT(*) as days_tracked,
                AVG(steps) as avg_steps,
                SUM(steps) as total_steps,
                AVG(distance_km) as avg_distance,
                SUM(distance_km) as total_distance,
                AVG(active_energy_kcal) as avg_active_energy,
                SUM(active_energy_kcal) as total_active_energy,
                AVG(exercise_minutes) as avg_exercise_minutes,
                SUM(exercise_minutes) as total_exercise_minutes,
                AVG(sleep_hours) as avg_sleep_hours,
                AVG(resting_heart_rate) as avg_resting_hr,
                AVG(hrv_sdnn) as avg_hrv
            FROM monthly_data
        """, [year, month]).fetchone()

        workout_result = self.con.execute("""
            SELECT
                COUNT(*) as workout_count,
                SUM(duration_minutes) as total_workout_duration,
                SUM(distance_km) as total_workout_distance,
                SUM(calories) as total_calories
            FROM workouts
            WHERE YEAR(start_time) = ? AND MONTH(start_time) = ?
        """, [year, month]).fetchone()

        return {
            "days_tracked": result[0],
            "avg_steps": round(result[1], 0) if result[1] else 0,
            "total_steps": result[2] or 0,
            "avg_distance_km": round(result[3], 2) if result[3] else 0,
            "total_distance_km": round(result[4], 2) if result[4] else 0,
            "avg_active_energy": round(result[5], 0) if result[5] else 0,
            "total_active_energy": round(result[6], 0) if result[6] else 0,
            "avg_exercise_minutes": round(result[7], 1) if result[7] else 0,
            "total_exercise_minutes": round(result[8], 1) if result[8] else 0,
            "avg_sleep_hours": round(result[9], 1) if result[9] else 0,
            "avg_resting_hr": round(result[10], 1) if result[10] else 0,
            "avg_hrv": round(result[11], 1) if result[11] else 0,
            "workout_count": workout_result[0] or 0,
            "total_workout_duration": round(workout_result[1], 1) if workout_result[1] else 0,
            "total_workout_distance": round(workout_result[2], 2) if workout_result[2] else 0,
            "total_calories": round(workout_result[3], 0) if workout_result[3] else 0,
        }

    def get_aggregated_stats(self, start_date: str, end_date: str) -> dict:
        """Get aggregated statistics for a specific date range."""
        result = self.con.execute("""
            WITH period_data AS (
                SELECT
                    date,
                    steps,
                    distance_km,
                    active_energy_kcal,
                    exercise_minutes,
                    sleep_hours,
                    resting_heart_rate,
                    hrv_sdnn
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
            )
            SELECT
                COUNT(*) as days_tracked,
                AVG(steps) as avg_steps,
                SUM(steps) as total_steps,
                AVG(distance_km) as avg_distance,
                SUM(distance_km) as total_distance,
                AVG(active_energy_kcal) as avg_active_energy,
                SUM(active_energy_kcal) as total_active_energy,
                AVG(exercise_minutes) as avg_exercise_minutes,
                SUM(exercise_minutes) as total_exercise_minutes,
                AVG(sleep_hours) as avg_sleep_hours,
                AVG(resting_heart_rate) as avg_resting_hr,
                AVG(hrv_sdnn) as avg_hrv
            FROM period_data
        """, [start_date, end_date]).fetchone()

        workout_result = self.con.execute("""
            SELECT
                COUNT(*) as workout_count,
                SUM(duration_minutes) as total_workout_duration,
                SUM(distance_km) as total_workout_distance,
                SUM(calories) as total_calories
            FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        return {
            "days_tracked": result[0],
            "avg_steps": round(result[1], 0) if result[1] else 0,
            "total_steps": result[2] or 0,
            "avg_distance_km": round(result[3], 2) if result[3] else 0,
            "total_distance_km": round(result[4], 2) if result[4] else 0,
            "avg_active_energy": round(result[5], 0) if result[5] else 0,
            "total_active_energy": round(result[6], 0) if result[6] else 0,
            "avg_exercise_minutes": round(result[7], 1) if result[7] else 0,
            "total_exercise_minutes": round(result[8], 1) if result[8] else 0,
            "avg_sleep_hours": round(result[9], 1) if result[9] else 0,
            "avg_resting_hr": round(result[10], 1) if result[10] else 0,
            "avg_hrv": round(result[11], 1) if result[11] else 0,
            "workout_count": workout_result[0] or 0,
            "total_workout_duration": round(workout_result[1], 1) if workout_result[1] else 0,
            "total_workout_distance": round(workout_result[2], 2) if workout_result[2] else 0,
            "total_calories": round(workout_result[3], 0) if workout_result[3] else 0,
        }

    def rebuild_daily_summary(self):
        """Rebuild the daily_summary table from health_metrics and workouts."""
        self.con.execute("""
            INSERT OR REPLACE INTO daily_summary
            SELECT
                hm.date,
                hm.steps as total_steps,
                hm.distance_km as total_distance_km,
                hm.active_energy_kcal as total_active_energy,
                COALESCE(w.workout_count, 0) as workout_count,
                COALESCE(w.total_duration, 0) as workout_duration_minutes,
                hm.sleep_hours,
                hm.resting_heart_rate as avg_resting_hr,
                hm.hrv_sdnn as avg_hrv,
                DAYOFWEEK(hm.date) - 1 as day_of_week,
                DAYOFWEEK(hm.date) IN (6, 7) as is_weekend,
                NULL as mood_score,
                CASE
                    WHEN hm.active_energy_kcal > 500 THEN 'high'
                    WHEN hm.active_energy_kcal > 300 THEN 'medium'
                    ELSE 'low'
                END as energy_level,
                NULL as notes,
                CURRENT_TIMESTAMP as created_at
            FROM health_metrics hm
            LEFT JOIN (
                SELECT
                    DATE(start_time) as workout_date,
                    COUNT(*) as workout_count,
                    SUM(duration_minutes) as total_duration
                FROM workouts
                GROUP BY DATE(start_time)
            ) w ON hm.date = w.workout_date
        """)

    def get_goals(self) -> dict:
        """Get current user goals."""
        rows = self.con.execute("SELECT metric, target_value, period FROM goals").fetchall()

        # Default goals if none exist
        defaults = {
            "steps": {"target": 10000, "period": "daily"},
            "exercise_minutes": {"target": 30, "period": "daily"},
            "sleep_hours": {"target": 7.5, "period": "daily"},
            "resting_hr": {"target": 60, "period": "daily"},
            "hrv": {"target": 50, "period": "daily"}
        }

        goals = {}
        for row in rows:
            goals[row[0]] = {"target": row[1], "period": row[2]}

        # Merge with defaults
        for k, v in defaults.items():
            if k not in goals:
                goals[k] = v

        return goals

    def update_goal(self, metric: str, target: float, period: str = "daily"):
        """Update a specific goal."""
        self.con.execute("""
            INSERT OR REPLACE INTO goals (metric, target_value, period, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """, [metric, target, period])

    def close(self):
        """Close database connection."""
        self.con.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
