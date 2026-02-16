"""Pattern detection in personal health data."""

import math

import duckdb


class PatternDetector:
    """Detect patterns and clusters in personal health data."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize pattern detector with database connection."""
        self.con = db_connection

    def get_default_criteria(self) -> dict:
        """Get the default criteria for a 'good day'."""
        return {
            'min_steps': 8000,
            'min_sleep': 7.0,
            'min_exercise': 20.0
        }

    def find_good_days(
        self,
        start_date: str,
        end_date: str,
        criteria: dict = None
    ) -> list[dict]:
        """Find days that meet 'good day' criteria.

        Default criteria:
        - Steps >= 8000
        - Sleep >= 7 hours
        - Exercise >= 20 minutes

        Returns list of good days with their metrics.
        """
        if criteria is None:
            criteria = self.get_default_criteria()

        result = self.con.execute("""
            SELECT
                date,
                steps,
                sleep_hours,
                exercise_minutes,
                active_energy_kcal,
                resting_heart_rate,
                hrv_sdnn
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND steps >= ?
              AND sleep_hours >= ?
              AND exercise_minutes >= ?
            ORDER BY date DESC
        """, [
            start_date,
            end_date,
            criteria['min_steps'],
            criteria['min_sleep'],
            criteria['min_exercise']
        ]).fetchall()

        return [
            {
                'date': row[0],
                'steps': row[1],
                'sleep_hours': row[2],
                'exercise_minutes': row[3],
                'active_energy': row[4],
                'resting_hr': row[5],
                'hrv': row[6]
            }
            for row in result
        ]

    def find_bad_days(
        self,
        start_date: str,
        end_date: str,
        criteria: dict = None
    ) -> list[dict]:
        """Find days that meet 'bad day' criteria.

        Default criteria:
        - Steps < 5000
        - Sleep < 6 hours
        - Exercise < 10 minutes

        Returns list of bad days with their metrics.
        """
        if criteria is None:
            criteria = {
                'max_steps': 5000,
                'max_sleep': 6.0,
                'max_exercise': 10.0
            }

        result = self.con.execute("""
            SELECT
                date,
                steps,
                sleep_hours,
                exercise_minutes,
                active_energy_kcal,
                resting_heart_rate
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND (steps < ?
                   OR sleep_hours < ?
                   OR exercise_minutes < ?)
            ORDER BY date DESC
        """, [
            start_date,
            end_date,
            criteria['max_steps'],
            criteria['max_sleep'],
            criteria['max_exercise']
        ]).fetchall()

        return [
            {
                'date': row[0],
                'steps': row[1],
                'sleep_hours': row[2],
                'exercise_minutes': row[3],
                'active_energy': row[4],
                'resting_hr': row[5],
                'issues': self._identify_issues(row, criteria)
            }
            for row in result
        ]

    def _identify_issues(self, row, criteria: dict) -> list[str]:
        """Identify what made a day 'bad'."""
        issues = []
        if row[1] and row[1] < criteria['max_steps']:
            issues.append(f"Low steps ({row[1]})")
        if row[2] and row[2] < criteria['max_sleep']:
            issues.append(f"Poor sleep ({row[2]:.1f}h)")
        if row[3] and row[3] < criteria['max_exercise']:
            issues.append(f"Minimal exercise ({row[3]:.0f}m)")
        return issues

    def detect_streaks(
        self,
        start_date: str,
        end_date: str,
        metric: str,
        threshold: float,
        operator: str = '>='
    ) -> list[dict]:
        """Detect consecutive day streaks meeting a criteria.

        Args:
            start_date: Start date
            end_date: End date
            metric: Metric name (e.g., 'steps', 'sleep_hours')
            threshold: Threshold value
            operator: Comparison operator (>=, >, <=, <)

        Returns:
            List of streaks with start_date, end_date, length
        """
        # Build the WHERE clause
        comparison = f"{metric} {operator} {threshold}"

        result = self.con.execute(f"""
            WITH daily_meets_criteria AS (
                SELECT
                    date,
                    {metric},
                    CASE WHEN {comparison} THEN 1 ELSE 0 END as meets_criteria
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
                  AND {metric} IS NOT NULL
                ORDER BY date
            ),
            streak_groups AS (
                SELECT
                    date,
                    {metric},
                    meets_criteria,
                    SUM(CASE
                        WHEN meets_criteria = 0 THEN 1
                        ELSE 0
                    END) OVER (ORDER BY date) as streak_group
                FROM daily_meets_criteria
            )
            SELECT
                MIN(date) as streak_start,
                MAX(date) as streak_end,
                COUNT(*) as streak_length,
                AVG({metric}) as avg_value
            FROM streak_groups
            WHERE meets_criteria = 1
            GROUP BY streak_group
            HAVING COUNT(*) >= 3
            ORDER BY streak_length DESC
        """, [start_date, end_date]).fetchall()

        return [
            {
                'start_date': row[0],
                'end_date': row[1],
                'length_days': row[2],
                'avg_value': round(row[3], 2) if row[3] else None,
                'metric': metric,
                'threshold': threshold
            }
            for row in result
        ]

    def analyze_day_of_week_patterns(
        self,
        start_date: str,
        end_date: str
    ) -> dict:
        """Analyze patterns by day of week.

        Returns average metrics for each day of week.
        """
        result = self.con.execute("""
            SELECT
                DAYOFWEEK(date) as dow,
                DAYNAME(date) as day_name,
                COUNT(*) as day_count,
                AVG(steps) as avg_steps,
                AVG(sleep_hours) as avg_sleep,
                AVG(exercise_minutes) as avg_exercise,
                AVG(active_energy_kcal) as avg_energy,
                AVG(resting_heart_rate) as avg_resting_hr
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
            GROUP BY DAYOFWEEK(date), DAYNAME(date)
            ORDER BY DAYOFWEEK(date)
        """, [start_date, end_date]).fetchall()

        return {
            row[1]: {  # day_name as key
                'day_of_week': row[0],
                'day_count': row[2],
                'avg_steps': round(row[3], 0) if row[3] else 0,
                'avg_sleep': round(row[4], 1) if row[4] else 0,
                'avg_exercise': round(row[5], 1) if row[5] else 0,
                'avg_energy': round(row[6], 0) if row[6] else 0,
                'avg_resting_hr': round(row[7], 1) if row[7] else 0,
            }
            for row in result
        }

    def find_workout_patterns(
        self,
        start_date: str,
        end_date: str
    ) -> dict:
        """Analyze workout patterns.

        Returns:
            - Most common workout types
            - Best performance days
            - Workout frequency patterns
        """
        # Most common workout types
        workout_types = self.con.execute("""
            SELECT
                workout_type,
                COUNT(*) as count,
                AVG(duration_minutes) as avg_duration,
                AVG(distance_km) as avg_distance,
                SUM(calories) as total_calories
            FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
            GROUP BY workout_type
            ORDER BY count DESC
        """, [start_date, end_date]).fetchall()

        # Workout frequency by day of week
        dow_frequency = self.con.execute("""
            SELECT
                DAYNAME(start_time) as day_name,
                COUNT(*) as workout_count
            FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
            GROUP BY DAYNAME(start_time), DAYOFWEEK(start_time)
            ORDER BY DAYOFWEEK(start_time)
        """, [start_date, end_date]).fetchall()

        # Best workout days (by calories)
        best_days = self.con.execute("""
            SELECT
                DATE(start_time) as workout_date,
                COUNT(*) as workout_count,
                SUM(duration_minutes) as total_duration,
                SUM(calories) as total_calories
            FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
            GROUP BY DATE(start_time)
            ORDER BY total_calories DESC
            LIMIT 10
        """, [start_date, end_date]).fetchall()

        return {
            'workout_types': [
                {
                    'type': row[0],
                    'count': row[1],
                    'avg_duration': round(row[2], 1) if row[2] else 0,
                    'avg_distance': round(row[3], 2) if row[3] else 0,
                    'total_calories': round(row[4], 0) if row[4] else 0
                }
                for row in workout_types
            ],
            'day_of_week_frequency': {
                row[0]: row[1] for row in dow_frequency
            },
            'best_workout_days': [
                {
                    'date': row[0],
                    'workout_count': row[1],
                    'total_duration': round(row[2], 1) if row[2] else 0,
                    'total_calories': round(row[3], 0) if row[3] else 0
                }
                for row in best_days
            ]
        }

    def detect_anomalies(
        self,
        start_date: str,
        end_date: str,
        metric: str,
        std_threshold: float = 2.0
    ) -> list[dict]:
        """Detect anomalies using standard deviation method.

        Flags values that are more than std_threshold standard deviations
        from the mean.

        Args:
            start_date: Start date
            end_date: End date
            metric: Metric to analyze
            std_threshold: Number of standard deviations for anomaly detection

        Returns:
            List of anomalous days
        """
        result = self.con.execute(f"""
            WITH stats AS (
                SELECT
                    AVG({metric}) as mean_value,
                    STDDEV({metric}) as std_value
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
                  AND {metric} IS NOT NULL
            )
            SELECT
                hm.date,
                hm.{metric} as value,
                s.mean_value,
                s.std_value,
                ABS(hm.{metric} - s.mean_value) / NULLIF(s.std_value, 0) as z_score
            FROM health_metrics hm, stats s
            WHERE hm.date BETWEEN ? AND ?
              AND hm.{metric} IS NOT NULL
              AND s.std_value IS NOT NULL AND s.std_value > 0
              AND ABS(hm.{metric} - s.mean_value) / s.std_value > ?
            ORDER BY z_score DESC
        """, [start_date, end_date, start_date, end_date, std_threshold]).fetchall()

        return [
            {
                'date': row[0],
                'value': round(row[1], 2) if row[1] is not None else None,
                'mean': round(row[2], 2) if row[2] is not None else None,
                'std': round(row[3], 2) if row[3] is not None else None,
                'z_score': round(row[4], 2) if row[4] is not None and not math.isnan(row[4]) else None,
                'metric': metric,
                'type': 'high' if row[1] > row[2] else 'low'
            }
            for row in result
        ]
