"""Data retention and cleanup manager for quarterly data management."""

import duckdb
from datetime import datetime, timedelta
from typing import Dict, Optional


class DataRetentionManager:
    """Manage data retention policies and cleanup operations."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize data retention manager."""
        self.con = db_connection

    def get_current_quarter_dates(self) -> tuple[str, str]:
        """Get start and end dates for the current quarter.

        Returns:
            Tuple of (start_date, end_date) as strings in YYYY-MM-DD format
        """
        today = datetime.now().date()
        quarter = (today.month - 1) // 3 + 1
        year = today.year

        # Calculate quarter start
        start_month = (quarter - 1) * 3 + 1
        start_date = datetime(year, start_month, 1).date()

        # Calculate quarter end
        if quarter == 4:
            end_date = datetime(year, 12, 31).date()
        else:
            next_quarter_start = datetime(year, start_month + 3, 1).date()
            end_date = next_quarter_start - timedelta(days=1)

        return str(start_date), str(end_date)

    def get_quarter_dates(self, year: int, quarter: int) -> tuple[str, str]:
        """Get start and end dates for a specific quarter.

        Args:
            year: The year
            quarter: The quarter number (1-4)

        Returns:
            Tuple of (start_date, end_date) as strings in YYYY-MM-DD format
        """
        if quarter < 1 or quarter > 4:
            raise ValueError("Quarter must be between 1 and 4")

        # Calculate quarter start
        start_month = (quarter - 1) * 3 + 1
        start_date = datetime(year, start_month, 1).date()

        # Calculate quarter end
        if quarter == 4:
            end_date = datetime(year, 12, 31).date()
        else:
            next_quarter_start = datetime(year, start_month + 3, 1).date()
            end_date = next_quarter_start - timedelta(days=1)

        return str(start_date), str(end_date)

    def get_retention_stats(self) -> Dict:
        """Get statistics about current data retention.

        Returns:
            Dictionary with data statistics
        """
        stats = {}

        # Get date range of all data
        date_range = self.con.execute("""
            SELECT MIN(date) as min_date, MAX(date) as max_date
            FROM health_metrics
        """).fetchone()

        if date_range and date_range[0]:
            stats['oldest_data'] = str(date_range[0])
            stats['newest_data'] = str(date_range[1])

            # Calculate data span in days
            if date_range[0] and date_range[1]:
                delta = date_range[1] - date_range[0]
                stats['data_span_days'] = delta.days

        # Count records
        stats['health_metrics_count'] = self.con.execute(
            "SELECT COUNT(*) FROM health_metrics"
        ).fetchone()[0]

        stats['workouts_count'] = self.con.execute(
            "SELECT COUNT(*) FROM workouts"
        ).fetchone()[0]

        stats['strava_activities_count'] = self.con.execute(
            "SELECT COUNT(*) FROM strava_activities"
        ).fetchone()[0]

        # Get current quarter info
        start_date, end_date = self.get_current_quarter_dates()
        stats['current_quarter_start'] = start_date
        stats['current_quarter_end'] = end_date

        # Count current quarter records
        stats['current_quarter_health_metrics'] = self.con.execute("""
            SELECT COUNT(*) FROM health_metrics
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()[0]

        stats['current_quarter_workouts'] = self.con.execute("""
            SELECT COUNT(*) FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()[0]

        return stats

    def cleanup_old_data(self, quarters_to_keep: int = 1, dry_run: bool = True) -> Dict:
        """Remove data older than specified number of quarters.

        Args:
            quarters_to_keep: Number of quarters to retain (default: 1)
            dry_run: If True, only report what would be deleted without actually deleting

        Returns:
            Dictionary with cleanup statistics
        """
        # Calculate cutoff date
        today = datetime.now().date()
        # Go back quarters_to_keep quarters
        months_back = quarters_to_keep * 3

        # Find the start of the quarter from months_back ago
        target_month = today.month - months_back
        target_year = today.year

        while target_month <= 0:
            target_month += 12
            target_year -= 1

        # Get the quarter for that month
        quarter = (target_month - 1) // 3 + 1
        cutoff_date, _ = self.get_quarter_dates(target_year, quarter)

        stats = {
            'cutoff_date': cutoff_date,
            'quarters_to_keep': quarters_to_keep,
            'dry_run': dry_run,
            'deleted': {}
        }

        # Count what will be deleted
        stats['deleted']['health_metrics'] = self.con.execute("""
            SELECT COUNT(*) FROM health_metrics WHERE date < ?
        """, [cutoff_date]).fetchone()[0]

        stats['deleted']['workouts'] = self.con.execute("""
            SELECT COUNT(*) FROM workouts WHERE DATE(start_time) < ?
        """, [cutoff_date]).fetchone()[0]

        stats['deleted']['strava_activities'] = self.con.execute("""
            SELECT COUNT(*) FROM strava_activities WHERE start_date_local < ?
        """, [cutoff_date]).fetchone()[0]

        stats['deleted']['insights'] = self.con.execute("""
            SELECT COUNT(*) FROM insights WHERE period_end < ?
        """, [cutoff_date]).fetchone()[0]

        if not dry_run:
            # Actually delete the data
            self.con.execute("DELETE FROM health_metrics WHERE date < ?", [cutoff_date])
            self.con.execute("DELETE FROM workouts WHERE DATE(start_time) < ?", [cutoff_date])
            self.con.execute("DELETE FROM strava_activities WHERE start_date_local < ?", [cutoff_date])
            self.con.execute("DELETE FROM insights WHERE period_end < ?", [cutoff_date])
            self.con.execute("DELETE FROM correlations WHERE period_end < ?", [cutoff_date])

            # Rebuild daily summary to reflect remaining data
            self.con.execute("DELETE FROM daily_summary WHERE date < ?", [cutoff_date])

        return stats

    def archive_old_data(self, quarters_to_keep: int = 1, archive_path: Optional[str] = None) -> Dict:
        """Archive data older than specified quarters to a separate database.

        Args:
            quarters_to_keep: Number of quarters to retain in main DB
            archive_path: Path to archive database (optional)

        Returns:
            Dictionary with archive statistics
        """
        if not archive_path:
            today = datetime.now().date()
            archive_path = f"./data/archive_{today.strftime('%Y%m%d')}.duckdb"

        # Calculate cutoff date
        today = datetime.now().date()
        months_back = quarters_to_keep * 3

        target_month = today.month - months_back
        target_year = today.year

        while target_month <= 0:
            target_month += 12
            target_year -= 1

        quarter = (target_month - 1) // 3 + 1
        cutoff_date, _ = self.get_quarter_dates(target_year, quarter)

        # Create archive database and copy old data
        archive_con = duckdb.connect(archive_path)

        # Create tables in archive database
        self._create_archive_tables(archive_con)

        # Copy data to archive
        stats = {
            'archive_path': archive_path,
            'cutoff_date': cutoff_date,
            'archived': {}
        }

        # Export and count archived records
        old_health = self.con.execute("""
            SELECT * FROM health_metrics WHERE date < ?
        """, [cutoff_date]).fetchdf()
        stats['archived']['health_metrics'] = len(old_health)
        if len(old_health) > 0:
            archive_con.execute("INSERT INTO health_metrics SELECT * FROM old_health")

        old_workouts = self.con.execute("""
            SELECT * FROM workouts WHERE DATE(start_time) < ?
        """, [cutoff_date]).fetchdf()
        stats['archived']['workouts'] = len(old_workouts)
        if len(old_workouts) > 0:
            archive_con.execute("INSERT INTO workouts SELECT * FROM old_workouts")

        old_strava = self.con.execute("""
            SELECT * FROM strava_activities WHERE start_date_local < ?
        """, [cutoff_date]).fetchdf()
        stats['archived']['strava_activities'] = len(old_strava)
        if len(old_strava) > 0:
            archive_con.execute("INSERT INTO strava_activities SELECT * FROM old_strava")

        archive_con.close()

        return stats

    def _create_archive_tables(self, archive_con: duckdb.DuckDBPyConnection):
        """Create tables in archive database."""
        # Copy schema from main database
        archive_con.execute("""
            CREATE TABLE IF NOT EXISTS health_metrics (
                date DATE PRIMARY KEY,
                steps INTEGER,
                distance_km DOUBLE,
                active_energy_kcal DOUBLE,
                resting_energy_kcal DOUBLE,
                exercise_minutes DOUBLE,
                flights_climbed INTEGER,
                sleep_hours DOUBLE,
                resting_heart_rate DOUBLE,
                walking_heart_rate DOUBLE,
                hrv_sdnn DOUBLE
            )
        """)

        archive_con.execute("""
            CREATE TABLE IF NOT EXISTS workouts (
                id VARCHAR PRIMARY KEY,
                source VARCHAR,
                workout_type VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_minutes DOUBLE,
                distance_km DOUBLE,
                calories DOUBLE,
                average_heart_rate DOUBLE,
                max_heart_rate DOUBLE
            )
        """)

        archive_con.execute("""
            CREATE TABLE IF NOT EXISTS strava_activities (
                id BIGINT PRIMARY KEY,
                name VARCHAR,
                type VARCHAR,
                sport_type VARCHAR,
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
                suffer_score INTEGER,
                raw_data JSON
            )
        """)
