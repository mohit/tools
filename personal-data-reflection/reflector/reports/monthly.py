"""Monthly reflection report generator."""

import duckdb
from datetime import datetime, date
from pathlib import Path
from typing import Dict

from reflector.analysis import InsightGenerator


class MonthlyReportGenerator:
    """Generate monthly reflection reports."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize report generator."""
        self.con = db_connection
        self.insight_gen = InsightGenerator(db_connection)

    def generate_report(
        self,
        year: int,
        month: int,
        output_format: str = 'markdown'
    ) -> str:
        """Generate a monthly reflection report.

        Args:
            year: Year
            month: Month (1-12)
            output_format: 'markdown' or 'text'

        Returns:
            Report content as string
        """
        # Get monthly stats
        stats = self._get_monthly_stats(year, month)

        # Generate insights
        insights = self.insight_gen.generate_monthly_insights(year, month)

        # Build report
        if output_format == 'markdown':
            return self._generate_markdown_report(year, month, stats, insights)
        else:
            return self._generate_text_report(year, month, stats, insights)

    def save_report(
        self,
        year: int,
        month: int,
        output_path: Path,
        output_format: str = 'markdown'
    ):
        """Generate and save a monthly report to file."""
        report = self.generate_report(year, month, output_format)

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)

        return output_path

    def _get_monthly_stats(self, year: int, month: int) -> Dict:
        """Get monthly statistics."""
        result = self.con.execute("""
            SELECT
                COUNT(*) as days_tracked,
                AVG(steps) as avg_steps,
                SUM(steps) as total_steps,
                MAX(steps) as max_steps,
                AVG(distance_km) as avg_distance,
                SUM(distance_km) as total_distance,
                AVG(active_energy_kcal) as avg_active_energy,
                SUM(active_energy_kcal) as total_active_energy,
                AVG(exercise_minutes) as avg_exercise,
                SUM(exercise_minutes) as total_exercise,
                AVG(sleep_hours) as avg_sleep,
                MIN(sleep_hours) as min_sleep,
                MAX(sleep_hours) as max_sleep,
                AVG(resting_heart_rate) as avg_resting_hr,
                AVG(hrv_sdnn) as avg_hrv,
                COUNT(CASE WHEN steps >= 10000 THEN 1 END) as days_over_10k_steps,
                COUNT(CASE WHEN sleep_hours >= 7 THEN 1 END) as days_good_sleep
            FROM health_metrics
            WHERE YEAR(date) = ? AND MONTH(date) = ?
        """, [year, month]).fetchone()

        workout_result = self.con.execute("""
            SELECT
                COUNT(*) as workout_count,
                SUM(duration_minutes) as total_duration,
                AVG(duration_minutes) as avg_duration,
                SUM(distance_km) as total_distance,
                SUM(calories) as total_calories
            FROM workouts
            WHERE YEAR(start_time) = ? AND MONTH(start_time) = ?
        """, [year, month]).fetchone()

        return {
            'days_tracked': result[0],
            'avg_steps': result[1],
            'total_steps': result[2],
            'max_steps': result[3],
            'avg_distance': result[4],
            'total_distance': result[5],
            'avg_active_energy': result[6],
            'total_active_energy': result[7],
            'avg_exercise': result[8],
            'total_exercise': result[9],
            'avg_sleep': result[10],
            'min_sleep': result[11],
            'max_sleep': result[12],
            'avg_resting_hr': result[13],
            'avg_hrv': result[14],
            'days_over_10k': result[15],
            'days_good_sleep': result[16],
            'workout_count': workout_result[0] or 0,
            'total_workout_duration': workout_result[1] or 0,
            'avg_workout_duration': workout_result[2] or 0,
            'total_workout_distance': workout_result[3] or 0,
            'total_calories': workout_result[4] or 0,
        }

    def _generate_markdown_report(
        self,
        year: int,
        month: int,
        stats: Dict,
        insights: Dict
    ) -> str:
        """Generate report in Markdown format."""
        month_name = date(year, month, 1).strftime('%B %Y')

        report = f"""# Personal Health Reflection - {month_name}

## Summary Statistics

### Activity
- **Total Steps**: {stats['total_steps']:,} ({stats['avg_steps']:.0f} avg/day)
- **Max Steps in a Day**: {stats['max_steps']:,}
- **Days with 10K+ Steps**: {stats['days_over_10k']} of {stats['days_tracked']} days
- **Total Distance**: {stats['total_distance']:.1f} km ({stats['avg_distance']:.1f} km avg/day)
- **Total Active Energy**: {stats['total_active_energy']:.0f} kcal ({stats['avg_active_energy']:.0f} avg/day)

### Exercise
- **Workouts Completed**: {stats['workout_count']}
- **Total Exercise Time**: {stats['total_workout_duration']:.0f} minutes ({stats['avg_workout_duration']:.0f} avg/workout)
- **Total Workout Distance**: {stats['total_workout_distance']:.1f} km
- **Calories Burned**: {stats['total_calories']:.0f} kcal

### Sleep
- **Average Sleep**: {stats['avg_sleep']:.1f} hours/night
- **Sleep Range**: {stats['min_sleep']:.1f}h - {stats['max_sleep']:.1f}h
- **Days with 7+ Hours**: {stats['days_good_sleep']} of {stats['days_tracked']} days

### Heart Health
"""

        if stats['avg_resting_hr']:
            report += f"- **Average Resting Heart Rate**: {stats['avg_resting_hr']:.0f} bpm\n"

        if stats['avg_hrv']:
            report += f"- **Average HRV**: {stats['avg_hrv']:.0f} ms\n"

        # Add highlights
        if insights['highlights']:
            report += "\n## Highlights - What Went Well\n\n"
            for insight in insights['highlights']:
                report += f"### {insight['title']}\n"
                report += f"{insight['description']}\n\n"

        # Add lowlights
        if insights['lowlights']:
            report += "\n## Areas for Improvement\n\n"
            for insight in insights['lowlights']:
                report += f"### {insight['title']}\n"
                report += f"{insight['description']}\n\n"

        # Add patterns
        if insights['patterns']:
            report += "\n## Patterns Discovered\n\n"
            for insight in insights['patterns']:
                report += f"### {insight['title']}\n"
                report += f"{insight['description']}\n\n"

        # Add recommendations
        if insights['recommendations']:
            report += "\n## Recommendations for Next Month\n\n"
            for i, insight in enumerate(insights['recommendations'], 1):
                report += f"{i}. **{insight['title']}**\n"
                report += f"   {insight['description']}\n\n"

        # Add closing
        report += f"\n---\n\n*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n"

        return report

    def _generate_text_report(
        self,
        year: int,
        month: int,
        stats: Dict,
        insights: Dict
    ) -> str:
        """Generate report in plain text format."""
        month_name = date(year, month, 1).strftime('%B %Y')

        report = f"""
{'='*70}
PERSONAL HEALTH REFLECTION - {month_name}
{'='*70}

SUMMARY STATISTICS
------------------

Activity:
  Total Steps: {stats['total_steps']:,} ({stats['avg_steps']:.0f} avg/day)
  Max Steps: {stats['max_steps']:,}
  Days with 10K+ Steps: {stats['days_over_10k']} of {stats['days_tracked']}
  Total Distance: {stats['total_distance']:.1f} km

Exercise:
  Workouts: {stats['workout_count']}
  Total Time: {stats['total_workout_duration']:.0f} minutes
  Calories: {stats['total_calories']:.0f} kcal

Sleep:
  Average: {stats['avg_sleep']:.1f} hours/night
  Range: {stats['min_sleep']:.1f}h - {stats['max_sleep']:.1f}h
  Days with 7+ hours: {stats['days_good_sleep']}

"""

        # Add insights
        if insights['highlights']:
            report += "\nHIGHLIGHTS - What Went Well\n" + "-"*30 + "\n\n"
            for insight in insights['highlights']:
                report += f"{insight['title']}\n"
                report += f"{insight['description']}\n\n"

        if insights['lowlights']:
            report += "\nAREAS FOR IMPROVEMENT\n" + "-"*30 + "\n\n"
            for insight in insights['lowlights']:
                report += f"{insight['title']}\n"
                report += f"{insight['description']}\n\n"

        if insights['recommendations']:
            report += "\nRECOMMENDATIONS FOR NEXT MONTH\n" + "-"*30 + "\n\n"
            for i, insight in enumerate(insights['recommendations'], 1):
                report += f"{i}. {insight['title']}\n"
                report += f"   {insight['description']}\n\n"

        report += f"\n{'='*70}\n"
        report += f"Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"

        return report
