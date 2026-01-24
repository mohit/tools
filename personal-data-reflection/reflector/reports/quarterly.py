"""Generate quarterly reflection reports."""

import duckdb
from datetime import datetime, timedelta
from typing import Optional
from ..analysis import InsightGenerator, CorrelationAnalyzer, PatternDetector


class QuarterlyReportGenerator:
    """Generate comprehensive quarterly reflection reports."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize quarterly report generator."""
        self.con = db_connection
        self.insight_generator = InsightGenerator(db_connection)
        self.corr_analyzer = CorrelationAnalyzer(db_connection)
        self.pattern_detector = PatternDetector(db_connection)

    def get_quarter_dates(self, year: int, quarter: int) -> tuple[str, str]:
        """Get start and end dates for a quarter."""
        if quarter < 1 or quarter > 4:
            raise ValueError("Quarter must be between 1 and 4")

        start_month = (quarter - 1) * 3 + 1
        start_date = datetime(year, start_month, 1).date()

        if quarter == 4:
            end_date = datetime(year, 12, 31).date()
        else:
            next_quarter_start = datetime(year, start_month + 3, 1).date()
            end_date = next_quarter_start - timedelta(days=1)

        return str(start_date), str(end_date)

    def generate_report(self, year: int, quarter: int, format: str = 'markdown') -> str:
        """Generate quarterly reflection report.

        Args:
            year: The year
            quarter: The quarter number (1-4)
            format: Output format ('markdown' or 'text')

        Returns:
            Formatted report as a string
        """
        start_date, end_date = self.get_quarter_dates(year, quarter)

        # Get summary statistics
        stats = self._get_quarterly_stats(start_date, end_date)

        # Generate insights for the full quarter
        insights = self._generate_quarterly_insights(year, quarter, start_date, end_date)

        # Generate monthly breakdown
        monthly_stats = self._get_monthly_breakdown(year, quarter)

        # Format the report
        if format == 'markdown':
            return self._format_markdown_report(year, quarter, stats, insights, monthly_stats)
        else:
            return self._format_text_report(year, quarter, stats, insights, monthly_stats)

    def _get_quarterly_stats(self, start_date: str, end_date: str) -> dict:
        """Get summary statistics for the quarter."""
        stats = {}

        # Activity metrics
        result = self.con.execute("""
            SELECT
                AVG(steps) as avg_steps,
                MAX(steps) as max_steps,
                MIN(steps) as min_steps,
                AVG(distance_km) as avg_distance,
                SUM(distance_km) as total_distance,
                COUNT(CASE WHEN steps >= 10000 THEN 1 END) as days_over_10k
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND steps IS NOT NULL
        """, [start_date, end_date]).fetchone()

        if result:
            stats['activity'] = {
                'avg_steps': result[0] or 0,
                'max_steps': result[1] or 0,
                'min_steps': result[2] or 0,
                'avg_distance_km': result[3] or 0,
                'total_distance_km': result[4] or 0,
                'days_over_10k': result[5] or 0
            }

        # Energy metrics
        result = self.con.execute("""
            SELECT
                AVG(active_energy_kcal) as avg_active,
                SUM(active_energy_kcal) as total_active,
                AVG(resting_energy_kcal) as avg_resting
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result:
            stats['energy'] = {
                'avg_active_kcal': result[0] or 0,
                'total_active_kcal': result[1] or 0,
                'avg_resting_kcal': result[2] or 0
            }

        # Sleep metrics
        result = self.con.execute("""
            SELECT
                AVG(sleep_hours) as avg_sleep,
                MAX(sleep_hours) as max_sleep,
                MIN(sleep_hours) as min_sleep,
                COUNT(CASE WHEN sleep_hours >= 7 THEN 1 END) as good_sleep_days,
                COUNT(CASE WHEN sleep_hours < 6 THEN 1 END) as poor_sleep_days
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND sleep_hours IS NOT NULL
        """, [start_date, end_date]).fetchone()

        if result:
            stats['sleep'] = {
                'avg_hours': result[0] or 0,
                'max_hours': result[1] or 0,
                'min_hours': result[2] or 0,
                'good_sleep_days': result[3] or 0,
                'poor_sleep_days': result[4] or 0
            }

        # Heart health metrics
        result = self.con.execute("""
            SELECT
                AVG(resting_heart_rate) as avg_rhr,
                MIN(resting_heart_rate) as min_rhr,
                AVG(hrv_sdnn) as avg_hrv,
                MAX(hrv_sdnn) as max_hrv
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result:
            stats['heart_health'] = {
                'avg_resting_hr': result[0] or 0,
                'min_resting_hr': result[1] or 0,
                'avg_hrv': result[2] or 0,
                'max_hrv': result[3] or 0
            }

        # Workout metrics
        result = self.con.execute("""
            SELECT
                COUNT(*) as total_workouts,
                SUM(duration_minutes) as total_duration,
                AVG(duration_minutes) as avg_duration,
                COUNT(DISTINCT workout_type) as workout_types,
                COUNT(DISTINCT DATE(start_time)) as workout_days
            FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result:
            stats['workouts'] = {
                'total_count': result[0] or 0,
                'total_minutes': result[1] or 0,
                'avg_duration': result[2] or 0,
                'unique_types': result[3] or 0,
                'workout_days': result[4] or 0
            }

        # Exercise minutes from health metrics
        result = self.con.execute("""
            SELECT
                AVG(exercise_minutes) as avg_exercise,
                SUM(exercise_minutes) as total_exercise,
                COUNT(CASE WHEN exercise_minutes >= 30 THEN 1 END) as days_30plus
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result:
            stats['exercise'] = {
                'avg_minutes': result[0] or 0,
                'total_minutes': result[1] or 0,
                'days_30plus': result[2] or 0
            }

        return stats

    def _generate_quarterly_insights(self, year: int, quarter: int, start_date: str, end_date: str) -> dict:
        """Generate insights for the entire quarter."""
        insights = {
            'highlights': [],
            'lowlights': [],
            'patterns': [],
            'recommendations': [],
            'trends': []
        }

        # Get insights for each month and aggregate
        start_month = (quarter - 1) * 3 + 1
        for month_offset in range(3):
            month = start_month + month_offset
            if month <= 12:
                monthly_insights = self.insight_generator.generate_monthly_insights(year, month)
                for category in ['highlights', 'lowlights', 'patterns', 'recommendations']:
                    insights[category].extend(monthly_insights.get(category, []))

        # Add quarter-specific trends
        insights['trends'].extend(self._analyze_quarterly_trends(start_date, end_date))

        # Deduplicate and prioritize insights
        insights = self._prioritize_insights(insights)

        return insights

    def _analyze_quarterly_trends(self, start_date: str, end_date: str) -> list:
        """Analyze trends across the quarter."""
        trends = []

        # Analyze step trend
        result = self.con.execute("""
            WITH monthly_avg AS (
                SELECT
                    strftime('%Y-%m', date) as month,
                    AVG(steps) as avg_steps
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
                GROUP BY strftime('%Y-%m', date)
                ORDER BY month
            )
            SELECT
                month,
                avg_steps,
                LAG(avg_steps) OVER (ORDER BY month) as prev_month_steps
            FROM monthly_avg
        """, [start_date, end_date]).fetchall()

        if len(result) >= 2:
            first_month = result[0][1]
            last_month = result[-1][1]
            if last_month > first_month * 1.1:  # 10% increase
                trends.append({
                    'title': 'Increasing Activity Trend',
                    'description': f'Your average daily steps increased from {first_month:,.0f} to {last_month:,.0f} across the quarter. Great momentum!',
                    'confidence': 0.9,
                    'metrics': ['steps']
                })
            elif last_month < first_month * 0.9:  # 10% decrease
                trends.append({
                    'title': 'Declining Activity Trend',
                    'description': f'Your average daily steps decreased from {first_month:,.0f} to {last_month:,.0f}. Consider what changed and how to rebuild momentum.',
                    'confidence': 0.9,
                    'metrics': ['steps']
                })

        # Analyze sleep trend
        result = self.con.execute("""
            WITH monthly_avg AS (
                SELECT
                    strftime('%Y-%m', date) as month,
                    AVG(sleep_hours) as avg_sleep
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
                  AND sleep_hours IS NOT NULL
                GROUP BY strftime('%Y-%m', date)
                ORDER BY month
            )
            SELECT
                month,
                avg_sleep,
                LAG(avg_sleep) OVER (ORDER BY month) as prev_month_sleep
            FROM monthly_avg
        """, [start_date, end_date]).fetchall()

        if len(result) >= 2:
            first_month = result[0][1]
            last_month = result[-1][1]
            if last_month > first_month + 0.5:  # 30+ minutes increase
                trends.append({
                    'title': 'Improving Sleep Pattern',
                    'description': f'Your average sleep improved from {first_month:.1f} to {last_month:.1f} hours. Better sleep supports all other health goals.',
                    'confidence': 0.9,
                    'metrics': ['sleep_hours']
                })
            elif last_month < first_month - 0.5:  # 30+ minutes decrease
                trends.append({
                    'title': 'Declining Sleep Quality',
                    'description': f'Your average sleep decreased from {first_month:.1f} to {last_month:.1f} hours. Prioritizing sleep should be a focus.',
                    'confidence': 0.9,
                    'metrics': ['sleep_hours']
                })

        return trends

    def _get_monthly_breakdown(self, year: int, quarter: int) -> list:
        """Get summary stats for each month in the quarter."""
        monthly_data = []
        start_month = (quarter - 1) * 3 + 1

        for month_offset in range(3):
            month = start_month + month_offset
            if month > 12:
                break

            # Calculate month dates
            month_start = datetime(year, month, 1).date()
            if month == 12:
                month_end = datetime(year, 12, 31).date()
            else:
                next_month = datetime(year, month + 1, 1).date()
                month_end = next_month - timedelta(days=1)

            # Get stats for this month
            result = self.con.execute("""
                SELECT
                    AVG(steps) as avg_steps,
                    AVG(sleep_hours) as avg_sleep,
                    AVG(exercise_minutes) as avg_exercise,
                    COUNT(CASE WHEN steps >= 10000 THEN 1 END) as days_10k
                FROM health_metrics
                WHERE date BETWEEN ? AND ?
            """, [str(month_start), str(month_end)]).fetchone()

            if result:
                monthly_data.append({
                    'month': month,
                    'month_name': month_start.strftime('%B'),
                    'avg_steps': result[0] or 0,
                    'avg_sleep': result[1] or 0,
                    'avg_exercise': result[2] or 0,
                    'days_10k': result[3] or 0
                })

        return monthly_data

    def _prioritize_insights(self, insights: dict) -> dict:
        """Deduplicate and prioritize insights."""
        # For each category, keep only the top insights by confidence
        for category in insights:
            # Sort by confidence
            insights[category] = sorted(
                insights[category],
                key=lambda x: x.get('confidence', 0),
                reverse=True
            )

            # Remove duplicates by title
            seen_titles = set()
            unique_insights = []
            for insight in insights[category]:
                if insight['title'] not in seen_titles:
                    seen_titles.add(insight['title'])
                    unique_insights.append(insight)

            # Keep top 5 per category
            insights[category] = unique_insights[:5]

        return insights

    def _format_markdown_report(self, year: int, quarter: int, stats: dict, insights: dict, monthly_stats: list) -> str:
        """Format report as Markdown."""
        quarter_name = f"Q{quarter} {year}"
        start_date, end_date = self.get_quarter_dates(year, quarter)

        report = f"# Personal Data Reflection: {quarter_name}\n\n"
        report += f"**Period:** {start_date} to {end_date}\n\n"
        report += "---\n\n"

        # Executive Summary
        report += "## Executive Summary\n\n"
        if 'activity' in stats:
            report += f"- **Average Daily Steps:** {stats['activity']['avg_steps']:,.0f}\n"
            report += f"- **Days with 10K+ Steps:** {stats['activity']['days_over_10k']}\n"
        if 'sleep' in stats:
            report += f"- **Average Sleep:** {stats['sleep']['avg_hours']:.1f} hours\n"
        if 'workouts' in stats:
            report += f"- **Total Workouts:** {stats['workouts']['total_count']}\n"
            report += f"- **Workout Days:** {stats['workouts']['workout_days']}\n"
        report += "\n"

        # Highlights
        if insights['highlights']:
            report += "## Highlights\n\n"
            for insight in insights['highlights']:
                report += f"### {insight['title']}\n"
                report += f"{insight['description']}\n\n"

        # Trends
        if insights.get('trends'):
            report += "## Quarterly Trends\n\n"
            for trend in insights['trends']:
                report += f"### {trend['title']}\n"
                report += f"{trend['description']}\n\n"

        # Monthly Breakdown
        if monthly_stats:
            report += "## Monthly Breakdown\n\n"
            report += "| Month | Avg Steps | Avg Sleep | Avg Exercise | 10K Days |\n"
            report += "|-------|-----------|-----------|--------------|----------|\n"
            for month in monthly_stats:
                report += f"| {month['month_name']} | {month['avg_steps']:,.0f} | {month['avg_sleep']:.1f}h | {month['avg_exercise']:.0f}m | {month['days_10k']} |\n"
            report += "\n"

        # Detailed Statistics
        report += "## Detailed Statistics\n\n"

        if 'activity' in stats:
            report += "### Activity\n"
            report += f"- Average: {stats['activity']['avg_steps']:,.0f} steps/day\n"
            report += f"- Peak: {stats['activity']['max_steps']:,.0f} steps\n"
            report += f"- Total Distance: {stats['activity']['total_distance_km']:.1f} km\n\n"

        if 'sleep' in stats:
            report += "### Sleep\n"
            report += f"- Average: {stats['sleep']['avg_hours']:.1f} hours\n"
            report += f"- Good Sleep Days (7+ hrs): {stats['sleep']['good_sleep_days']}\n"
            report += f"- Poor Sleep Days (<6 hrs): {stats['sleep']['poor_sleep_days']}\n\n"

        if 'workouts' in stats:
            report += "### Workouts\n"
            report += f"- Total: {stats['workouts']['total_count']} workouts\n"
            report += f"- Total Duration: {stats['workouts']['total_minutes']:.0f} minutes\n"
            report += f"- Average Duration: {stats['workouts']['avg_duration']:.0f} minutes\n"
            report += f"- Workout Types: {stats['workouts']['unique_types']}\n\n"

        if 'heart_health' in stats:
            report += "### Heart Health\n"
            report += f"- Average Resting HR: {stats['heart_health']['avg_resting_hr']:.0f} bpm\n"
            if stats['heart_health']['avg_hrv'] > 0:
                report += f"- Average HRV: {stats['heart_health']['avg_hrv']:.0f} ms\n"
            report += "\n"

        # Areas for Improvement
        if insights['lowlights']:
            report += "## Areas for Improvement\n\n"
            for insight in insights['lowlights']:
                report += f"### {insight['title']}\n"
                report += f"{insight['description']}\n\n"

        # Patterns & Insights
        if insights['patterns']:
            report += "## Patterns Discovered\n\n"
            for insight in insights['patterns']:
                report += f"### {insight['title']}\n"
                report += f"{insight['description']}\n\n"

        # Recommendations
        if insights['recommendations']:
            report += "## Recommendations for Next Quarter\n\n"
            for i, insight in enumerate(insights['recommendations'], 1):
                report += f"{i}. **{insight['title']}**: {insight['description']}\n"
            report += "\n"

        return report

    def _format_text_report(self, year: int, quarter: int, stats: dict, insights: dict, monthly_stats: list) -> str:
        """Format report as plain text."""
        quarter_name = f"Q{quarter} {year}"
        start_date, end_date = self.get_quarter_dates(year, quarter)

        report = f"PERSONAL DATA REFLECTION: {quarter_name}\n"
        report += f"Period: {start_date} to {end_date}\n"
        report += "=" * 60 + "\n\n"

        # Executive Summary
        report += "EXECUTIVE SUMMARY\n"
        report += "-" * 60 + "\n"
        if 'activity' in stats:
            report += f"Average Daily Steps: {stats['activity']['avg_steps']:,.0f}\n"
            report += f"Days with 10K+ Steps: {stats['activity']['days_over_10k']}\n"
        if 'sleep' in stats:
            report += f"Average Sleep: {stats['sleep']['avg_hours']:.1f} hours\n"
        if 'workouts' in stats:
            report += f"Total Workouts: {stats['workouts']['total_count']}\n"
            report += f"Workout Days: {stats['workouts']['workout_days']}\n"
        report += "\n"

        # Highlights
        if insights['highlights']:
            report += "HIGHLIGHTS\n"
            report += "-" * 60 + "\n"
            for insight in insights['highlights']:
                report += f"\n{insight['title']}\n"
                report += f"{insight['description']}\n"
            report += "\n"

        # Monthly Breakdown
        if monthly_stats:
            report += "MONTHLY BREAKDOWN\n"
            report += "-" * 60 + "\n"
            for month in monthly_stats:
                report += f"\n{month['month_name']}:\n"
                report += f"  Steps: {month['avg_steps']:,.0f} avg | Sleep: {month['avg_sleep']:.1f}h | Exercise: {month['avg_exercise']:.0f}m\n"
            report += "\n"

        # Recommendations
        if insights['recommendations']:
            report += "RECOMMENDATIONS FOR NEXT QUARTER\n"
            report += "-" * 60 + "\n"
            for i, insight in enumerate(insights['recommendations'], 1):
                report += f"\n{i}. {insight['title']}\n"
                report += f"   {insight['description']}\n"
            report += "\n"

        return report

    def save_report(self, year: int, quarter: int, output_path: str, format: str = 'markdown') -> str:
        """Generate and save quarterly report to a file."""
        from pathlib import Path

        report = self.generate_report(year, quarter, format)

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w') as f:
            f.write(report)

        return str(output_file)
