"""Generate insights from patterns and correlations."""

import duckdb
from typing import List, Dict
from datetime import datetime, timedelta
from .correlations import CorrelationAnalyzer
from .patterns import PatternDetector


class InsightGenerator:
    """Generate actionable insights from personal data."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """Initialize insight generator."""
        self.con = db_connection
        self.corr_analyzer = CorrelationAnalyzer(db_connection)
        self.pattern_detector = PatternDetector(db_connection)

    def generate_monthly_insights(
        self,
        year: int,
        month: int
    ) -> Dict[str, List[Dict]]:
        """Generate comprehensive insights for a month.

        Returns insights categorized as:
        - highlights: Positive achievements
        - lowlights: Areas needing improvement
        - patterns: Behavioral patterns discovered
        - recommendations: Suggested behavior changes
        """
        # Calculate date range
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year}-12-31"
        else:
            from datetime import date
            next_month = date(year, month + 1, 1)
            end_date = str(next_month - timedelta(days=1))

        insights = {
            'highlights': [],
            'lowlights': [],
            'patterns': [],
            'recommendations': []
        }

        # Generate highlights
        insights['highlights'].extend(self._generate_highlights(start_date, end_date))

        # Generate lowlights
        insights['lowlights'].extend(self._generate_lowlights(start_date, end_date))

        # Generate pattern insights
        insights['patterns'].extend(self._generate_pattern_insights(start_date, end_date))

        # Generate recommendations
        insights['recommendations'].extend(
            self._generate_recommendations(start_date, end_date, insights)
        )

        # Save insights to database
        for category, category_insights in insights.items():
            for insight in category_insights:
                self._save_insight(category, insight, start_date, end_date)

        return insights

    def _generate_highlights(self, start_date: str, end_date: str) -> List[Dict]:
        """Generate positive highlights."""
        highlights = []

        # Check for good days
        good_days = self.pattern_detector.find_good_days(start_date, end_date)
        if good_days:
            percentage = (len(good_days) / 30) * 100  # Rough approximation
            highlights.append({
                'title': f"{len(good_days)} Excellent Days",
                'description': f"You had {len(good_days)} days meeting all health goals (8K+ steps, 7+ hrs sleep, 20+ min exercise). That's {percentage:.0f}% of the month!",
                'confidence': 1.0,
                'metrics': ['steps', 'sleep_hours', 'exercise_minutes'],
                'data': {'good_day_count': len(good_days)}
            })

        # Check for workout streaks
        workout_streaks = self.pattern_detector.detect_streaks(
            start_date, end_date, 'exercise_minutes', 20, '>='
        )
        if workout_streaks:
            longest = max(workout_streaks, key=lambda x: x['length_days'])
            highlights.append({
                'title': f"{longest['length_days']}-Day Workout Streak",
                'description': f"Your longest workout streak was {longest['length_days']} consecutive days with 20+ minutes of exercise (from {longest['start_date']} to {longest['end_date']}).",
                'confidence': 1.0,
                'metrics': ['exercise_minutes'],
                'data': longest
            })

        # Check for step achievements
        result = self.con.execute("""
            SELECT
                MAX(steps) as max_steps,
                AVG(steps) as avg_steps,
                COUNT(CASE WHEN steps >= 10000 THEN 1 END) as days_over_10k
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result and result[2] > 0:
            # Get date of max steps
            max_date_res = self.con.execute("""
                SELECT date FROM health_metrics 
                WHERE date BETWEEN ? AND ? AND steps = ?
                LIMIT 1
            """, [start_date, end_date, result[0]]).fetchone()
            
            date_str = ""
            if max_date_res:
                d = max_date_res[0]
                date_str = f" on {d.strftime('%b %d')}"

            highlights.append({
                'title': f"{result[2]} Days with 10K+ Steps",
                'description': f"You hit 10,000+ steps on {result[2]} days this month. Your highest was {result[0]:,.0f} steps{date_str}!",
                'confidence': 1.0,
                'metrics': ['steps'],
                'data': {
                    'days_over_10k': result[2],
                    'max_steps': result[0],
                    'avg_steps': result[1]
                }
            })

        # Check for workout variety
        workout_patterns = self.pattern_detector.find_workout_patterns(start_date, end_date)
        if len(workout_patterns['workout_types']) >= 3:
            types = [w['type'] for w in workout_patterns['workout_types'][:3]]
            highlights.append({
                'title': "Diverse Workout Routine",
                'description': f"You engaged in {len(workout_patterns['workout_types'])} different workout types this month, including {', '.join(types)}. Variety helps prevent burnout!",
                'confidence': 0.8,
                'metrics': ['workouts'],
                'data': {'workout_types': workout_patterns['workout_types']}
            })

        return highlights

    def _generate_lowlights(self, start_date: str, end_date: str) -> List[Dict]:
        """Generate areas needing improvement."""
        lowlights = []

        # Check for bad days
        bad_days = self.pattern_detector.find_bad_days(start_date, end_date)
        if len(bad_days) > 5:  # More than 5 bad days
            lowlights.append({
                'title': f"{len(bad_days)} Challenging Days",
                'description': f"You had {len(bad_days)} days that didn't meet health goals. Common issues: {', '.join(set([issue for day in bad_days[:5] for issue in day['issues']]))}.",
                'confidence': 1.0,
                'metrics': ['steps', 'sleep_hours', 'exercise_minutes'],
                'data': {'bad_day_count': len(bad_days), 'sample_issues': bad_days[:5]}
            })

        # Check for low sleep
        result = self.con.execute("""
            SELECT
                AVG(sleep_hours) as avg_sleep,
                COUNT(CASE WHEN sleep_hours < 6 THEN 1 END) as poor_sleep_days,
                MIN(sleep_hours) as worst_sleep
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND sleep_hours IS NOT NULL
        """, [start_date, end_date]).fetchone()

        if result and result[1] > 7:  # More than 7 days with poor sleep
            lowlights.append({
                'title': "Sleep Needs Attention",
                'description': f"You had {result[1]} days with less than 6 hours of sleep. Average sleep was {result[0]:.1f} hours. Quality sleep is crucial for recovery and performance.",
                'confidence': 1.0,
                'metrics': ['sleep_hours'],
                'data': {
                    'poor_sleep_days': result[1],
                    'avg_sleep': result[0],
                    'worst_sleep': result[2]
                }
            })

        # Check for sedentary days
        result = self.con.execute("""
            SELECT COUNT(*) as sedentary_days
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND steps < 5000
        """, [start_date, end_date]).fetchone()

        if result and result[0] > 5:
            lowlights.append({
                'title': f"{result[0]} Very Sedentary Days",
                'description': f"You had {result[0]} days with fewer than 5,000 steps. Consider setting reminders to move throughout the day.",
                'confidence': 1.0,
                'metrics': ['steps'],
                'data': {'sedentary_days': result[0]}
            })

        # Check for workout gaps
        result = self.con.execute("""
            WITH workout_days AS (
                SELECT DISTINCT DATE(start_time) as workout_date
                FROM workouts
                WHERE DATE(start_time) BETWEEN ? AND ?
            )
            SELECT
                (SELECT COUNT(DISTINCT date) FROM health_metrics WHERE date BETWEEN ? AND ?) -
                COUNT(*) as days_without_workout
            FROM workout_days
        """, [start_date, end_date, start_date, end_date]).fetchone()

        if result and result[0] > 20:  # More than 20 days without workouts
            lowlights.append({
                'title': "Inconsistent Workout Routine",
                'description': f"You had {result[0]} days without recorded workouts. Building consistency is key to long-term health.",
                'confidence': 0.9,
                'metrics': ['workouts'],
                'data': {'days_without_workout': result[0]}
            })

        return lowlights

    def _generate_pattern_insights(self, start_date: str, end_date: str) -> List[Dict]:
        """Generate insights about behavioral patterns."""
        insights = []

        # Day of week patterns
        dow_patterns = self.pattern_detector.analyze_day_of_week_patterns(
            start_date, end_date
        )

        if dow_patterns:
            # Find most active day
            most_active_day = max(
                dow_patterns.items(),
                key=lambda x: x[1]['avg_steps']
            )
            least_active_day = min(
                dow_patterns.items(),
                key=lambda x: x[1]['avg_steps']
            )

            insights.append({
                'title': f"{most_active_day[0]} is Your Most Active Day",
                'description': f"You average {most_active_day[1]['avg_steps']:,.0f} steps on {most_active_day[0]}s vs {least_active_day[1]['avg_steps']:,.0f} on {least_active_day[0]}s. Weekend vs weekday patterns detected.",
                'confidence': 0.9,
                'metrics': ['steps'],
                'data': {'dow_patterns': dow_patterns}
            })

        # Correlation insights
        correlations = self.corr_analyzer.compute_correlations(start_date, end_date)
        strong_corrs = [c for c in correlations if abs(c['correlation']) > 0.5]

        for corr in strong_corrs[:2]:  # Top 2 strong correlations
            insights.append({
                'title': f"Connection: {corr['metric_a'].replace('_', ' ').title()} & {corr['metric_b'].replace('_', ' ').title()}",
                'description': corr['description'],
                'confidence': min(abs(corr['correlation']), 1.0),
                'metrics': [corr['metric_a'], corr['metric_b']],
                'data': corr
            })

        # Check for workout timing patterns
        workout_patterns = self.pattern_detector.find_workout_patterns(start_date, end_date)
        if workout_patterns['day_of_week_frequency']:
            best_workout_days = sorted(
                workout_patterns['day_of_week_frequency'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:2]

            insights.append({
                'title': f"You Prefer {best_workout_days[0][0]} Workouts",
                'description': f"Most of your workouts happen on {best_workout_days[0][0]}s ({best_workout_days[0][1]} workouts). Consider this when planning your week.",
                'confidence': 0.8,
                'metrics': ['workouts'],
                'data': {'workout_day_preferences': workout_patterns['day_of_week_frequency']}
            })

        return insights

    def _generate_recommendations(
        self,
        start_date: str,
        end_date: str,
        insights: Dict
    ) -> List[Dict]:
        """Generate actionable recommendations."""
        recommendations = []

        # Check if sleep correlates with performance
        sleep_step_corr = self.corr_analyzer._compute_correlation(
            'sleep_hours', 'steps', start_date, end_date
        )

        if sleep_step_corr and sleep_step_corr['correlation'] > 0.3:
            avg_sleep = sleep_step_corr['avg_a']
            if avg_sleep < 7:
                recommendations.append({
                    'title': "Prioritize Sleep for Better Activity",
                    'description': f"Your data shows a positive correlation between sleep and daily steps. Aim for 7-8 hours of sleep to boost your activity levels (current average: {avg_sleep:.1f}h).",
                    'confidence': 0.85,
                    'metrics': ['sleep_hours', 'steps'],
                    'data': sleep_step_corr
                })

        # Check workout consistency
        result = self.con.execute("""
            SELECT COUNT(DISTINCT DATE(start_time)) as workout_days
            FROM workouts
            WHERE DATE(start_time) BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result and result[0] < 12:  # Less than 3x per week
            recommendations.append({
                'title': "Build Workout Consistency",
                'description': f"You worked out {result[0]} days this month. Try scheduling 3-4 specific workout days each week to build a sustainable routine.",
                'confidence': 0.9,
                'metrics': ['workouts'],
                'data': {'workout_days': result[0]}
            })

        # Step goal recommendation
        result = self.con.execute("""
            SELECT
                AVG(steps) as avg_steps,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY steps) as p75_steps
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
              AND steps IS NOT NULL
        """, [start_date, end_date]).fetchone()

        if result and result[0] < 8000:
            target = int((result[1] // 1000 + 1) * 1000)  # Round up to nearest 1000
            recommendations.append({
                'title': f"Aim for {target:,} Daily Steps",
                'description': f"Your average is {result[0]:,.0f} steps. Try targeting {target:,} steps daily - you've already hit this on your best days!",
                'confidence': 0.8,
                'metrics': ['steps'],
                'data': {'current_avg': result[0], 'suggested_target': target}
            })

        # Weekend activity recommendation
        result = self.con.execute("""
            SELECT
                AVG(CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN steps END) as weekend_avg,
                AVG(CASE WHEN DAYOFWEEK(date) NOT IN (1, 7) THEN steps END) as weekday_avg
            FROM health_metrics
            WHERE date BETWEEN ? AND ?
        """, [start_date, end_date]).fetchone()

        if result and result[0] and result[1] and result[0] < result[1] * 0.7:
            recommendations.append({
                'title': "Stay Active on Weekends",
                'description': f"Your weekend activity ({result[0]:,.0f} avg steps) is significantly lower than weekdays ({result[1]:,.0f} steps). Plan active weekend activities to maintain momentum.",
                'confidence': 0.85,
                'metrics': ['steps'],
                'data': {'weekend_avg': result[0], 'weekday_avg': result[1]}
            })

        return recommendations

    def _save_insight(
        self,
        category: str,
        insight: Dict,
        start_date: str,
        end_date: str
    ):
        """Save insight to database."""
        try:
            import json
            self.con.execute("""
                INSERT INTO insights (
                    category, title, description, metrics_involved,
                    confidence_score, period_start, period_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                category,
                insight['title'],
                insight['description'],
                json.dumps(insight.get('metrics', [])),
                insight.get('confidence', 0.5),
                start_date,
                end_date
            ])
        except Exception as e:
            print(f"Error saving insight: {e}")
