"""Flask web application for personal data reflection dashboard."""

from flask import Flask, render_template, jsonify, request
from pathlib import Path
from datetime import datetime, timedelta
import json
import math

from reflector.database import ReflectionDB
from reflector.analysis import CorrelationAnalyzer, PatternDetector, InsightGenerator
from reflector.reports import QuarterlyReportGenerator
from reflector.data_retention import DataRetentionManager


def create_app(db_path: str = "./data/reflection.duckdb"):
    """Create and configure Flask app."""
    app = Flask(__name__)
    app.config['DB_PATH'] = db_path

    def clean_nan(data):
        """Recursively replace NaN with None for JSON serialization."""
        if isinstance(data, dict):
            return {k: clean_nan(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_nan(v) for v in data]
        elif isinstance(data, float) and math.isnan(data):
            return None
        return data

    def get_db():
        """Get database connection."""
        return ReflectionDB(app.config['DB_PATH'])

    @app.route('/')
    def index():
        """Main dashboard page."""
        return render_template('index.html')

    @app.route('/api/overview')
    def api_overview():
        """Get overview statistics."""
        db = get_db()
        min_date, max_date = db.get_date_range()

        if not min_date or not max_date:
            return jsonify({
                'error': 'No data available',
                'has_data': False
            })

        # Get current month stats
        now = datetime.now()
        current_month_stats = db.get_monthly_stats(now.year, now.month)

        # Get previous month for comparison
        prev_month = now.replace(day=1) - timedelta(days=1)
        prev_month_stats = db.get_monthly_stats(prev_month.year, prev_month.month)

        db.close()

        return jsonify(clean_nan({
            'has_data': True,
            'date_range': {
                'start': str(min_date),
                'end': str(max_date)
            },
            'current_month': {
                'year': now.year,
                'month': now.month,
                'stats': current_month_stats
            },
            'previous_month': {
                'year': prev_month.year,
                'month': prev_month.month,
                'stats': prev_month_stats
            }
        }))

    @app.route('/api/monthly/<int:year>/<int:month>')
    def api_monthly(year, month):
        """Get monthly statistics and insights."""
        db = get_db()
        stats = db.get_monthly_stats(year, month)

        # Generate insights
        insight_gen = InsightGenerator(db.con)
        insights = insight_gen.generate_monthly_insights(year, month)

        db.close()

        return jsonify(clean_nan({
            'year': year,
            'month': month,
            'stats': stats,
            'insights': insights
        }))

    @app.route('/api/daily/<start_date>/<end_date>')
    def api_daily(start_date, end_date):
        """Get daily data for date range."""
        db = get_db()
        metrics = db.get_health_metrics(start_date, end_date)
        workouts = db.get_workouts(start_date, end_date)
        db.close()

        # Convert to JSON-serializable format
        metrics_data = [
            {
                'date': str(row[0]),
                'steps': row[1],
                'distance_km': row[2],
                'active_energy': row[3],
                'resting_energy': row[4],
                'exercise_minutes': row[5],
                'flights_climbed': row[6],
                'resting_hr': row[7],
                'walking_hr': row[8],
                'hrv': row[9],
                'sleep_hours': row[10],
                'sleep_quality': row[11],
                'body_mass': row[12]
            }
            for row in metrics
        ]

        workouts_data = [
            {
                'id': row[0],
                'source': row[1],
                'type': row[2],
                'start_time': str(row[3]),
                'end_time': str(row[4]),
                'duration_minutes': row[5],
                'distance_km': row[6],
                'elevation_gain_m': row[7],
                'calories': row[8],
                'avg_hr': row[9],
                'max_hr': row[10]
            }
            for row in workouts
        ]

        return jsonify(clean_nan({
            'metrics': metrics_data,
            'workouts': workouts_data
        }))

    @app.route('/api/correlations/<start_date>/<end_date>')
    def api_correlations(start_date, end_date):
        """Get correlation analysis."""
        db = get_db()
        analyzer = CorrelationAnalyzer(db.con)
        correlations = analyzer.compute_correlations(start_date, end_date)
        db.close()

        return jsonify(clean_nan(correlations))

    @app.route('/api/patterns/<start_date>/<end_date>')
    def api_patterns(start_date, end_date):
        """Get pattern analysis."""
        db = get_db()
        detector = PatternDetector(db.con)

        patterns = {
            'goals': detector.get_default_criteria(),
            'good_days': detector.find_good_days(start_date, end_date),
            'bad_days': detector.find_bad_days(start_date, end_date),
            'day_of_week': detector.analyze_day_of_week_patterns(start_date, end_date),
            'workouts': detector.find_workout_patterns(start_date, end_date),
            'step_streaks': detector.detect_streaks(start_date, end_date, 'steps', 8000, '>='),
            'sleep_anomalies': detector.detect_anomalies(start_date, end_date, 'sleep_hours', 2.0)
        }

        db.close()

        # Convert dates to strings for JSON serialization
        for day in patterns['good_days']:
            day['date'] = str(day['date'])
        for day in patterns['bad_days']:
            day['date'] = str(day['date'])
        for streak in patterns['step_streaks']:
            streak['start_date'] = str(streak['start_date'])
            streak['end_date'] = str(streak['end_date'])
        for anomaly in patterns['sleep_anomalies']:
            anomaly['date'] = str(anomaly['date'])

        return jsonify(clean_nan(patterns))

    @app.route('/api/summary')
    def api_summary():
        """Get aggregated summary for a specific period."""
        period = request.args.get('period', 'month')
        ref_date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        
        try:
            ref_date = datetime.strptime(ref_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': 'Invalid date format'}), 400

        # Calculate date ranges
        if period == 'week':
            # Start of week (Monday)
            start_date = ref_date - timedelta(days=ref_date.weekday())
            end_date = start_date + timedelta(days=6)
            # Previous week
            prev_start = start_date - timedelta(days=7)
            prev_end = end_date - timedelta(days=7)
            
        elif period == 'month':
            # Start of month
            start_date = ref_date.replace(day=1)
            # End of month
            if start_date.month == 12:
                end_date = start_date.replace(year=start_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_date = start_date.replace(month=start_date.month + 1, day=1) - timedelta(days=1)
            
            # Previous month
            prev_end = start_date - timedelta(days=1)
            prev_start = prev_end.replace(day=1)

        elif period == 'quarter':
            # Start of quarter (Jan, Apr, Jul, Oct)
            quarter = (ref_date.month - 1) // 3 + 1
            start_month = (quarter - 1) * 3 + 1
            start_date = ref_date.replace(month=start_month, day=1)
            
            # End of quarter
            if start_month + 3 > 12:
                end_date = ref_date.replace(year=ref_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_date = ref_date.replace(month=start_month + 3, day=1) - timedelta(days=1)
                
            # Previous quarter
            prev_end = start_date - timedelta(days=1)
            # Calculate start of previous quarter
            prev_quarter_end_month = prev_end.month
            prev_quarter_start_month = prev_quarter_end_month - ((prev_quarter_end_month - 1) % 3)
            prev_start = prev_end.replace(month=prev_quarter_start_month, day=1)

        elif period == 'year':
            start_date = ref_date.replace(month=1, day=1)
            end_date = ref_date.replace(month=12, day=31)
            
            prev_start = start_date.replace(year=start_date.year - 1)
            prev_end = end_date.replace(year=end_date.year - 1)
        
        else:
            return jsonify({'error': 'Invalid period'}), 400

        # Handle "Period-to-Date" comparison if the period includes today
        today = datetime.now().date()
        current_query_end = end_date.date()
        prev_query_end = prev_end.date()

        if start_date.date() <= today <= end_date.date():
            # Current period is active. We should compare "to-date"
            days_elapsed = (today - start_date.date()).days
            
            current_query_end = today
            # Clamp previous end to same duration, but do not exceed the previous period end
            prev_query_end = min(
                prev_start.date() + timedelta(days=days_elapsed),
                prev_end.date()
            )

        # Fetch data
        db = get_db()
        current_stats = db.get_aggregated_stats(str(start_date.date()), str(current_query_end))
        previous_stats = db.get_aggregated_stats(str(prev_start.date()), str(prev_query_end))
        db.close()

        return jsonify(clean_nan({
            'period': period,
            'ref_date': ref_date_str,
            'current': {
                'start_date': str(start_date.date()),
                'end_date': str(current_query_end),
                'stats': current_stats
            },
            'previous': {
                'start_date': str(prev_start.date()),
                'end_date': str(prev_query_end),
                'stats': previous_stats
            }
        }))

    @app.route('/api/insights/<int:year>/<int:month>')
    def api_insights(year, month):
        """Get generated insights for a month."""
        db = get_db()
        insight_gen = InsightGenerator(db.con)
        insights = insight_gen.generate_monthly_insights(year, month)
        db.close()

        return jsonify(clean_nan(insights))

    @app.route('/api/goals', methods=['GET'])
    def get_goals():
        """Get user goals."""
        db = get_db()
        goals = db.get_goals()
        db.close()
        return jsonify(goals)

    @app.route('/api/goals', methods=['PUT'])
    def update_goal():
        """Update a goal."""
        data = request.json
        if not data or 'metric' not in data or 'target' not in data:
            return jsonify({'error': 'Missing metric or target'}), 400

        db = get_db()
        db.update_goal(data['metric'], float(data['target']), data.get('period', 'daily'))
        goals = db.get_goals()
        db.close()
        return jsonify(goals)

    @app.route('/api/quarterly/<int:year>/<int:quarter>')
    def api_quarterly(year, quarter):
        """Get quarterly statistics and insights."""
        db = get_db()
        generator = QuarterlyReportGenerator(db.con)

        try:
            start_date, end_date = generator.get_quarter_dates(year, quarter)
            stats = generator._get_quarterly_stats(start_date, end_date)
            insights = generator._generate_quarterly_insights(year, quarter, start_date, end_date)
            monthly_stats = generator._get_monthly_breakdown(year, quarter)

            db.close()

            return jsonify(clean_nan({
                'year': year,
                'quarter': quarter,
                'period': {
                    'start': start_date,
                    'end': end_date
                },
                'stats': stats,
                'insights': insights,
                'monthly_breakdown': monthly_stats
            }))
        except ValueError as e:
            db.close()
            return jsonify({'error': str(e)}), 400

    @app.route('/api/quarterly/current')
    def api_quarterly_current():
        """Get current quarter statistics."""
        now = datetime.now()
        year = now.year
        quarter = (now.month - 1) // 3 + 1

        db = get_db()
        generator = QuarterlyReportGenerator(db.con)

        start_date, end_date = generator.get_quarter_dates(year, quarter)
        stats = generator._get_quarterly_stats(start_date, end_date)
        insights = generator._generate_quarterly_insights(year, quarter, start_date, end_date)
        monthly_stats = generator._get_monthly_breakdown(year, quarter)

        db.close()

        return jsonify(clean_nan({
            'year': year,
            'quarter': quarter,
            'period': {
                'start': start_date,
                'end': end_date
            },
            'stats': stats,
            'insights': insights,
            'monthly_breakdown': monthly_stats
        }))

    @app.route('/api/retention/stats')
    def api_retention_stats():
        """Get data retention statistics."""
        db = get_db()
        manager = DataRetentionManager(db.con)
        stats = manager.get_retention_stats()
        db.close()

        return jsonify(clean_nan(stats))

    @app.route('/api/quarterly/report/<int:year>/<int:quarter>')
    def api_quarterly_report(year, quarter):
        """Get quarterly report in markdown or text format."""
        format = request.args.get('format', 'markdown')

        db = get_db()
        generator = QuarterlyReportGenerator(db.con)

        try:
            report = generator.generate_report(year, quarter, format)
            db.close()

            return jsonify({
                'year': year,
                'quarter': quarter,
                'format': format,
                'report': report
            })
        except ValueError as e:
            db.close()
            return jsonify({'error': str(e)}), 400

    return app


def run_server(
    db_path: str = "./data/reflection.duckdb",
    port: int = 5000,
    debug: bool = False,
    host: str = "127.0.0.1"
):
    """Run the Flask development server."""
    app = create_app(db_path)
    app.run(host=host, port=port, debug=debug)
