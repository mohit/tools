"""Flask web application for personal data reflection dashboard."""

from flask import Flask, render_template, jsonify, request
from pathlib import Path
from datetime import datetime, timedelta
import json

from reflector.database import ReflectionDB
from reflector.analysis import CorrelationAnalyzer, PatternDetector, InsightGenerator


def create_app(db_path: str = "./data/reflection.duckdb"):
    """Create and configure Flask app."""
    app = Flask(__name__)
    app.config['DB_PATH'] = db_path

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

        return jsonify({
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
        })

    @app.route('/api/monthly/<int:year>/<int:month>')
    def api_monthly(year, month):
        """Get monthly statistics and insights."""
        db = get_db()
        stats = db.get_monthly_stats(year, month)

        # Generate insights
        insight_gen = InsightGenerator(db.con)
        insights = insight_gen.generate_monthly_insights(year, month)

        db.close()

        return jsonify({
            'year': year,
            'month': month,
            'stats': stats,
            'insights': insights
        })

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

        return jsonify({
            'metrics': metrics_data,
            'workouts': workouts_data
        })

    @app.route('/api/correlations/<start_date>/<end_date>')
    def api_correlations(start_date, end_date):
        """Get correlation analysis."""
        db = get_db()
        analyzer = CorrelationAnalyzer(db.con)
        correlations = analyzer.compute_correlations(start_date, end_date)
        db.close()

        return jsonify(correlations)

    @app.route('/api/patterns/<start_date>/<end_date>')
    def api_patterns(start_date, end_date):
        """Get pattern analysis."""
        db = get_db()
        detector = PatternDetector(db.con)

        patterns = {
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

        return jsonify(patterns)

    @app.route('/api/insights/<int:year>/<int:month>')
    def api_insights(year, month):
        """Get generated insights for a month."""
        db = get_db()
        insight_gen = InsightGenerator(db.con)
        insights = insight_gen.generate_monthly_insights(year, month)
        db.close()

        return jsonify(insights)

    return app


def run_server(db_path: str = "./data/reflection.duckdb", port: int = 5000, debug: bool = False):
    """Run the Flask development server."""
    app = create_app(db_path)
    app.run(host='127.0.0.1', port=port, debug=debug)
