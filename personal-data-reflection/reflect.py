#!/usr/bin/env python3
"""Personal Data Reflection Tool - CLI Interface."""

import argparse
import sys
from pathlib import Path
from datetime import datetime

from reflector.database import ReflectionDB
from reflector.importers import HealthImporter, StravaImporter
from reflector.analysis import CorrelationAnalyzer, InsightGenerator
from reflector.reports import MonthlyReportGenerator
from reflector.dashboard import create_app


def import_health_data(args):
    """Import Apple Health data."""
    data_path = Path(args.path)

    if not data_path.exists():
        print(f"Error: Path not found: {data_path}")
        return 1

    print(f"Importing Apple Health data from {data_path}...")

    with ReflectionDB(args.database) as db:
        importer = HealthImporter(db.con)

        try:
            # Try CSV first
            if data_path.is_file() and data_path.suffix == '.csv':
                counts = importer.import_from_csv(data_path)
            elif data_path.is_file() and data_path.suffix == '.json':
                counts = importer.import_from_json(data_path)
            else:
                # Try to find CSV or JSON in directory
                csv_files = list(data_path.glob('*.csv'))
                json_files = list(data_path.glob('*.json'))

                if csv_files:
                    print(f"Found {len(csv_files)} CSV file(s)")
                    counts = importer.import_from_csv(csv_files[0])
                elif json_files:
                    print(f"Found {len(json_files)} JSON file(s)")
                    counts = importer.import_from_json(json_files[0])
                else:
                    print("Error: No CSV or JSON files found")
                    return 1

            print(f"\nImport complete:")
            print(f"  Health metrics: {counts['health_metrics']} days")
            print(f"  Workouts: {counts['workouts']}")

            # Rebuild daily summary
            print("\nRebuilding daily summary...")
            db.rebuild_daily_summary()
            print("Done!")

        except Exception as e:
            print(f"Error importing data: {e}")
            import traceback
            traceback.print_exc()
            return 1

    return 0


def import_strava_data(args):
    """Import Strava data."""
    data_path = Path(args.path)

    if not data_path.exists():
        print(f"Error: Path not found: {data_path}")
        return 1

    print(f"Importing Strava data from {data_path}...")

    with ReflectionDB(args.database) as db:
        importer = StravaImporter(db.con)

        try:
            counts = importer.import_from_directory(data_path)

            print(f"\nImport complete:")
            print(f"  Activities: {counts['activities']}")
            print(f"  Workouts: {counts['workouts']}")

            # Rebuild daily summary
            print("\nRebuilding daily summary...")
            db.rebuild_daily_summary()
            print("Done!")

        except Exception as e:
            print(f"Error importing data: {e}")
            import traceback
            traceback.print_exc()
            return 1

    return 0


def analyze_data(args):
    """Run correlation analysis."""
    print("Running correlation analysis...")

    with ReflectionDB(args.database) as db:
        analyzer = CorrelationAnalyzer(db.con)

        # Get date range
        min_date, max_date = db.get_date_range()
        if not min_date or not max_date:
            print("Error: No data available")
            return 1

        print(f"Analyzing data from {min_date} to {max_date}")

        # Compute correlations
        correlations = analyzer.find_strongest_correlations(
            str(min_date),
            str(max_date),
            min_correlation=0.3,
            limit=10
        )

        if not correlations:
            print("No significant correlations found")
            return 0

        print(f"\nFound {len(correlations)} significant correlations:\n")

        for i, corr in enumerate(correlations, 1):
            print(f"{i}. {corr['metric_a']} ↔ {corr['metric_b']}")
            print(f"   Correlation: {corr['correlation']:.3f} ({corr['strength']})")
            print(f"   {corr['description']}")
            print()

    return 0


def generate_report(args):
    """Generate a monthly reflection report."""
    if args.month:
        # Parse YYYY-MM format
        try:
            year, month = map(int, args.month.split('-'))
        except ValueError:
            print("Error: Month should be in YYYY-MM format (e.g., 2024-01)")
            return 1
    else:
        # Use current month
        now = datetime.now()
        year = now.year
        month = now.month

    print(f"Generating report for {year}-{month:02d}...")

    with ReflectionDB(args.database) as db:
        generator = MonthlyReportGenerator(db.con)

        try:
            if args.output:
                # Save to file
                output_path = Path(args.output)
                output_format = 'markdown' if output_path.suffix in ['.md', '.markdown'] else 'text'

                saved_path = generator.save_report(year, month, output_path, output_format)
                print(f"\nReport saved to: {saved_path}")
            else:
                # Print to stdout
                report = generator.generate_report(year, month, 'text')
                print("\n" + report)

        except Exception as e:
            print(f"Error generating report: {e}")
            import traceback
            traceback.print_exc()
            return 1

    return 0


def serve_dashboard(args):
    """Start the web dashboard."""
    print(f"Starting dashboard on http://localhost:{args.port}")
    print("Press Ctrl+C to stop")

    try:
        app = create_app(args.database)
        app.run(host='127.0.0.1', port=args.port, debug=args.debug)
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Personal Data Reflection Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import data
  %(prog)s import-health /path/to/apple-health-export/
  %(prog)s import-strava /path/to/strava-export/

  # Analyze and generate reports
  %(prog)s analyze
  %(prog)s report --month 2024-01
  %(prog)s report --output reports/january-2024.md

  # Start dashboard
  %(prog)s serve
  %(prog)s serve --port 8080
        """
    )

    parser.add_argument(
        '--database',
        default='./data/reflection.duckdb',
        help='Path to DuckDB database (default: ./data/reflection.duckdb)'
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Import health data
    import_health_parser = subparsers.add_parser(
        'import-health',
        help='Import Apple Health data'
    )
    import_health_parser.add_argument(
        'path',
        help='Path to Apple Health export (CSV or JSON file, or directory)'
    )

    # Import Strava data
    import_strava_parser = subparsers.add_parser(
        'import-strava',
        help='Import Strava data'
    )
    import_strava_parser.add_argument(
        'path',
        help='Path to Strava export directory'
    )

    # Analyze
    analyze_parser = subparsers.add_parser(
        'analyze',
        help='Run correlation analysis'
    )

    # Generate report
    report_parser = subparsers.add_parser(
        'report',
        help='Generate monthly reflection report'
    )
    report_parser.add_argument(
        '--month',
        help='Month to generate report for (YYYY-MM format, default: current month)'
    )
    report_parser.add_argument(
        '--output',
        help='Output file path (default: print to stdout)'
    )

    # Serve dashboard
    serve_parser = subparsers.add_parser(
        'serve',
        help='Start web dashboard'
    )
    serve_parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port to run server on (default: 5000)'
    )
    serve_parser.add_argument(
        '--debug',
        action='store_true',
        help='Run in debug mode'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Route to appropriate handler
    handlers = {
        'import-health': import_health_data,
        'import-strava': import_strava_data,
        'analyze': analyze_data,
        'report': generate_report,
        'serve': serve_dashboard,
    }

    handler = handlers.get(args.command)
    if handler:
        return handler(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
