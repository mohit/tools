#!/usr/bin/env python3
"""
Apple Health Data Parser

Parse exported Apple Health XML data and convert to useful formats (CSV, JSON).
"""

import xml.etree.ElementTree as ET
import csv
import json
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict


class HealthDataParser:
    """Parser for Apple Health export XML data."""

    def __init__(self, xml_file):
        self.xml_file = Path(xml_file)
        self.tree = None
        self.root = None

    def parse(self):
        """Parse the XML file."""
        print(f"Parsing {self.xml_file}...")
        print("This may take a while for large exports...")

        try:
            self.tree = ET.parse(self.xml_file)
            self.root = self.tree.getroot()
            print("Parsing complete!")
            return True
        except (ET.ParseError, OSError) as e:
            print(f"Error parsing XML: {e}", file=sys.stderr)
            return False

    def get_record_types(self):
        """Get all unique record types in the export."""
        if self.root is None:
            return []

        record_types = set()
        for record in self.root.findall('.//Record'):
            record_type = record.get('type', '')
            if record_type:
                record_types.add(record_type)

        return sorted(record_types)

    def get_workout_types(self):
        """Get all unique workout types in the export."""
        if self.root is None:
            return []

        workout_types = set()
        for workout in self.root.findall('.//Workout'):
            workout_type = workout.get('workoutActivityType', '')
            if workout_type:
                workout_types.add(workout_type)

        return sorted(workout_types)

    def export_records_to_csv(self, output_file, record_type=None, start_date=None, end_date=None):
        """
        Export health records to CSV.

        Args:
            output_file: Output CSV file path
            record_type: Filter by specific record type (e.g., 'HKQuantityTypeIdentifierStepCount')
            start_date: Filter records after this date (YYYY-MM-DD)
            end_date: Filter records before this date (YYYY-MM-DD)
        """
        if self.root is None:
            print("Error: XML not parsed. Call parse() first.", file=sys.stderr)
            return False

        records = []
        record_count = 0

        for record in self.root.findall('.//Record'):
            # Filter by type if specified
            if record_type and record.get('type') != record_type:
                continue

            # Filter by date if specified
            if start_date or end_date:
                record_date_str = record.get('startDate', '')
                if record_date_str:
                    try:
                        record_date = datetime.fromisoformat(record_date_str.replace('Z', '+00:00'))
                        # Convert the filter dates to offset-aware by adding UTC timezone
                        from datetime import timezone
                        if start_date:
                            start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
                            if record_date < start_dt:
                                continue
                        if end_date:
                            end_dt = datetime.fromisoformat(end_date + 'T23:59:59').replace(tzinfo=timezone.utc)
                            if record_date > end_dt:
                                continue
                    except ValueError:
                        continue

            # Extract record data
            record_data = {
                'type': record.get('type', ''),
                'sourceName': record.get('sourceName', ''),
                'sourceVersion': record.get('sourceVersion', ''),
                'unit': record.get('unit', ''),
                'value': record.get('value', ''),
                'startDate': record.get('startDate', ''),
                'endDate': record.get('endDate', ''),
                'creationDate': record.get('creationDate', ''),
            }

            # Add metadata if present
            metadata_elements = record.findall('.//MetadataEntry')
            for meta in metadata_elements:
                key = meta.get('key', '').replace('HKMetadataKey', '')
                value = meta.get('value', '')
                record_data[f'metadata_{key}'] = value

            records.append(record_data)
            record_count += 1

            if record_count % 10000 == 0:
                print(f"  Processed {record_count} records...")

        if not records:
            print("No matching records found.", file=sys.stderr)
            return False

        # Write to CSV
        output_path = Path(output_file)
        print(f"\nWriting {len(records)} records to {output_path}...")

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            # Get all unique keys across all records
            all_keys = set()
            for record in records:
                all_keys.update(record.keys())
            fieldnames = sorted(all_keys)

            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"Export complete: {output_path}")
        return True

    def export_workouts_to_csv(self, output_file, workout_type=None):
        """Export workout data to CSV."""
        if self.root is None:
            print("Error: XML not parsed. Call parse() first.", file=sys.stderr)
            return False

        workouts = []

        for workout in self.root.findall('.//Workout'):
            # Filter by type if specified
            if workout_type and workout.get('workoutActivityType') != workout_type:
                continue

            workout_data = {
                'workoutActivityType': workout.get('workoutActivityType', ''),
                'duration': workout.get('duration', ''),
                'durationUnit': workout.get('durationUnit', ''),
                'totalDistance': workout.get('totalDistance', ''),
                'totalDistanceUnit': workout.get('totalDistanceUnit', ''),
                'totalEnergyBurned': workout.get('totalEnergyBurned', ''),
                'totalEnergyBurnedUnit': workout.get('totalEnergyBurnedUnit', ''),
                'sourceName': workout.get('sourceName', ''),
                'startDate': workout.get('startDate', ''),
                'endDate': workout.get('endDate', ''),
                'creationDate': workout.get('creationDate', ''),
            }

            # Add metadata if present
            metadata_elements = workout.findall('.//MetadataEntry')
            for meta in metadata_elements:
                key = meta.get('key', '').replace('HKMetadataKey', '')
                value = meta.get('value', '')
                workout_data[f'metadata_{key}'] = value

            # Add workout statistics
            stats = workout.findall('.//WorkoutStatistics')
            for stat in stats:
                stat_type = stat.get('type', '').replace('HKQuantityTypeIdentifier', '')
                stat_sum = stat.get('sum', '')
                stat_unit = stat.get('unit', '')
                if stat_type and stat_sum:
                    workout_data[f'stat_{stat_type}'] = stat_sum
                    workout_data[f'stat_{stat_type}_unit'] = stat_unit

            workouts.append(workout_data)

        if not workouts:
            print("No matching workouts found.", file=sys.stderr)
            return False

        # Write to CSV
        output_path = Path(output_file)
        print(f"Writing {len(workouts)} workouts to {output_path}...")

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            # Get all unique keys
            all_keys = set()
            for workout in workouts:
                all_keys.update(workout.keys())
            fieldnames = sorted(all_keys)

            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(workouts)

        print(f"Export complete: {output_path}")
        return True

    def get_summary_stats(self):
        """Get summary statistics about the health data."""
        if self.root is None:
            return None

        stats = {
            'total_records': len(self.root.findall('.//Record')),
            'total_workouts': len(self.root.findall('.//Workout')),
            'record_types': len(self.get_record_types()),
            'workout_types': len(self.get_workout_types()),
        }

        # Get date range
        all_dates = []
        for record in self.root.findall('.//Record'):
            date_str = record.get('startDate', '')
            if date_str:
                try:
                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    all_dates.append(date)
                except ValueError:
                    pass

        if all_dates:
            stats['earliest_date'] = min(all_dates).strftime('%Y-%m-%d')
            stats['latest_date'] = max(all_dates).strftime('%Y-%m-%d')

        return stats


def main():
    """Main entry point for the health parser tool."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Parse Apple Health export data and convert to CSV/JSON'
    )
    parser.add_argument(
        'xml_file',
        help='Path to export.xml file from Apple Health export'
    )
    parser.add_argument(
        'command',
        choices=['list-types', 'list-workouts', 'summary', 'export-records', 'export-workouts'],
        help='Command to run'
    )
    parser.add_argument(
        '--output',
        help='Output file path (for export commands)'
    )
    parser.add_argument(
        '--type',
        help='Filter by specific type (e.g., HKQuantityTypeIdentifierStepCount)'
    )
    parser.add_argument(
        '--start-date',
        help='Filter records after this date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        help='Filter records before this date (YYYY-MM-DD)'
    )

    args = parser.parse_args()

    # Create parser and load XML
    health_parser = HealthDataParser(args.xml_file)
    if not health_parser.parse():
        sys.exit(1)

    # Execute command
    if args.command == 'list-types':
        record_types = health_parser.get_record_types()
        print(f"\nFound {len(record_types)} record types:\n")
        for rt in record_types:
            # Clean up the type name for display
            display_name = rt.replace('HKQuantityTypeIdentifier', '').replace('HKCategoryTypeIdentifier', '')
            print(f"  {rt}")
            if display_name != rt:
                print(f"    → {display_name}")

    elif args.command == 'list-workouts':
        workout_types = health_parser.get_workout_types()
        print(f"\nFound {len(workout_types)} workout types:\n")
        for wt in workout_types:
            display_name = wt.replace('HKWorkoutActivityType', '')
            print(f"  {wt}")
            if display_name != wt:
                print(f"    → {display_name}")

    elif args.command == 'summary':
        stats = health_parser.get_summary_stats()
        if stats:
            print("\nHealth Data Summary:")
            print(f"  Total Records: {stats['total_records']:,}")
            print(f"  Total Workouts: {stats['total_workouts']:,}")
            print(f"  Record Types: {stats['record_types']}")
            print(f"  Workout Types: {stats['workout_types']}")
            if 'earliest_date' in stats:
                print(f"  Date Range: {stats['earliest_date']} to {stats['latest_date']}")

    elif args.command == 'export-records':
        if not args.output:
            print("Error: --output required for export-records command")
            sys.exit(1)

        health_parser.export_records_to_csv(
            args.output,
            record_type=args.type,
            start_date=args.start_date,
            end_date=args.end_date
        )

    elif args.command == 'export-workouts':
        if not args.output:
            print("Error: --output required for export-workouts command")
            sys.exit(1)

        health_parser.export_workouts_to_csv(args.output, workout_type=args.type)


if __name__ == '__main__':
    main()
