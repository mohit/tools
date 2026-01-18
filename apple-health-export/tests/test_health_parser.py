"""
Tests for health_parser.py
"""

import os
import sys
import csv
import tempfile
from pathlib import Path
from datetime import datetime
import unittest
from unittest.mock import patch

# Add parent directory to path to import the module
sys.path.insert(0, str(Path(__file__).parent.parent))

import health_parser


class TestHealthDataParser(unittest.TestCase):
    """Test cases for HealthDataParser class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)

        # Use the sample export XML from fixtures
        self.fixtures_dir = Path(__file__).parent / "fixtures"
        self.sample_xml = self.fixtures_dir / "sample_export.xml"

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)

    def test_parser_initialization(self):
        """Test parser initialization."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        self.assertEqual(parser.xml_file, self.sample_xml)
        self.assertIsNone(parser.tree)
        self.assertIsNone(parser.root)

    def test_parse_success(self):
        """Test successful parsing of XML file."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        result = parser.parse()

        self.assertTrue(result)
        self.assertIsNotNone(parser.tree)
        self.assertIsNotNone(parser.root)
        self.assertEqual(parser.root.tag, 'HealthData')

    def test_parse_invalid_xml(self):
        """Test parsing invalid XML file."""
        invalid_xml = self.temp_path / "invalid.xml"
        invalid_xml.write_text("not valid xml")

        parser = health_parser.HealthDataParser(invalid_xml)
        result = parser.parse()

        self.assertFalse(result)

    def test_parse_missing_file(self):
        """Test parsing when file doesn't exist."""
        missing_file = self.temp_path / "missing.xml"

        parser = health_parser.HealthDataParser(missing_file)
        result = parser.parse()

        self.assertFalse(result)

    def test_get_record_types(self):
        """Test getting unique record types."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        record_types = parser.get_record_types()

        self.assertIsInstance(record_types, list)
        self.assertGreater(len(record_types), 0)
        self.assertIn('HKQuantityTypeIdentifierStepCount', record_types)
        self.assertIn('HKQuantityTypeIdentifierHeartRate', record_types)

        # Should be sorted
        self.assertEqual(record_types, sorted(record_types))

    def test_get_record_types_no_parse(self):
        """Test getting record types without parsing first."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        record_types = parser.get_record_types()

        self.assertEqual(record_types, [])

    def test_get_workout_types(self):
        """Test getting unique workout types."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        workout_types = parser.get_workout_types()

        self.assertIsInstance(workout_types, list)
        self.assertGreater(len(workout_types), 0)
        self.assertIn('HKWorkoutActivityTypeRunning', workout_types)
        self.assertIn('HKWorkoutActivityTypeWalking', workout_types)
        self.assertIn('HKWorkoutActivityTypeCycling', workout_types)

        # Should be sorted
        self.assertEqual(workout_types, sorted(workout_types))

    def test_get_workout_types_no_parse(self):
        """Test getting workout types without parsing first."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        workout_types = parser.get_workout_types()

        self.assertEqual(workout_types, [])

    def test_export_records_to_csv_all_records(self):
        """Test exporting all records to CSV."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "all_records.csv"

        result = parser.export_records_to_csv(output_file)

        self.assertTrue(result)
        self.assertTrue(output_file.exists())

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            self.assertGreater(len(rows), 0)
            # Should have step count and heart rate records
            types = {row['type'] for row in rows}
            self.assertIn('HKQuantityTypeIdentifierStepCount', types)
            self.assertIn('HKQuantityTypeIdentifierHeartRate', types)

    def test_export_records_to_csv_filtered_by_type(self):
        """Test exporting records filtered by type."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "steps.csv"

        result = parser.export_records_to_csv(
            output_file,
            record_type='HKQuantityTypeIdentifierStepCount'
        )

        self.assertTrue(result)

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # All rows should be step count
            for row in rows:
                self.assertEqual(row['type'], 'HKQuantityTypeIdentifierStepCount')

            # Should have 3 step count records from sample data
            self.assertEqual(len(rows), 3)

    def test_export_records_to_csv_filtered_by_date(self):
        """Test exporting records filtered by date range."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "filtered_dates.csv"

        result = parser.export_records_to_csv(
            output_file,
            start_date='2024-01-11',
            end_date='2024-01-11'
        )

        self.assertTrue(result)

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # All rows should be from 2024-01-11
            for row in rows:
                start_date = row['startDate']
                self.assertIn('2024-01-11', start_date)

    def test_export_records_to_csv_with_metadata(self):
        """Test that metadata is included in CSV export."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "heart_rate.csv"

        result = parser.export_records_to_csv(
            output_file,
            record_type='HKQuantityTypeIdentifierHeartRate'
        )

        self.assertTrue(result)

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have metadata columns
            self.assertGreater(len(rows), 0)
            # Check that metadata keys are present
            fieldnames = reader.fieldnames
            metadata_fields = [f for f in fieldnames if f.startswith('metadata_')]
            self.assertGreater(len(metadata_fields), 0)

    def test_export_records_to_csv_no_matches(self):
        """Test exporting when no records match the filter."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "no_matches.csv"

        result = parser.export_records_to_csv(
            output_file,
            record_type='HKQuantityTypeIdentifierNonExistent'
        )

        self.assertFalse(result)
        # File should not be created
        self.assertFalse(output_file.exists())

    def test_export_records_to_csv_not_parsed(self):
        """Test exporting without parsing first."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        output_file = self.temp_path / "output.csv"

        result = parser.export_records_to_csv(output_file)

        self.assertFalse(result)

    def test_export_workouts_to_csv_all_workouts(self):
        """Test exporting all workouts to CSV."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "all_workouts.csv"

        result = parser.export_workouts_to_csv(output_file)

        self.assertTrue(result)
        self.assertTrue(output_file.exists())

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have 3 workouts (running, walking, cycling)
            self.assertEqual(len(rows), 3)

            types = {row['workoutActivityType'] for row in rows}
            self.assertIn('HKWorkoutActivityTypeRunning', types)
            self.assertIn('HKWorkoutActivityTypeWalking', types)
            self.assertIn('HKWorkoutActivityTypeCycling', types)

    def test_export_workouts_to_csv_filtered_by_type(self):
        """Test exporting workouts filtered by type."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "running.csv"

        result = parser.export_workouts_to_csv(
            output_file,
            workout_type='HKWorkoutActivityTypeRunning'
        )

        self.assertTrue(result)

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have 1 running workout
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['workoutActivityType'], 'HKWorkoutActivityTypeRunning')

    def test_export_workouts_to_csv_includes_statistics(self):
        """Test that workout statistics are included in CSV."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "workouts_with_stats.csv"

        result = parser.export_workouts_to_csv(
            output_file,
            workout_type='HKWorkoutActivityTypeRunning'
        )

        self.assertTrue(result)

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have workout statistics columns
            fieldnames = reader.fieldnames
            stat_fields = [f for f in fieldnames if f.startswith('stat_')]
            self.assertGreater(len(stat_fields), 0)

            # Verify the running workout has heart rate stats
            row = rows[0]
            self.assertIn('stat_HeartRate', row)

    def test_export_workouts_to_csv_includes_metadata(self):
        """Test that workout metadata is included in CSV."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "workouts_metadata.csv"

        result = parser.export_workouts_to_csv(output_file)

        self.assertTrue(result)

        # Read and verify CSV
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            # Should have metadata columns
            fieldnames = reader.fieldnames
            metadata_fields = [f for f in fieldnames if f.startswith('metadata_')]
            self.assertGreater(len(metadata_fields), 0)

    def test_export_workouts_to_csv_no_matches(self):
        """Test exporting workouts when no matches found."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        output_file = self.temp_path / "no_workouts.csv"

        result = parser.export_workouts_to_csv(
            output_file,
            workout_type='HKWorkoutActivityTypeNonExistent'
        )

        self.assertFalse(result)

    def test_export_workouts_to_csv_not_parsed(self):
        """Test exporting workouts without parsing first."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        output_file = self.temp_path / "output.csv"

        result = parser.export_workouts_to_csv(output_file)

        self.assertFalse(result)

    def test_get_summary_stats(self):
        """Test getting summary statistics."""
        parser = health_parser.HealthDataParser(self.sample_xml)
        parser.parse()

        stats = parser.get_summary_stats()

        self.assertIsNotNone(stats)
        self.assertIn('total_records', stats)
        self.assertIn('total_workouts', stats)
        self.assertIn('record_types', stats)
        self.assertIn('workout_types', stats)
        self.assertIn('earliest_date', stats)
        self.assertIn('latest_date', stats)

        # Verify counts
        self.assertGreater(stats['total_records'], 0)
        self.assertGreater(stats['total_workouts'], 0)
        self.assertEqual(stats['total_workouts'], 3)  # Sample has 3 workouts

        # Verify date format
        self.assertRegex(stats['earliest_date'], r'\d{4}-\d{2}-\d{2}')
        self.assertRegex(stats['latest_date'], r'\d{4}-\d{2}-\d{2}')

    def test_get_summary_stats_not_parsed(self):
        """Test getting summary stats without parsing first."""
        parser = health_parser.HealthDataParser(self.sample_xml)

        stats = parser.get_summary_stats()

        self.assertIsNone(stats)


class TestHealthParserCLI(unittest.TestCase):
    """Test command-line interface for health parser."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        self.fixtures_dir = Path(__file__).parent / "fixtures"
        self.sample_xml = self.fixtures_dir / "sample_export.xml"

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)

    def test_cli_list_types_command(self):
        """Test CLI list-types command."""
        with patch('sys.argv', ['health_parser.py', str(self.sample_xml), 'list-types']):
            try:
                health_parser.main()
            except SystemExit:
                pass  # Command runs and exits normally

    def test_cli_list_workouts_command(self):
        """Test CLI list-workouts command."""
        with patch('sys.argv', ['health_parser.py', str(self.sample_xml), 'list-workouts']):
            try:
                health_parser.main()
            except SystemExit:
                pass  # Command runs and exits normally

    def test_cli_summary_command(self):
        """Test CLI summary command."""
        with patch('sys.argv', ['health_parser.py', str(self.sample_xml), 'summary']):
            try:
                health_parser.main()
            except SystemExit:
                pass  # Command runs and exits normally

    def test_cli_export_records_command(self):
        """Test CLI export-records command."""
        output_file = self.temp_path / "output.csv"

        with patch('sys.argv', [
            'health_parser.py',
            str(self.sample_xml),
            'export-records',
            '--output', str(output_file),
            '--type', 'HKQuantityTypeIdentifierStepCount'
        ]):
            try:
                health_parser.main()
            except SystemExit:
                pass

            # Verify file was created
            self.assertTrue(output_file.exists())

    def test_cli_export_records_missing_output(self):
        """Test CLI export-records without required --output."""
        with patch('sys.argv', [
            'health_parser.py',
            str(self.sample_xml),
            'export-records'
        ]):
            with self.assertRaises(SystemExit) as cm:
                health_parser.main()

            self.assertEqual(cm.exception.code, 1)

    def test_cli_export_workouts_command(self):
        """Test CLI export-workouts command."""
        output_file = self.temp_path / "workouts.csv"

        with patch('sys.argv', [
            'health_parser.py',
            str(self.sample_xml),
            'export-workouts',
            '--output', str(output_file)
        ]):
            try:
                health_parser.main()
            except SystemExit:
                pass

            # Verify file was created
            self.assertTrue(output_file.exists())

    def test_cli_with_date_filters(self):
        """Test CLI with start and end date filters."""
        output_file = self.temp_path / "filtered.csv"

        with patch('sys.argv', [
            'health_parser.py',
            str(self.sample_xml),
            'export-records',
            '--output', str(output_file),
            '--start-date', '2024-01-10',
            '--end-date', '2024-01-10'
        ]):
            try:
                health_parser.main()
            except SystemExit:
                pass

            # Verify file was created
            self.assertTrue(output_file.exists())

    def test_cli_invalid_xml_file(self):
        """Test CLI with invalid/missing XML file."""
        with patch('sys.argv', [
            'health_parser.py',
            '/nonexistent/file.xml',
            'summary'
        ]):
            with self.assertRaises(SystemExit) as cm:
                health_parser.main()

            self.assertEqual(cm.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
