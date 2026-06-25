"""
Tests for health_export.py
"""

import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent directory to path to import the module
import health_export


class TestHealthExport(unittest.TestCase):
    """Test cases for health export functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)

    def create_sample_export_zip(self, filename="export.zip"):
        """Create a sample export.zip file for testing."""
        zip_path = self.temp_path / filename

        # Create a sample export.xml
        sample_xml = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE HealthData>
<HealthData locale="en_US">
  <ExportDate value="2024-01-15 12:00:00 -0800"/>
  <Record type="HKQuantityTypeIdentifierStepCount"
          sourceName="iPhone"
          unit="count"
          value="1234"
          startDate="2024-01-10 08:00:00 -0800"
          endDate="2024-01-10 08:15:00 -0800"/>
</HealthData>"""

        # Create zip file with apple_health_export folder structure
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('apple_health_export/export.xml', sample_xml)
            zf.writestr('apple_health_export/export_cda.xml', '<ClinicalDocument/>')

        return zip_path

    @patch('subprocess.run')
    def test_trigger_health_export_success(self, mock_run):
        """Test triggering health export via AppleScript."""
        mock_run.return_value = MagicMock(returncode=0)

        result = health_export.trigger_health_export(self.temp_dir)

        self.assertTrue(result)
        mock_run.assert_called_once()

        # Check that osascript was called
        call_args = mock_run.call_args[0][0]
        self.assertEqual(call_args[0], 'osascript')

    @patch('subprocess.run')
    def test_trigger_health_export_failure(self, mock_run):
        """Test health export when osascript fails."""
        from subprocess import CalledProcessError
        mock_run.side_effect = CalledProcessError(1, 'osascript')

        result = health_export.trigger_health_export(self.temp_dir)

        self.assertFalse(result)

    @patch('subprocess.run')
    def test_trigger_health_export_not_macos(self, mock_run):
        """Test health export when osascript is not available."""
        mock_run.side_effect = FileNotFoundError()

        result = health_export.trigger_health_export(self.temp_dir)

        self.assertFalse(result)

    def test_find_health_export_found(self):
        """Test finding the most recent export file."""
        # Create multiple export files
        export1 = self.temp_path / "export.zip"
        export2 = self.temp_path / "export_old.zip"

        export1.touch()
        export2.touch()

        # Make export1 newer
        os.utime(export2, (0, 0))

        result = health_export.find_health_export(self.temp_dir)

        self.assertEqual(result, export1)

    def test_find_health_export_not_found(self):
        """Test when no export file exists."""
        result = health_export.find_health_export(self.temp_dir)

        self.assertIsNone(result)

    def test_find_health_export_multiple_files(self):
        """Test finding most recent when multiple exports exist."""
        # Create three export files
        files = []
        for i, name in enumerate(['export_old.zip', 'export_older.zip', 'export_newest.zip']):
            f = self.temp_path / name
            f.touch()
            # Set different modification times
            os.utime(f, (i * 1000, i * 1000))
            files.append(f)

        result = health_export.find_health_export(self.temp_dir)

        # Should return the newest file
        self.assertEqual(result, files[2])

    def test_extract_export_success(self):
        """Test extracting a valid export.zip file."""
        zip_path = self.create_sample_export_zip()

        result = health_export.extract_export(zip_path)

        self.assertIsNotNone(result)
        self.assertTrue(result.exists())
        self.assertTrue((result / "apple_health_export" / "export.xml").exists())

    def test_extract_export_file_not_found(self):
        """Test extracting when file doesn't exist."""
        fake_path = self.temp_path / "nonexistent.zip"

        result = health_export.extract_export(fake_path)

        self.assertIsNone(result)

    def test_extract_export_invalid_zip(self):
        """Test extracting an invalid zip file."""
        # Create an invalid zip file
        invalid_zip = self.temp_path / "invalid.zip"
        invalid_zip.write_text("not a zip file")

        result = health_export.extract_export(invalid_zip)

        self.assertIsNone(result)

    def test_extract_export_custom_output_dir(self):
        """Test extracting with custom output directory."""
        zip_path = self.create_sample_export_zip()
        custom_dir = self.temp_path / "custom_output"

        result = health_export.extract_export(zip_path, custom_dir)

        self.assertEqual(result, custom_dir)
        self.assertTrue(custom_dir.exists())

    def test_get_export_info_success(self):
        """Test getting info about an extracted export."""
        zip_path = self.create_sample_export_zip()
        extract_dir = health_export.extract_export(zip_path)

        info = health_export.get_export_info(extract_dir)

        self.assertIsNotNone(info)
        self.assertIn('xml_file', info)
        self.assertIn('size_mb', info)
        self.assertIn('export_dir', info)
        self.assertTrue(info['xml_file'].exists())
        self.assertGreater(info['size_mb'], 0)

    def test_get_export_info_directory_not_found(self):
        """Test getting info when directory doesn't exist."""
        fake_dir = self.temp_path / "nonexistent"

        result = health_export.get_export_info(fake_dir)

        self.assertIsNone(result)

    def test_get_export_info_no_xml(self):
        """Test getting info when export.xml is missing."""
        empty_dir = self.temp_path / "empty"
        empty_dir.mkdir()

        result = health_export.get_export_info(empty_dir)

        self.assertIsNone(result)

    def test_get_export_info_alternate_location(self):
        """Test getting info when export.xml is in root instead of subfolder."""
        # Create export.xml directly in directory
        alt_dir = self.temp_path / "alternate"
        alt_dir.mkdir()
        xml_file = alt_dir / "export.xml"
        xml_file.write_text("""<?xml version="1.0"?><HealthData></HealthData>""")

        info = health_export.get_export_info(alt_dir)

        self.assertIsNotNone(info)
        self.assertEqual(info['xml_file'], xml_file)


class TestHealthExportCLI(unittest.TestCase):
    """Test command-line interface."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temporary files."""
        import shutil
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path)

    def create_sample_export_zip(self):
        """Create a sample export.zip file for testing."""
        zip_path = self.temp_path / "export.zip"
        sample_xml = """<?xml version="1.0"?><HealthData></HealthData>"""

        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('apple_health_export/export.xml', sample_xml)

        return zip_path

    @patch('subprocess.run')
    @patch('sys.argv', ['health_export.py', 'export'])
    def test_cli_export_command(self, mock_run):
        """Test CLI export command."""
        mock_run.return_value = MagicMock(returncode=0)

        # Should not raise an exception
        try:
            health_export.main()
        except SystemExit:
            pass

    @patch('sys.argv', ['health_export.py', 'find', '--dir', '/tmp'])
    @patch('health_export.find_health_export')
    def test_cli_find_command_not_found(self, mock_find):
        """Test CLI find command when no export found."""
        mock_find.return_value = None

        with self.assertRaises(SystemExit) as cm:
            health_export.main()

        self.assertEqual(cm.exception.code, 1)

    def test_cli_extract_command_success(self):
        """Test CLI extract command with valid file."""
        zip_path = self.create_sample_export_zip()

        with patch('sys.argv', ['health_export.py', 'extract', '--file', str(zip_path)]):
            try:
                health_export.main()
            except SystemExit:
                pass  # Ignore exit

            # Verify extraction happened
            self.assertTrue(any(self.temp_path.glob('apple_health_export_*')))

    def test_cli_info_command_missing_dir(self):
        """Test CLI info command without required --dir argument."""
        with patch('sys.argv', ['health_export.py', 'info']):
            with self.assertRaises(SystemExit) as cm:
                health_export.main()

            self.assertEqual(cm.exception.code, 1)


class TestCheckFreshness(unittest.TestCase):
    """Tests for the check_freshness() staleness detection function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = Path(self.temp_dir)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    # ------------------------------------------------------------------ helpers

    def _touch_with_age(self, filename, age_days):
        """Create *filename* in data_dir with mtime set to *age_days* ago."""
        import time
        p = self.data_dir / filename
        p.touch()
        mtime = time.time() - age_days * 86400
        os.utime(p, (mtime, mtime))
        return p

    # ------------------------------------------------------------------ tests

    def test_fresh_files_returns_empty_list(self):
        """All files within threshold → no stale entries."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=1)  # 1 day old

        stale = health_export.check_freshness(
            data_dir=self.data_dir, threshold_days=30
        )

        self.assertEqual(stale, [])

    def test_stale_files_returns_all_entries(self):
        """All files exceed threshold → all returned as stale."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=60)  # 60 days old

        stale = health_export.check_freshness(
            data_dir=self.data_dir, threshold_days=30
        )

        self.assertEqual(len(stale), len(health_export._FRESHNESS_FILES))
        stale_names = {e["name"] for e in stale}
        self.assertEqual(stale_names, set(health_export._FRESHNESS_FILES))

    def test_stale_entry_has_expected_keys(self):
        """Each stale entry contains the required metadata keys."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=60)

        stale = health_export.check_freshness(
            data_dir=self.data_dir, threshold_days=30
        )

        for entry in stale:
            self.assertIn("name", entry)
            self.assertIn("path", entry)
            self.assertIn("age_days", entry)
            self.assertIn("mtime", entry)

    def test_missing_file_reported_as_stale(self):
        """A file that doesn't exist at all is treated as stale."""
        # Create all but the first file
        for name in health_export._FRESHNESS_FILES[1:]:
            self._touch_with_age(name, age_days=1)

        stale = health_export.check_freshness(
            data_dir=self.data_dir, threshold_days=30
        )

        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["name"], health_export._FRESHNESS_FILES[0])
        self.assertEqual(stale[0]["age_days"], float("inf"))

    def test_partial_staleness(self):
        """Only files that exceed the threshold appear in results."""
        # First file: stale; rest: fresh
        self._touch_with_age(health_export._FRESHNESS_FILES[0], age_days=60)
        for name in health_export._FRESHNESS_FILES[1:]:
            self._touch_with_age(name, age_days=1)

        stale = health_export.check_freshness(
            data_dir=self.data_dir, threshold_days=30
        )

        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["name"], health_export._FRESHNESS_FILES[0])

    def test_nonexistent_directory_returns_empty(self):
        """Missing data directory is tolerated — returns empty list."""
        fake_dir = self.data_dir / "no_such_dir"

        stale = health_export.check_freshness(data_dir=fake_dir, threshold_days=30)

        self.assertEqual(stale, [])

    def test_custom_threshold_tighter(self):
        """A tighter threshold catches files that would otherwise be fresh."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=15)

        # threshold=30 → all fresh
        self.assertEqual(
            health_export.check_freshness(self.data_dir, threshold_days=30), []
        )
        # threshold=7 → all stale
        stale = health_export.check_freshness(self.data_dir, threshold_days=7)
        self.assertEqual(len(stale), len(health_export._FRESHNESS_FILES))

    def test_stale_age_days_is_accurate(self):
        """Reported age_days reflects the actual file age."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=45)

        stale = health_export.check_freshness(
            data_dir=self.data_dir, threshold_days=30
        )

        for entry in stale:
            # Allow ±1 day tolerance for timing jitter during the test.
            self.assertAlmostEqual(entry["age_days"], 45, delta=1)


class TestPrintFreshnessReport(unittest.TestCase):
    """Tests for print_freshness_report() output and return value."""

    def test_all_fresh_returns_true(self):
        result = health_export.print_freshness_report([], threshold_days=30)
        self.assertTrue(result)

    def test_stale_returns_false(self):
        stale = [
            {
                "name": "health_workouts.parquet",
                "path": Path("/fake/health_workouts.parquet"),
                "age_days": 137,
                "mtime": "2026-02-07",
            }
        ]
        result = health_export.print_freshness_report(stale, threshold_days=30)
        self.assertFalse(result)

    def test_stale_output_contains_actionable_message(self):
        """The printed report must contain the privacy.apple.com URL."""
        stale = [
            {
                "name": "export.xml",
                "path": Path("/fake/export.xml"),
                "age_days": 155,
                "mtime": "2026-01-20",
            }
        ]
        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            health_export.print_freshness_report(stale, threshold_days=30)

        output = buf.getvalue()
        self.assertIn("privacy.apple.com", output)

    def test_stale_output_contains_filename(self):
        """Stale file name must appear in the printed report."""
        stale = [
            {
                "name": "health_records.parquet",
                "path": Path("/fake/health_records.parquet"),
                "age_days": 100,
                "mtime": "2026-03-16",
            }
        ]
        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            health_export.print_freshness_report(stale, threshold_days=30)

        self.assertIn("health_records.parquet", buf.getvalue())

    def test_missing_file_shows_missing_label(self):
        """Files with age_days=inf should display 'MISSING'."""
        stale = [
            {
                "name": "health_workouts.parquet",
                "path": Path("/fake/health_workouts.parquet"),
                "age_days": float("inf"),
                "mtime": "(missing)",
            }
        ]
        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            health_export.print_freshness_report(stale, threshold_days=30)

        self.assertIn("MISSING", buf.getvalue())


class TestCheckFreshnessCLI(unittest.TestCase):
    """CLI integration tests for the 'check-freshness' sub-command."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.data_dir = Path(self.temp_dir)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _touch_with_age(self, filename, age_days):
        import time
        p = self.data_dir / filename
        p.touch()
        mtime = time.time() - age_days * 86400
        os.utime(p, (mtime, mtime))
        return p

    def test_cli_check_freshness_all_fresh_exits_zero(self):
        """check-freshness exits 0 when all files are within the threshold."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=1)

        argv = ['health_export.py', 'check-freshness', '--dir', str(self.data_dir)]
        with patch('sys.argv', argv):
            try:
                health_export.main()
            except SystemExit as exc:
                self.fail(f"Expected exit 0 but got SystemExit({exc.code})")

    def test_cli_check_freshness_stale_exits_one(self):
        """check-freshness exits 1 when at least one file is stale."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=60)

        argv = ['health_export.py', 'check-freshness', '--dir', str(self.data_dir)]
        with patch('sys.argv', argv):
            with self.assertRaises(SystemExit) as cm:
                health_export.main()

            self.assertEqual(cm.exception.code, 1)

    def test_cli_check_freshness_custom_threshold(self):
        """--threshold-days flag overrides the default 30-day threshold."""
        for name in health_export._FRESHNESS_FILES:
            self._touch_with_age(name, age_days=15)

        # With default threshold (30) this should be fresh → exit 0
        argv = ['health_export.py', 'check-freshness', '--dir', str(self.data_dir)]
        with patch('sys.argv', argv):
            try:
                health_export.main()
            except SystemExit as exc:
                self.fail(f"Expected exit 0 but got SystemExit({exc.code})")

        # With tight threshold (7) files are stale → exit 1
        argv_tight = [
            'health_export.py', 'check-freshness',
            '--dir', str(self.data_dir),
            '--threshold-days', '7',
        ]
        with patch('sys.argv', argv_tight):
            with self.assertRaises(SystemExit) as cm:
                health_export.main()

            self.assertEqual(cm.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
