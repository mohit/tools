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


if __name__ == '__main__':
    unittest.main()
