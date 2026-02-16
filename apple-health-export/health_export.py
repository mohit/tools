#!/usr/bin/env python3
"""
Apple Health Data Exporter

This script helps export health data from Apple Health on macOS.
It can trigger exports via AppleScript and parse the resulting XML data.
"""

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime


def trigger_health_export(output_dir=None):
    """
    Trigger Apple Health export using AppleScript automation.

    This will open Health.app and attempt to navigate to the export function.
    Note: You'll still need to manually confirm the export dialog.
    """
    if output_dir is None:
        output_dir = Path.home() / "Downloads"

    applescript = '''
    tell application "Health"
        activate
    end tell

    display notification "Please export your health data manually:" with title "Health Export"
    display notification "1. Click your profile icon (top right)" with title "Step 1"
    display notification "2. Scroll down and tap 'Export All Health Data'" with title "Step 2"
    display notification "3. Save the export.zip file" with title "Step 3"
    '''

    try:
        subprocess.run(['osascript', '-e', applescript], check=True)
        print("Health.app opened. Please manually export your data:")
        print("  1. Click your profile icon (top right)")
        print("  2. Scroll down and click 'Export All Health Data'")
        print("  3. Save the export.zip file")
        print(f"\nSuggested save location: {output_dir}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error opening Health.app: {e}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("Error: osascript not found. Are you running on macOS?", file=sys.stderr)
        return False


def find_health_export(search_dir=None):
    """
    Find the most recent Apple Health export.zip file.
    """
    if search_dir is None:
        search_dir = Path.home() / "Downloads"

    search_path = Path(search_dir)
    export_files = list(search_path.glob("export*.zip"))

    if not export_files:
        return None

    # Return the most recently modified file
    most_recent = max(export_files, key=lambda p: p.stat().st_mtime)
    return most_recent


def extract_export(zip_path, output_dir=None):
    """
    Extract the Apple Health export.zip file.
    """
    zip_path = Path(zip_path)

    if not zip_path.exists():
        print(f"Error: Export file not found: {zip_path}", file=sys.stderr)
        return None

    if output_dir is None:
        output_dir = zip_path.parent / f"apple_health_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    print(f"Extracting {zip_path} to {output_dir}...")

    try:
        import zipfile
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"Extraction complete: {output_dir}")
        return output_dir
    except (OSError, zipfile.BadZipFile) as e:
        print(f"Error extracting file: {e}", file=sys.stderr)
        return None


def get_export_info(export_dir):
    """
    Get information about an extracted Apple Health export.
    """
    export_dir = Path(export_dir)

    if not export_dir.exists():
        print(f"Error: Directory not found: {export_dir}", file=sys.stderr)
        return None

    # Look for the main export.xml file
    xml_file = export_dir / "apple_health_export" / "export.xml"
    if not xml_file.exists():
        # Try alternate location
        xml_file = export_dir / "export.xml"

    if not xml_file.exists():
        print(f"Error: export.xml not found in {export_dir}", file=sys.stderr)
        return None

    # Get file size
    file_size_mb = xml_file.stat().st_size / (1024 * 1024)

    info = {
        'xml_file': xml_file,
        'size_mb': file_size_mb,
        'export_dir': export_dir
    }

    print(f"\nExport Information:")
    print(f"  Location: {xml_file}")
    print(f"  Size: {file_size_mb:.2f} MB")

    return info


def main():
    """Main entry point for the health export tool."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Export and manage Apple Health data on macOS'
    )
    parser.add_argument(
        'command',
        choices=['export', 'extract', 'info', 'find'],
        help='Command to run'
    )
    parser.add_argument(
        '--file',
        help='Path to export.zip file (for extract/info commands)'
    )
    parser.add_argument(
        '--dir',
        help='Directory to search or output directory'
    )

    args = parser.parse_args()

    if args.command == 'export':
        trigger_health_export(args.dir)

    elif args.command == 'find':
        export_file = find_health_export(args.dir)
        if export_file:
            print(f"Found export file: {export_file}")
            print(f"Modified: {datetime.fromtimestamp(export_file.stat().st_mtime)}")
        else:
            print("No export file found")
            sys.exit(1)

    elif args.command == 'extract':
        if not args.file:
            # Try to find it automatically
            args.file = find_health_export(args.dir)
            if not args.file:
                print("Error: No export file specified and none found automatically")
                print("Use --file to specify the export.zip location")
                sys.exit(1)

        extract_dir = extract_export(args.file, args.dir)
        if extract_dir:
            get_export_info(extract_dir)

    elif args.command == 'info':
        if not args.dir:
            print("Error: --dir required for info command")
            sys.exit(1)
        get_export_info(args.dir)


if __name__ == '__main__':
    main()
