#!/bin/bash
#
# Installation script for apple-health-export
# Requires: uv (https://github.com/astral-sh/uv)
#

set -e  # Exit on error

echo "======================================"
echo "Apple Health Export - Installation"
echo "======================================"
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed."
    echo "Please install uv first: https://github.com/astral-sh/uv"
    echo ""
    echo "Quick install:"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

echo "✓ Found uv: $(uv --version)"
echo ""

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Installing apple-health-export..."
echo ""

# Install the package in development mode with dev dependencies
echo "Installing dependencies..."
uv pip install -e ".[dev]"

echo ""
echo "======================================"
echo "Installation complete!"
echo "======================================"
echo ""
echo "You can now use the following commands:"
echo ""
echo "  health-export export      # Start health data export"
echo "  health-export extract     # Extract export.zip"
echo "  health-export find        # Find existing export files"
echo ""
echo "  health-parse <xml> summary           # Show data summary"
echo "  health-parse <xml> list-types        # List all data types"
echo "  health-parse <xml> list-workouts     # List workout types"
echo "  health-parse <xml> export-records    # Export to CSV"
echo "  health-parse <xml> export-workouts   # Export workouts to CSV"
echo ""
echo "Or use the Python scripts directly:"
echo ""
echo "  python3 health_export.py --help"
echo "  python3 health_parser.py --help"
echo ""
echo "Run tests with:"
echo ""
echo "  pytest"
echo "  pytest --cov"
echo ""
