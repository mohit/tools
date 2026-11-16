#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR/safari}"

if ! command -v xcrun >/dev/null 2>&1; then
  echo "Error: xcrun is not installed. Install Xcode command line tools first." >&2
  exit 1
fi

if [ -e "$OUTPUT_DIR" ] && [ -n "$(ls -A "$OUTPUT_DIR" 2>/dev/null || true)" ]; then
  echo "Error: output directory is not empty: $OUTPUT_DIR" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_DIR")"
OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_DIR")" && pwd)/$(basename "$OUTPUT_DIR")"
STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/moer-safari.XXXXXX")"
trap 'rm -rf "$STAGING_DIR"' EXIT
for resource in background.js content-2fa-detector.js content-google-voice.js popup.js popup.html icon.svg; do
  cp "$SCRIPT_DIR/$resource" "$STAGING_DIR/$resource"
done
cp "$SCRIPT_DIR/manifest_safari.json" "$STAGING_DIR/manifest.json"
xcrun safari-web-extension-converter "$STAGING_DIR" \
  --project-location "$OUTPUT_DIR" \
  --copy-resources \
  --no-open

echo "Safari project generated at: $OUTPUT_DIR"
echo "Next: open the .xcodeproj in Xcode, build/run, then enable the extension in Safari Settings > Extensions."
