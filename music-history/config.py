"""Shared configuration for music-history tools."""
from __future__ import annotations

import os
from pathlib import Path

import datalake_paths as dp

# Every location derives from DATALAKE_ROOT. Entry points must call
# require_datalake() before writing so a missing or wrong disk fails first.
DATALAKE_ROOT = dp.configured_root()
RAW_ROOT = DATALAKE_ROOT / "raw"
CURATED_ROOT = DATALAKE_ROOT / "curated"
STATE_ROOT = DATALAKE_ROOT / "state" / "music-history"
ARTIFACTS_ROOT = DATALAKE_ROOT / "artifacts"
CATALOG_DB = DATALAKE_ROOT / "catalog" / "datalake.duckdb"


def require_datalake() -> Path:
    """Verify the datalake disk or exit with a clear message before any write."""
    try:
        return dp.require_root(DATALAKE_ROOT)
    except dp.DatalakeUnavailable as exc:
        raise SystemExit(str(exc)) from None

SCRIPT_DIR = Path(__file__).resolve().parent
EXPORT_METADATA_PATH = STATE_ROOT / "apple_music_export_metadata.json"

MAX_STALENESS_DAYS = int(os.environ.get("APPLE_MUSIC_MAX_STALENESS_DAYS", "365"))

RAW_APPLE_MUSIC_DIR = RAW_ROOT / "apple-music"
RAW_FILE_STALENESS_DAYS = int(os.environ.get("APPLE_MUSIC_RAW_FILE_STALENESS_DAYS", "30"))
