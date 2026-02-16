"""Shared configuration for music-history tools."""
from __future__ import annotations

import os
from pathlib import Path

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", str(Path.home() / "datalake.me")))
RAW_ROOT = Path(os.environ.get("DATALAKE_RAW_ROOT", str(DATALAKE_ROOT / "raw")))
CURATED_ROOT = Path(os.environ.get("DATALAKE_CURATED_ROOT", str(DATALAKE_ROOT / "curated")))
CATALOG_DB = DATALAKE_ROOT / "catalog" / "datalake.duckdb"

SCRIPT_DIR = Path(__file__).resolve().parent
EXPORT_METADATA_PATH = SCRIPT_DIR / "apple_music_export_metadata.json"

MAX_STALENESS_DAYS = int(os.environ.get("APPLE_MUSIC_MAX_STALENESS_DAYS", "365"))
