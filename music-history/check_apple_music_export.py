from __future__ import annotations

import argparse
import sys
from pathlib import Path

from apple_music_export_guard import (
    AppleMusicExportGuardError,
    enforce_fresh_export_or_raise,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Apple Music export freshness metadata."
    )
    parser.add_argument(
        "--metadata-path",
        type=Path,
        default=Path(__file__).with_name("apple_music_export_metadata.json"),
        help="Path to Apple Music export metadata JSON",
    )
    parser.add_argument(
        "--max-staleness-days",
        type=int,
        default=365,
        help="Maximum allowed age (in days) for latest_play_date",
    )
    args = parser.parse_args()

    try:
        metadata, age_days = enforce_fresh_export_or_raise(
            args.metadata_path,
            max_staleness_days=args.max_staleness_days,
        )
    except AppleMusicExportGuardError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        "Apple Music export freshness check passed. "
        f"latest_play_date={metadata.latest_play_date.isoformat()} "
        f"last_export_date={metadata.last_export_date.isoformat()} "
        f"age_days={age_days}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
