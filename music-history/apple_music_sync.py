import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import apple_music_monitor as monitor
import apple_music_musickit_sync as musickit
import apple_music_processor as processor

DEFAULT_RAW_BASE = Path(
    os.environ.get(
        "DATALAKE_RAW_ROOT",
        "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports",
    )
)
DEFAULT_CURATED_BASE = Path(
    os.environ.get(
        "DATALAKE_CURATED_ROOT",
        "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake/curated",
    )
)
DEFAULT_PLAY_ACTIVITY_RAW = DEFAULT_RAW_BASE / "apple-music"
DEFAULT_PLAY_ACTIVITY_CURATED = DEFAULT_CURATED_BASE / "apple-music" / "play-activity"
DEFAULT_MUSICKIT_RAW = DEFAULT_RAW_BASE / "apple-music" / "musickit"
DEFAULT_MUSICKIT_CURATED = DEFAULT_CURATED_BASE / "apple-music" / "recent-played"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Apple Music sync automation. Processes the latest privacy export Play Activity CSV "
            "(full history source of truth) and optionally captures MusicKit recent-played snapshots."
        )
    )
    parser.add_argument("--raw-root", type=Path, default=Path(str(DEFAULT_PLAY_ACTIVITY_RAW)))
    parser.add_argument("--csv-file", type=Path, default=None)
    parser.add_argument("--curated-root", type=Path, default=Path(str(DEFAULT_PLAY_ACTIVITY_CURATED)))
    parser.add_argument("--warn-days", type=int, default=30)
    parser.add_argument("--critical-days", type=int, default=90)
    parser.add_argument(
        "--skip-musickit",
        action="store_true",
        help="Skip MusicKit supplemental sync even if tokens are available.",
    )
    parser.add_argument("--developer-token", default=os.environ.get("APPLE_MUSIC_DEVELOPER_TOKEN"))
    parser.add_argument("--user-token", default=os.environ.get("APPLE_MUSIC_USER_TOKEN"))
    parser.add_argument("--musickit-raw-root", type=Path, default=Path(str(DEFAULT_MUSICKIT_RAW)))
    parser.add_argument("--musickit-curated-root", type=Path, default=Path(str(DEFAULT_MUSICKIT_CURATED)))
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    return parser.parse_args()


def run_sync(args: argparse.Namespace) -> tuple[dict, int]:
    summary: dict = {
        "play_activity": {
            "processed": False,
            "status": "missing",
        },
        "musickit": {
            "enabled": False,
            "synced": False,
        },
        "notes": [
            "MusicKit recent-played is supplemental only and does not replace privacy export full history."
        ],
    }

    raw_root = Path(args.raw_root).expanduser()
    csv_file = Path(args.csv_file).expanduser() if args.csv_file else None
    curated_root = Path(args.curated_root).expanduser()

    try:
        selected_csv = processor.discover_csv(raw_root=raw_root, explicit_file=csv_file)
        processed = processor.process_csv(csv_path=selected_csv, curated_root=curated_root)
        latest_played = monitor.extract_latest_played_at(selected_csv)

        if latest_played is None:
            latest_played = selected_csv.stat().st_mtime
            latest_played_dt = datetime.fromtimestamp(latest_played, tz=UTC)
        else:
            latest_played_dt = latest_played

        days_stale = (datetime.now(UTC) - latest_played_dt).days
        status, exit_code = monitor.compute_status(
            days_stale=days_stale,
            warn_days=args.warn_days,
            critical_days=args.critical_days,
        )

        summary["play_activity"] = {
            "processed": True,
            "status": status,
            "days_stale": days_stale,
            "latest_played_at_utc": latest_played_dt.isoformat(),
            "csv_file": str(selected_csv),
            "total_rows": processed["total_rows"],
            "curated_root": processed["curated_root"],
        }
    except FileNotFoundError:
        exit_code = 2
        summary["play_activity"] = {
            "processed": False,
            "status": "missing",
            "reason": "No Play Activity CSV found. Request/export from privacy.apple.com.",
        }

    can_run_musickit = bool(args.developer_token and args.user_token and not args.skip_musickit)
    summary["musickit"]["enabled"] = can_run_musickit

    if can_run_musickit:
        payload = musickit.fetch_recent_tracks(
            developer_token=args.developer_token,
            user_token=args.user_token,
        )
        raw_path = musickit.write_raw_snapshot(Path(args.musickit_raw_root).expanduser(), payload)
        row_count = musickit.upsert_curated(payload, Path(args.musickit_curated_root).expanduser())

        summary["musickit"] = {
            "enabled": True,
            "synced": True,
            "fetched_tracks": len(payload.get("data", [])),
            "raw_snapshot": str(raw_path),
            "curated_rows": row_count,
            "curated_root": str(Path(args.musickit_curated_root).expanduser()),
        }
    elif args.skip_musickit:
        summary["musickit"]["reason"] = "Skipped by flag"
    else:
        summary["musickit"]["reason"] = "Missing MusicKit token(s)"

    return summary, exit_code


def main() -> None:
    args = parse_args()
    summary, exit_code = run_sync(args)

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        play_status = summary["play_activity"].get("status", "unknown")
        print(f"Play Activity status: {play_status}")
        if summary["play_activity"].get("processed"):
            print(f"Rows: {summary['play_activity']['total_rows']}")
            print(f"CSV: {summary['play_activity']['csv_file']}")
        else:
            print(summary["play_activity"].get("reason", "Play Activity processing skipped."))

        if summary["musickit"].get("synced"):
            print(f"MusicKit fetched tracks: {summary['musickit']['fetched_tracks']}")
            print(f"MusicKit raw snapshot: {summary['musickit']['raw_snapshot']}")
        else:
            print(f"MusicKit: {summary['musickit'].get('reason', 'not run')}")

        print(summary["notes"][0])

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
