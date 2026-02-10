from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from .takeout import (
    LOCATION_COLUMNS,
    MUSIC_COLUMNS,
    SEARCH_COLUMNS,
    build_analysis_report,
    merge_to_curated,
    parse_takeout,
    write_catalog,
    write_raw_snapshots,
)

DEFAULT_RAW_ROOT = Path.home() / "datalake.me" / "raw"
DEFAULT_CURATED_ROOT = Path.home() / "datalake.me" / "curated"
DEFAULT_CATALOG_ROOT = Path.home() / "datalake.me" / "catalog"


def main() -> None:
    parser = argparse.ArgumentParser(prog="google-takeout-focused")
    subparsers = parser.add_subparsers(dest="command", required=True)

    checklist_parser = subparsers.add_parser("takeout-checklist")
    checklist_parser.add_argument("--json", action="store_true")

    analyze_parser = subparsers.add_parser("analyze")
    analyze_parser.add_argument("--input", required=True, help="Takeout directory or .zip file")
    analyze_parser.add_argument("--report-path", help="Optional output report JSON path")

    ingest_parser = subparsers.add_parser("ingest")
    ingest_parser.add_argument("--input", required=True, help="Takeout directory or .zip file")
    ingest_parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    ingest_parser.add_argument("--curated-root", default=str(DEFAULT_CURATED_ROOT))
    ingest_parser.add_argument("--catalog-root", default=str(DEFAULT_CATALOG_ROOT))
    ingest_parser.add_argument("--snapshot-id", help="Optional snapshot id. Defaults to UTC timestamp.")

    args = parser.parse_args()

    if args.command == "takeout-checklist":
        _run_checklist(as_json=args.json)
        return

    if args.command == "analyze":
        data = parse_takeout(Path(args.input).expanduser())
        report = build_analysis_report(data)
        if args.report_path:
            report_path = Path(args.report_path).expanduser()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
            print(f"Wrote report to {report_path}")
        print(json.dumps(report, indent=2, sort_keys=True))
        return

    if args.command == "ingest":
        takeout_path = Path(args.input).expanduser()
        raw_root = Path(args.raw_root).expanduser()
        curated_root = Path(args.curated_root).expanduser()
        catalog_root = Path(args.catalog_root).expanduser()
        snapshot_id = args.snapshot_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

        data = parse_takeout(takeout_path)
        takeout_root = _resolve_root(takeout_path)
        copied = write_raw_snapshots(data.matched_files, takeout_root, raw_root, snapshot_id)

        totals = {
            "location_timeline": merge_to_curated(
                records=data.location_events,
                dataset_name="location_timeline",
                columns=LOCATION_COLUMNS,
                ts_column="event_ts",
                curated_root=curated_root,
            ),
            "search_history": merge_to_curated(
                records=data.search_events,
                dataset_name="search_history",
                columns=SEARCH_COLUMNS,
                ts_column="event_ts",
                curated_root=curated_root,
            ),
            "youtube_music_history": merge_to_curated(
                records=data.music_events,
                dataset_name="youtube_music_history",
                columns=MUSIC_COLUMNS,
                ts_column="event_ts",
                curated_root=curated_root,
            ),
        }
        catalog_path = write_catalog(catalog_root, snapshot_id, data, totals)

        print(f"snapshot_id={snapshot_id}")
        print(f"raw_copied={copied}")
        print(f"curated_totals={totals}")
        print(f"catalog={catalog_path}")


def _resolve_root(input_path: Path) -> Path:
    if input_path.is_dir():
        return input_path
    if input_path.is_file() and input_path.suffix.lower() == ".zip":
        raise ValueError("Raw snapshot copy from .zip input is not supported. Extract first and pass directory.")
    raise ValueError(f"Unsupported input path: {input_path}")


def _run_checklist(as_json: bool) -> None:
    checklist = {
        "include_services": [
            "Location History (Timeline)",
            "Search",
            "YouTube and YouTube Music",
        ],
        "exclude_services": [
            "Gmail",
            "Drive",
            "Docs",
            "Photos",
            "YouTube (video uploads)",
            "Chrome",
            "other Google services",
        ],
        "delivery": {
            "export_once": True,
            "file_type": "zip",
            "archive_size": "2GB",
        },
        "recommended_cadence": "quarterly",
    }
    if as_json:
        print(json.dumps(checklist, indent=2, sort_keys=True))
        return

    print("Include services:")
    for item in checklist["include_services"]:
        print(f"- {item}")
    print("Exclude services:")
    for item in checklist["exclude_services"]:
        print(f"- {item}")
    print("Cadence: quarterly")


if __name__ == "__main__":
    main()
