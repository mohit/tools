import argparse
import csv
import json
import os
from datetime import UTC, datetime
from pathlib import Path


DEFAULT_RAW_BASE = Path(
    os.environ.get(
        "DATALAKE_RAW_ROOT",
        "/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports",
    )
)
DEFAULT_RAW_ROOT = DEFAULT_RAW_BASE / "apple-music"
PLAYED_AT_COLUMNS = [
    "Event Start Timestamp",
    "Play Date UTC",
    "Last Played Date",
    "Event Start Date",
]


def discover_csv(raw_root: Path, explicit_file: Path | None) -> Path:
    if explicit_file:
        if not explicit_file.exists():
            raise FileNotFoundError(f"CSV file not found: {explicit_file}")
        return explicit_file

    candidates = sorted(
        (path for path in raw_root.rglob("*.csv") if _is_play_activity_csv_name(path.name)),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No Apple Music Play Activity CSV found under {raw_root}. "
            "Expected file name containing 'Play Activity' (spaces/underscores/hyphens accepted)."
        )
    return candidates[0]


def _is_play_activity_csv_name(name: str) -> bool:
    normalized = name.casefold().replace("_", " ").replace("-", " ")
    return normalized.endswith(".csv") and "play activity" in normalized


def _parse_dt(value: str) -> datetime | None:
    value = value.strip()
    if not value:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            continue

    return None


def extract_latest_played_at(csv_path: Path) -> datetime | None:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return None

        played_col = None
        for candidate in PLAYED_AT_COLUMNS:
            if candidate in reader.fieldnames:
                played_col = candidate
                break

        if not played_col:
            return None

        latest = None
        for row in reader:
            value = row.get(played_col, "")
            parsed = _parse_dt(value)
            if parsed and (latest is None or parsed > latest):
                latest = parsed

        return latest


def compute_status(days_stale: int, warn_days: int, critical_days: int) -> tuple[str, int]:
    if days_stale >= critical_days:
        return "critical", 2
    if days_stale >= warn_days:
        return "warning", 1
    return "fresh", 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Apple Music export freshness and return status code for automation."
    )
    parser.add_argument("--raw-root", type=Path, default=Path(str(DEFAULT_RAW_ROOT)))
    parser.add_argument("--csv-file", type=Path, default=None)
    parser.add_argument("--warn-days", type=int, default=30)
    parser.add_argument("--critical-days", type=int, default=90)
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    raw_root = Path(args.raw_root).expanduser()
    explicit_csv = Path(args.csv_file).expanduser() if args.csv_file else None
    try:
        csv_path = discover_csv(raw_root=raw_root, explicit_file=explicit_csv)
    except FileNotFoundError as exc:
        payload = {
            "status": "missing",
            "csv_file": None,
            "warn_days": args.warn_days,
            "critical_days": args.critical_days,
            "reason": str(exc),
        }
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(str(exc))
        raise SystemExit(3)

    latest_played = extract_latest_played_at(csv_path)
    if latest_played is None:
        latest_played = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=UTC)

    now = datetime.now(UTC)
    days_stale = (now - latest_played).days
    status, exit_code = compute_status(days_stale, warn_days=args.warn_days, critical_days=args.critical_days)

    payload = {
        "status": status,
        "days_stale": days_stale,
        "latest_played_at_utc": latest_played.isoformat(),
        "csv_file": str(csv_path),
        "warn_days": args.warn_days,
        "critical_days": args.critical_days,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"status={status} days_stale={days_stale} latest_played_at_utc={payload['latest_played_at_utc']}")
        print(f"csv_file={payload['csv_file']}")

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
