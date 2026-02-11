import argparse
import csv
import sys
from datetime import UTC, datetime
from pathlib import Path


DEFAULT_CSV_PATH = Path.home() / "datalake.me/raw/apple-music/Apple Music - Track Play History.csv"
PLAY_DATE_COLUMNS = ("Play Date UTC", "Play Date")


def parse_iso8601_utc(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def analyze_export(csv_path: Path) -> tuple[int, datetime]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Apple Music export not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV appears to be empty or missing a header row.")

        play_date_column = next((col for col in PLAY_DATE_COLUMNS if col in reader.fieldnames), None)
        if not play_date_column:
            cols = ", ".join(reader.fieldnames)
            raise ValueError(
                "Could not find a play-date column. Expected one of "
                f"{PLAY_DATE_COLUMNS}. Found: {cols}"
            )

        row_count = 0
        newest_play: datetime | None = None
        for row in reader:
            row_count += 1
            raw_date = (row.get(play_date_column) or "").strip()
            if not raw_date:
                continue
            parsed = parse_iso8601_utc(raw_date)
            if newest_play is None or parsed > newest_play:
                newest_play = parsed

    if row_count == 0:
        raise ValueError("CSV has a header but no data rows.")
    if newest_play is None:
        raise ValueError("CSV rows were read, but no parseable play timestamps were found.")

    return row_count, newest_play


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate freshness of Apple privacy export Track Play History CSV."
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"Path to Apple Music Track Play History CSV (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=45,
        help="Fail if newest play is older than this many days (default: 45).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    csv_path = args.csv_path.expanduser()
    now_utc = datetime.now(UTC)

    try:
        row_count, newest_play = analyze_export(csv_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    age_days = (now_utc - newest_play).days
    newest_play_iso = newest_play.isoformat().replace("+00:00", "Z")

    print(f"CSV: {csv_path}")
    print(f"Rows: {row_count}")
    print(f"Newest play: {newest_play_iso}")
    print(f"Age (days): {age_days}")

    if age_days > args.max_age_days:
        print(
            "ERROR: Apple Music play history export is stale.\n"
            f"Newest play is {newest_play_iso}, older than {args.max_age_days} days.\n"
            "Action: request a fresh Apple Music export from privacy.apple.com "
            "(Data & Privacy > Get a copy of your data > Apple Media Services information), "
            "then replace the CSV and rerun ingestion.",
            file=sys.stderr,
        )
        return 2

    print("Freshness check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
