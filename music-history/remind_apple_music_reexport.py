#!/usr/bin/env python3
"""Reminder/check utility for stale Apple Music privacy exports."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass(frozen=True)
class AppleMusicSnapshot:
    location: Path
    rows: int
    range_start: date
    range_end: date
    latest_play_date: date
    source_export_date: date
    last_ingested: date
    health_latest: date
    health_age_days: int


SNAPSHOT = AppleMusicSnapshot(
    location=Path("~/datalake.me/raw/apple-music/").expanduser(),
    rows=88_949,
    range_start=date(2015, 7, 2),
    range_end=date(2023, 11, 9),
    latest_play_date=date(2023, 11, 5),
    source_export_date=date(2023, 11, 10),
    last_ingested=date(2026, 2, 7),
    health_latest=date(2026, 1, 20),
    health_age_days=19,
)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_message(
    *,
    today: date,
    latest_play_date: date,
    threshold_days: int,
    snapshot: AppleMusicSnapshot,
) -> tuple[str, bool]:
    age_days = (today - latest_play_date).days
    is_stale = age_days >= threshold_days
    status = "STALE" if is_stale else "FRESH"
    header = f"[{status}] Apple Music export freshness check"

    lines = [
        header,
        "",
        f"- Today: {today.isoformat()}",
        f"- Data location: {snapshot.location}",
        f"- Rows in last known snapshot: {snapshot.rows:,}",
        f"- Last known data range: {snapshot.range_start.isoformat()} to {snapshot.range_end.isoformat()}",
        f"- Latest play date: {latest_play_date.isoformat()} ({age_days} days old)",
        f"- Source privacy export date: {snapshot.source_export_date.isoformat()}",
        f"- Last ingested: {snapshot.last_ingested.isoformat()}",
        f"- Staleness threshold: {threshold_days} days",
        "",
        "Action:",
        "- Request a new Apple Music data export at https://privacy.apple.com/.",
        "- Apple usually takes a few days to prepare the export.",
        "",
        "Note:",
        (
            f"- Apple Health export is currently fine "
            f"(latest {snapshot.health_latest.isoformat()}, "
            f"{snapshot.health_age_days} days old in the issue context)."
        ),
    ]
    return "\n".join(lines) + "\n", is_stale


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check Apple Music export freshness against the known stale snapshot "
            "and print a re-export reminder."
        )
    )
    parser.add_argument(
        "--today",
        type=parse_date,
        default=date.today(),
        help="Override today's date (YYYY-MM-DD) for reproducible checks.",
    )
    parser.add_argument(
        "--latest-play-date",
        type=parse_date,
        default=SNAPSHOT.latest_play_date,
        help="Override latest known play date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--threshold-days",
        type=int,
        default=365,
        help="Mark data as stale when age is >= threshold days.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional file path to write the reminder output.",
    )
    args = parser.parse_args()

    message, is_stale = build_message(
        today=args.today,
        latest_play_date=args.latest_play_date,
        threshold_days=args.threshold_days,
        snapshot=SNAPSHOT,
    )

    if args.output:
        args.output.write_text(message)
    else:
        print(message, end="")

    return 1 if is_stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
