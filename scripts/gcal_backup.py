#!/usr/bin/env python3
"""Weekly Google Calendar backup via gog CLI.

Exports configured calendars to dated JSON snapshots:
~/datalake.me/raw/google-calendar/<calendar-name>/YYYY-MM-DD.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

CALENDARS = [
    {
        "name": "mohit.ed@gmail.com",
        "folder": "mohit.ed@gmail.com",
        "id": "primary",
    },
    {
        "name": "Burp",
        "folder": "Burp",
        "id": "59su84rs0gglunof4pp3dlb4p8@group.calendar.google.com",
    },
    {
        "name": "Family",
        "folder": "Family",
        "id": "family13862746254541864797@group.calendar.google.com",
    },
    {
        "name": "Tarzan",
        "folder": "Tarzan",
        "id": "bhb2sf2i6f9cdv8bnv46roov60@group.calendar.google.com",
    },
    {
        "name": "Work-ish",
        "folder": "Work-ish",
        "id": "dqrf1vd22nvspd6jbpdrt0bikg@group.calendar.google.com",
    },
]

ALLOWED_ACCESS_ROLES = {"owner", "writer"}
TOKEN_KEYS = ("nextPageToken", "next_page_token", "nextPage", "next_page")
ITEM_KEYS = ("items", "events")


def parse_args() -> argparse.Namespace:
    default_account = os.environ.get("GOG_ACCOUNT", "mohit.ed@gmail.com")
    default_root = Path.home() / "datalake.me" / "raw" / "google-calendar"
    parser = argparse.ArgumentParser(
        description="Back up configured Google Calendars into dated JSON snapshots."
    )
    parser.add_argument(
        "--account",
        default=default_account,
        help="Google account for gog auth (default: %(default)s).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=default_root,
        help="Snapshot root path (default: %(default)s).",
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Snapshot date stamp (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--max-events-page",
        type=int,
        default=2500,
        help="Max events per gog events request page (default: %(default)s).",
    )
    parser.add_argument(
        "--no-access-check",
        action="store_true",
        help="Skip owner/writer access role validation.",
    )
    return parser.parse_args()


def run_gog_json(args: list[str], account: str) -> Any:
    cmd = [
        "gog",
        "--json",
        "--no-input",
        "--account",
        account,
        *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr.strip() or proc.stdout.strip() or "unknown gog error"
        raise RuntimeError(f"gog command failed: {' '.join(cmd)}\n{err}")
    out = proc.stdout.strip()
    if not out:
        return {}
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"gog output was not valid JSON for command: {' '.join(cmd)}"
        ) from exc


def extract_items_and_token(payload: Any) -> tuple[list[dict[str, Any]], str | None]:
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict):
        items: Any = None
        for key in ITEM_KEYS:
            if key in payload:
                items = payload[key]
                break
        if items is None:
            if payload:
                return [payload], None
            return [], None
        if not isinstance(items, list):
            raise RuntimeError("Unexpected gog JSON shape: items is not a list")
        token = None
        for key in TOKEN_KEYS:
            value = payload.get(key)
            if value:
                token = str(value)
                break
        return items, token
    raise RuntimeError(f"Unexpected gog JSON payload type: {type(payload)}")


def fetch_paged(command: list[str], account: str, max_results: int) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    seen_tokens: set[str] = set()
    page_token: str | None = None

    while True:
        args = [*command, "--max", str(max_results)]
        if page_token:
            args.extend(["--page", page_token])
        payload = run_gog_json(args=args, account=account)
        items, next_token = extract_items_and_token(payload)
        all_items.extend(items)
        if not next_token:
            break
        if next_token in seen_tokens:
            raise RuntimeError("Pagination loop detected from gog next page token")
        seen_tokens.add(next_token)
        page_token = next_token

    return all_items


def safe_folder_name(value: str) -> str:
    value = value.strip().replace("/", "_")
    return value or "calendar"


def calendar_access_map(account: str) -> dict[str, str]:
    calendars = fetch_paged(
        command=["calendar", "calendars"], account=account, max_results=250
    )
    access: dict[str, str] = {}
    for item in calendars:
        calendar_id = item.get("id")
        role = item.get("accessRole") or item.get("access_role")
        if calendar_id and isinstance(role, str):
            access[str(calendar_id)] = role.lower()
    return access


def write_snapshot(
    output_root: Path,
    snapshot_date: str,
    account: str,
    calendar_name: str,
    calendar_folder: str,
    calendar_id: str,
    events: list[dict[str, Any]],
) -> Path:
    output_dir = output_root / calendar_folder
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{snapshot_date}.json"

    payload = {
        "backup_date": snapshot_date,
        "account": account,
        "calendar": {
            "name": calendar_name,
            "id": calendar_id,
            "folder": calendar_folder,
        },
        "event_count": len(events),
        "events": events,
    }
    output_file.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return output_file


def main() -> int:
    args = parse_args()

    if shutil.which("gog") is None:
        print("ERROR: gog CLI not found on PATH.", file=sys.stderr)
        return 1

    if not re.match(r"^\d{4}-\d{2}-\d{2}$", args.date):
        print("ERROR: --date must be in YYYY-MM-DD format.", file=sys.stderr)
        return 1

    configured = [
        {
            **calendar,
            "folder": safe_folder_name(calendar["folder"] or calendar["name"]),
        }
        for calendar in CALENDARS
    ]

    access = {}
    if not args.no_access_check:
        try:
            access = calendar_access_map(account=args.account)
        except Exception as exc:  # noqa: BLE001
            print(
                f"WARNING: could not validate calendar access roles: {exc}",
                file=sys.stderr,
            )

    failures = 0
    skipped = 0
    for calendar in configured:
        calendar_id = calendar["id"]
        if access:
            role = access.get(calendar_id)
            if calendar_id == "primary":
                role = role or access.get(args.account)
            if role and role not in ALLOWED_ACCESS_ROLES:
                skipped += 1
                print(
                    f"SKIP: {calendar['name']} ({calendar_id}) has access role '{role}'."
                )
                continue

        try:
            events = fetch_paged(
                command=["calendar", "events", calendar_id],
                account=args.account,
                max_results=args.max_events_page,
            )
            path = write_snapshot(
                output_root=args.output_root.expanduser(),
                snapshot_date=args.date,
                account=args.account,
                calendar_name=calendar["name"],
                calendar_folder=calendar["folder"],
                calendar_id=calendar_id,
                events=events,
            )
            print(f"OK: wrote {len(events)} events -> {path}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(
                f"ERROR: failed backing up {calendar['name']} ({calendar_id}): {exc}",
                file=sys.stderr,
            )

    if failures:
        print(
            f"Completed with {failures} failure(s) and {skipped} skipped calendar(s).",
            file=sys.stderr,
        )
        return 1

    print(f"Completed successfully. Skipped calendars: {skipped}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
