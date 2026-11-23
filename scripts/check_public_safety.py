#!/usr/bin/env python3
"""Fail if tracked files look like they contain personal data or secrets.

This repository is public, but its tools handle personal data. Run in CI on every push and
pull request:

    python3 scripts/check_public_safety.py

It checks every file tracked by git for:
- data files (exports, databases, archives, listening/location histories) outside test fixtures
- email addresses outside placeholder domains
- Google Calendar IDs, real-looking tailnet hostnames and Tailscale IPs
- common secret formats (cloud keys, GitHub/Anthropic/OpenAI tokens, private keys)

A finding can be allowed on one line with the comment marker ``public-safety: ok``. Use it only
for values that are genuinely fake. Never add personal values to this file: it is public too.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ALLOW_MARK = "public-safety: ok"

DATA_SUFFIXES = {
    ".csv", ".tsv", ".jsonl", ".ndjson", ".parquet", ".duckdb", ".db", ".sqlite", ".sqlite3",
    ".gpx", ".fit", ".tcx", ".kml", ".har", ".zip", ".gz", ".tar", ".7z", ".mbox", ".eml",
    ".heic", ".mov", ".mp4", ".p12", ".pem", ".key", ".env",
}
# Synthetic fixtures live here; anything else with a data suffix needs a look.
FIXTURE_DIR_RE = re.compile(r"(^|/)(tests?|fixtures?|testdata)(/|$)")

PLACEHOLDER_EMAIL_DOMAINS = {
    "example.com", "example.org", "example.net", "users.noreply.github.com", "anthropic.com",
}
PLACEHOLDER_HOST_LABELS = {"your-tailnet", "tailnet", "example", "<tailnet>", "machine", "host"}

PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("email address", re.compile(r"\b[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+\.)+[A-Za-z]{2,}\b")),
    ("Google Calendar ID", re.compile(r"\b[a-z0-9]{10,}@group\.calendar\.google\.com\b")),
    ("tailnet hostname", re.compile(r"\b[a-z0-9-]+\.([a-z0-9-]+)\.ts\.net\b")),
    ("Tailscale IP", re.compile(r"\b100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}\b")),
    ("AWS access key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b")),
    ("Anthropic/OpenAI key", re.compile(r"\bsk-(ant-)?[A-Za-z0-9_-]{24,}\b")),
    ("Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b")),
    ("Google API key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("private key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
]


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True
    ).stdout
    return [p for p in out.decode().split("\0") if p]


def is_finding(kind: str, match: re.Match[str]) -> bool:
    if kind == "email address":
        domain = match.group(0).rsplit("@", 1)[1].lower()
        if domain.endswith("group.calendar.google.com"):
            return False  # reported as a calendar ID
        return domain not in PLACEHOLDER_EMAIL_DOMAINS and not domain.endswith(".example")
    if kind == "tailnet hostname":
        return match.group(1) not in PLACEHOLDER_HOST_LABELS
    if kind == "Tailscale IP":
        return match.group(0) != "100.64.0.1"  # documentation placeholder used in tests
    return True


def scan(root: Path) -> list[str]:
    problems: list[str] = []
    for rel in tracked_files(root):
        path = root / rel
        if Path(rel).suffix.lower() in DATA_SUFFIXES and not FIXTURE_DIR_RE.search(rel):
            problems.append(f"{rel}: data file outside tests/fixtures (keep personal data in the datalake)")
        if rel.endswith(("package-lock.json", "uv.lock")) or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue  # binary
        for lineno, line in enumerate(text.splitlines(), 1):
            if ALLOW_MARK in line:
                continue
            for kind, pattern in PATTERNS:
                for match in pattern.finditer(line):
                    if is_finding(kind, match):
                        problems.append(f"{rel}:{lineno}: {kind}")
    return problems


def main() -> int:
    root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True
        ).stdout.strip()
    )
    problems = scan(root)
    if problems:
        print("Possible personal data or secrets in a public repository:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        print(
            f"\nMove real values to the datalake or environment variables. For genuinely fake "
            f"values, add '{ALLOW_MARK}' on that line.",
            file=sys.stderr,
        )
        return 1
    print("public-safety: no findings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
