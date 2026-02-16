from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path


class AppleMusicExportGuardError(RuntimeError):
    """Raised when Apple Music export freshness checks fail."""


@dataclass(frozen=True)
class AppleMusicExportMetadata:
    last_export_date: date
    latest_play_date: date
    source: str
    status: str | None
    issue: int | None


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise AppleMusicExportGuardError(
            f"Invalid {field_name} '{value}'. Expected YYYY-MM-DD."
        ) from exc


def load_export_metadata(metadata_path: Path) -> AppleMusicExportMetadata:
    if not metadata_path.exists():
        raise AppleMusicExportGuardError(
            f"Missing Apple Music export metadata file: {metadata_path}"
        )

    try:
        raw = json.loads(metadata_path.read_text())
    except json.JSONDecodeError as exc:
        raise AppleMusicExportGuardError(
            f"Could not parse JSON metadata file: {metadata_path}"
        ) from exc

    required = ["last_export_date", "latest_play_date", "source"]
    missing = [key for key in required if key not in raw]
    if missing:
        missing_str = ", ".join(missing)
        raise AppleMusicExportGuardError(
            f"Metadata file {metadata_path} is missing required keys: {missing_str}"
        )

    return AppleMusicExportMetadata(
        last_export_date=_parse_iso_date(raw["last_export_date"], "last_export_date"),
        latest_play_date=_parse_iso_date(raw["latest_play_date"], "latest_play_date"),
        source=str(raw["source"]),
        status=str(raw.get("status")) if raw.get("status") is not None else None,
        issue=int(raw["issue"]) if raw.get("issue") is not None else None,
    )


def check_export_freshness(
    metadata_path: Path,
    *,
    max_staleness_days: int = 365,
    today: date | None = None,
) -> tuple[AppleMusicExportMetadata, int]:
    metadata = load_export_metadata(metadata_path)
    reference_day = today or date.today()

    age_days = (reference_day - metadata.latest_play_date).days
    if age_days < 0:
        return metadata, age_days

    if age_days > max_staleness_days:
        issue_hint = (
            f" (issue #{metadata.issue})" if metadata.issue is not None else ""
        )
        raise AppleMusicExportGuardError(
            "Apple Music export is stale. "
            f"Latest play date: {metadata.latest_play_date.isoformat()} "
            f"({age_days} days old). "
            f"Last export date: {metadata.last_export_date.isoformat()}. "
            "Request a new export from privacy.apple.com, replace the raw Apple Music "
            "data, then update apple_music_export_metadata.json"
            f"{issue_hint}."
        )

    return metadata, age_days


def enforce_fresh_export_or_raise(
    metadata_path: Path,
    *,
    max_staleness_days: int = 365,
) -> tuple[AppleMusicExportMetadata, int]:
    return check_export_freshness(
        metadata_path,
        max_staleness_days=max_staleness_days,
    )
