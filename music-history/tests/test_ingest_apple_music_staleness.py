"""Tests for raw-CSV mtime-based staleness detection in ingest_apple_music."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

import ingest_apple_music as ingest_mod
from ingest_apple_music import check_raw_csv_staleness


# ---------------------------------------------------------------------------
# check_raw_csv_staleness
# ---------------------------------------------------------------------------


def _write_csv(path: Path, content: str = "header\n") -> None:
    path.write_text(content)


def test_returns_none_when_directory_has_no_csvs(tmp_path: Path) -> None:
    age_days, stale = check_raw_csv_staleness(tmp_path, threshold_days=30)
    assert age_days is None
    assert stale == []


def test_returns_none_when_directory_does_not_exist(tmp_path: Path) -> None:
    missing = tmp_path / "no-such-dir"
    age_days, stale = check_raw_csv_staleness(missing, threshold_days=30)
    assert age_days is None
    assert stale == []


def test_fresh_csv_returns_no_stale_files(tmp_path: Path) -> None:
    csv = tmp_path / "Apple Music - Track Play History.csv"
    _write_csv(csv)
    now = time.time()
    # File was just written — age is ~0 days, well within 30-day threshold.
    age_days, stale = check_raw_csv_staleness(tmp_path, threshold_days=30, _now=now)
    assert age_days is not None
    assert age_days < 1
    assert stale == []


def test_stale_csv_is_detected(tmp_path: Path) -> None:
    csv = tmp_path / "Apple Music - Track Play History.csv"
    _write_csv(csv)
    # Simulate 128 days of staleness by shifting "now" forward.
    fake_now = time.time() + 128 * 86400
    age_days, stale = check_raw_csv_staleness(tmp_path, threshold_days=30, _now=fake_now)
    assert age_days is not None
    assert age_days > 127
    assert csv in stale


def test_age_computed_from_newest_csv(tmp_path: Path) -> None:
    old_csv = tmp_path / "old.csv"
    new_csv = tmp_path / "new.csv"
    _write_csv(old_csv)
    _write_csv(new_csv)
    # Make old_csv appear much older by adjusting fake_now relative to new_csv.
    # new_csv was just written; advance "now" by 10 days — still fresh.
    fake_now = time.time() + 10 * 86400
    age_days, stale = check_raw_csv_staleness(tmp_path, threshold_days=30, _now=fake_now)
    # Newest file is ~10 days old — below threshold.
    assert age_days is not None
    assert age_days < 11
    assert stale == []


def test_non_csv_files_are_ignored(tmp_path: Path) -> None:
    (tmp_path / "README.txt").write_text("ignored")
    (tmp_path / "data.json").write_text("{}")
    age_days, stale = check_raw_csv_staleness(tmp_path, threshold_days=30)
    assert age_days is None
    assert stale == []


# ---------------------------------------------------------------------------
# ingest() — staleness warning / strict-freshness behaviour
# ---------------------------------------------------------------------------


def test_ingest_warns_and_continues_when_stale(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """By default, stale raw files emit a WARNING and do NOT abort."""
    csv = tmp_path / "Apple Music - Track Play History.csv"
    _write_csv(csv)
    fake_now = time.time() + 128 * 86400

    monkeypatch.setattr(ingest_mod, "RAW_APPLE_MUSIC_DIR", tmp_path)
    monkeypatch.setattr(ingest_mod, "RAW_FILE_STALENESS_DAYS", 30)

    # Patch check_raw_csv_staleness to return stale result without relying on fs timestamps.
    monkeypatch.setattr(
        ingest_mod,
        "check_raw_csv_staleness",
        lambda raw_dir, threshold_days, **kwargs: (128.0, [csv]),
    )

    # Prevent the rest of ingest() from running (no real JSONL / DuckDB available).
    def _fake_ingest_body(*_, **__):  # noqa: ANN001, ANN002, ANN003
        raise StopIteration("body reached")

    # Patch duckdb.connect to stop early — we only care about the warning path.
    import duckdb as _duckdb
    monkeypatch.setattr(_duckdb, "connect", lambda *a, **kw: (_ for _ in ()).throw(StopIteration("duckdb")))

    # Also stub out the metadata check so it passes cleanly.
    from apple_music_export_guard import AppleMusicExportMetadata
    from datetime import date
    fake_metadata = AppleMusicExportMetadata(
        last_export_date=date.today(),
        latest_play_date=date.today(),
        source="privacy.apple.com",
        status=None,
        issue=None,
    )
    monkeypatch.setattr(
        ingest_mod,
        "check_export_freshness",
        lambda *a, **kw: (fake_metadata, 0),
    )
    # Prevent actual JSONL path check
    monkeypatch.setattr(ingest_mod, "JSONL_PATH", tmp_path / "dummy.jsonl")
    monkeypatch.setattr(ingest_mod, "CURATED_ROOT", tmp_path / "curated")

    with pytest.raises(StopIteration):
        ingest_mod.ingest(strict_freshness=False)

    captured = capsys.readouterr()
    assert "WARNING:" in captured.out
    assert "privacy.apple.com" in captured.out
    assert "128.0 days old" in captured.out


def test_ingest_aborts_with_strict_freshness_when_stale(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With --strict-freshness, stale raw files cause SystemExit(2)."""
    csv = tmp_path / "Apple Music - Track Play History.csv"
    _write_csv(csv)

    monkeypatch.setattr(ingest_mod, "RAW_APPLE_MUSIC_DIR", tmp_path)
    monkeypatch.setattr(ingest_mod, "RAW_FILE_STALENESS_DAYS", 30)
    monkeypatch.setattr(
        ingest_mod,
        "check_raw_csv_staleness",
        lambda raw_dir, threshold_days, **kwargs: (128.0, [csv]),
    )

    with pytest.raises(SystemExit) as exc_info:
        ingest_mod.ingest(strict_freshness=True)

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "ERROR:" in captured.out
    assert "privacy.apple.com" in captured.out


def test_ingest_no_warning_when_fresh(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh raw files produce no staleness warning."""
    monkeypatch.setattr(ingest_mod, "RAW_APPLE_MUSIC_DIR", tmp_path)
    monkeypatch.setattr(ingest_mod, "RAW_FILE_STALENESS_DAYS", 30)
    monkeypatch.setattr(
        ingest_mod,
        "check_raw_csv_staleness",
        lambda raw_dir, threshold_days, **kwargs: (5.0, []),
    )

    from apple_music_export_guard import AppleMusicExportMetadata
    from datetime import date
    fake_metadata = AppleMusicExportMetadata(
        last_export_date=date.today(),
        latest_play_date=date.today(),
        source="privacy.apple.com",
        status=None,
        issue=None,
    )
    monkeypatch.setattr(
        ingest_mod,
        "check_export_freshness",
        lambda *a, **kw: (fake_metadata, 0),
    )
    monkeypatch.setattr(ingest_mod, "JSONL_PATH", tmp_path / "dummy.jsonl")
    monkeypatch.setattr(ingest_mod, "CURATED_ROOT", tmp_path / "curated")

    import duckdb as _duckdb
    monkeypatch.setattr(_duckdb, "connect", lambda *a, **kw: (_ for _ in ()).throw(StopIteration("duckdb")))

    with pytest.raises(StopIteration):
        ingest_mod.ingest(strict_freshness=False)

    captured = capsys.readouterr()
    assert "WARNING:" not in captured.out
    assert "5.0 days" in captured.out


# ---------------------------------------------------------------------------
# main() — CLI argument parsing
# ---------------------------------------------------------------------------


def test_main_parses_strict_freshness_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_ingest(strict_freshness: bool = False) -> None:
        calls.append({"strict_freshness": strict_freshness})

    monkeypatch.setattr(ingest_mod, "ingest", fake_ingest)

    ingest_mod.main(["--strict-freshness"])
    assert calls == [{"strict_freshness": True}]


def test_main_defaults_to_no_strict_freshness(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_ingest(strict_freshness: bool = False) -> None:
        calls.append({"strict_freshness": strict_freshness})

    monkeypatch.setattr(ingest_mod, "ingest", fake_ingest)

    ingest_mod.main([])
    assert calls == [{"strict_freshness": False}]
