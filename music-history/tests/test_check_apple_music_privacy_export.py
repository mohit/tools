from datetime import UTC, datetime
from pathlib import Path

import pytest

import check_apple_music_privacy_export as checker
from check_apple_music_privacy_export import analyze_export, main


def write_csv(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def test_analyze_export_reads_row_count_and_newest_play(tmp_path: Path) -> None:
    csv_path = tmp_path / "Apple Music - Track Play History.csv"
    write_csv(
        csv_path,
        "Track Name,Play Date UTC\n"
        "Song A,2025-12-01T01:02:03Z\n"
        "Song B,2026-02-10T22:00:00Z\n",
    )

    row_count, newest_play = analyze_export(csv_path)

    assert row_count == 2
    assert newest_play == datetime(2026, 2, 10, 22, 0, 0, tzinfo=UTC)


def test_main_returns_success_for_fresh_data(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    csv_path = tmp_path / "Apple Music - Track Play History.csv"
    write_csv(
        csv_path,
        "Track Name,Play Date UTC\n"
        "Song A,2026-02-10T22:00:00Z\n",
    )

    exit_code = main(["--csv-path", str(csv_path), "--max-age-days", "3650"])

    out = capsys.readouterr()
    assert exit_code == 0
    assert "Freshness check passed." in out.out


def test_main_returns_stale_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    csv_path = tmp_path / "Apple Music - Track Play History.csv"
    write_csv(
        csv_path,
        "Track Name,Play Date UTC\n"
        "Song A,2023-11-05T03:22:36Z\n",
    )

    exit_code = main(["--csv-path", str(csv_path), "--max-age-days", "7"])

    out = capsys.readouterr()
    assert exit_code == 2
    assert "Apple Music play history export is stale." in out.err
    assert "2023-11-05T03:22:36Z" in out.err
    assert "privacy.apple.com" in out.err


def test_main_returns_error_for_missing_file(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"

    exit_code = main(["--csv-path", str(missing)])

    out = capsys.readouterr()
    assert exit_code == 1
    assert "Apple Music export not found" in out.err


def test_analyze_export_errors_when_date_column_missing(tmp_path: Path) -> None:
    csv_path = tmp_path / "Apple Music - Track Play History.csv"
    write_csv(
        csv_path,
        "Track Name,Artist Name\n"
        "Song A,Artist A\n",
    )

    with pytest.raises(ValueError, match="Could not find a play-date column"):
        analyze_export(csv_path)


def test_main_returns_error_for_directory_csv_path(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    csv_dir = tmp_path / "not-a-file"
    csv_dir.mkdir()

    exit_code = main(["--csv-path", str(csv_dir)])

    out = capsys.readouterr()
    assert exit_code == 1
    assert "ERROR:" in out.err


def test_main_returns_error_for_permission_denied(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def raise_permission_error(_: Path) -> tuple[int, datetime]:
        raise PermissionError("Permission denied")

    monkeypatch.setattr(checker, "analyze_export", raise_permission_error)

    exit_code = main(["--csv-path", "irrelevant.csv"])

    out = capsys.readouterr()
    assert exit_code == 1
    assert "ERROR: Permission denied" in out.err


def test_main_uses_full_timedelta_precision_for_staleness(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    csv_path = tmp_path / "Apple Music - Track Play History.csv"
    newest_play = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    write_csv(
        csv_path,
        f"Track Name,Play Date UTC\nSong A,{newest_play.isoformat().replace('+00:00', 'Z')}\n",
    )

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return datetime(2026, 2, 15, 12, 0, 0, tzinfo=UTC)

    monkeypatch.setattr(checker, "datetime", FixedDateTime)

    exit_code = main(["--csv-path", str(csv_path), "--max-age-days", "45"])

    out = capsys.readouterr()
    assert exit_code == 2
    assert "Apple Music play history export is stale." in out.err
