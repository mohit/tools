import argparse
import sys
import webbrowser
import zipfile
from datetime import UTC, datetime
from pathlib import Path


DEFAULT_RAW_ROOT = Path.home() / "datalake.me" / "raw" / "apple-music"
DEFAULT_DOWNLOADS = Path.home() / "Downloads"
PRIVACY_EXPORT_URL = "https://privacy.apple.com/account"


def _find_latest_zip(downloads_dir: Path) -> Path | None:
    zips = sorted(downloads_dir.glob("*.zip"), key=lambda path: path.stat().st_mtime, reverse=True)
    return zips[0] if zips else None


def _extract_play_activity(zip_path: Path, output_root: Path) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d")
    target_dir = output_root / stamp
    target_dir.mkdir(parents=True, exist_ok=True)

    extracted = None
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if "Play Activity" in name and name.lower().endswith(".csv"):
                filename = Path(name).name
                out_path = target_dir / filename
                with zf.open(name) as src, out_path.open("wb") as dst:
                    dst.write(src.read())
                extracted = out_path
                break

    if not extracted:
        raise FileNotFoundError(
            "Could not find a Play Activity CSV in the Apple privacy export zip. "
            "Expected a file with 'Play Activity' in its name."
        )

    return extracted


def _open_privacy_portal(use_selenium: bool) -> None:
    if not use_selenium:
        webbrowser.open(PRIVACY_EXPORT_URL)
        return

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        print("Selenium not installed; opening browser with default webbrowser module.")
        webbrowser.open(PRIVACY_EXPORT_URL)
        return

    options = Options()
    # Keep browser visible for interactive login/2FA.
    driver = webdriver.Chrome(options=options)
    driver.get(PRIVACY_EXPORT_URL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Helper for Apple privacy export workflow. Opens privacy.apple.com and optionally "
            "extracts Apple Music Play Activity CSV from a downloaded zip."
        )
    )
    parser.add_argument("--raw-root", type=Path, default=Path(str(DEFAULT_RAW_ROOT)))
    parser.add_argument("--downloads-dir", type=Path, default=Path(str(DEFAULT_DOWNLOADS)))
    parser.add_argument(
        "--zip-file",
        type=Path,
        default=None,
        help="Path to a downloaded privacy export zip (skip browser step).",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open privacy.apple.com to start a manual export request.",
    )
    parser.add_argument(
        "--use-selenium",
        action="store_true",
        help="Try Selenium to open browser; falls back to webbrowser if unavailable.",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Extract Play Activity CSV from the provided zip or latest Downloads zip.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = Path(args.raw_root).expanduser()
    downloads_dir = Path(args.downloads_dir).expanduser()
    zip_file = Path(args.zip_file).expanduser() if args.zip_file else None

    if args.open_browser:
        _open_privacy_portal(use_selenium=args.use_selenium)
        print(
            "Opened privacy.apple.com. Complete login + export request manually. "
            "When Apple emails your export zip, re-run with --extract."
        )

    if args.extract:
        selected_zip = zip_file or _find_latest_zip(downloads_dir)
        if not selected_zip:
            raise FileNotFoundError(f"No zip file found in {downloads_dir}")
        if not selected_zip.exists():
            raise FileNotFoundError(f"Zip file not found: {selected_zip}")

        extracted = _extract_play_activity(selected_zip, raw_root)
        print(f"Extracted Play Activity CSV: {extracted}")
        print("Next step: run apple_music_processor.py to refresh curated parquet.")

    if not args.open_browser and not args.extract:
        print(
            "Nothing to do. Use --open-browser, --extract, or both.\n"
            "Example:\n"
            "  python apple_music_export_helper.py --open-browser\n"
            "  python apple_music_export_helper.py --extract"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
