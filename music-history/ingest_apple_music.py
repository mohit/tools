import argparse
import time
from pathlib import Path

import duckdb
from apple_music_export_guard import (
    AppleMusicExportGuardError,
    check_export_freshness,
)
from config import (
    CATALOG_DB,
    EXPORT_METADATA_PATH,
    MAX_STALENESS_DAYS,
    RAW_APPLE_MUSIC_DIR,
    RAW_FILE_STALENESS_DAYS,
)

# Config
from config import CURATED_ROOT as _CURATED_BASE

JSONL_PATH = Path("apple_music_history.jsonl").absolute()
CURATED_ROOT = _CURATED_BASE / "apple_music/library"


def check_raw_csv_staleness(
    raw_dir: Path,
    threshold_days: int,
    *,
    _now: float | None = None,
) -> tuple[float | None, list[Path]]:
    """Check modification time of CSV files in *raw_dir*.

    Returns ``(age_days, stale_files)`` where *age_days* is the age of the
    *newest* CSV file (based on mtime), or ``None`` when no CSV files are
    found.  *stale_files* is the list of files whose mtime exceeds
    *threshold_days*.
    """
    csv_files = list(raw_dir.glob("*.csv"))
    if not csv_files:
        return None, []

    now = _now if _now is not None else time.time()
    mtimes = {p: p.stat().st_mtime for p in csv_files}
    newest_mtime = max(mtimes.values())
    age_days = (now - newest_mtime) / 86400

    stale_files: list[Path] = []
    if age_days > threshold_days:
        stale_files = [p for p, mtime in mtimes.items() if (now - mtime) / 86400 > threshold_days]

    return age_days, stale_files


def _emit_staleness_warning(age_days: float, threshold_days: int, strict: bool) -> None:
    msg = (
        f"Apple Music raw export files are stale "
        f"({age_days:.1f} days old, threshold {threshold_days} days). "
        "Request a new export from privacy.apple.com "
        "(Data & Privacy > Get a copy of your data > Apple Media Services "
        "information), then replace the raw files and rerun ingestion."
    )
    if strict:
        print(f"ERROR: {msg}")
        raise SystemExit(2)
    print(f"WARNING: {msg}")


def ingest(strict_freshness: bool = False) -> None:
    # 1. Check raw CSV file modification times
    if RAW_APPLE_MUSIC_DIR.exists():
        age_days, stale_files = check_raw_csv_staleness(
            RAW_APPLE_MUSIC_DIR, RAW_FILE_STALENESS_DAYS
        )
        if age_days is None:
            print(f"WARNING: No CSV files found in raw directory: {RAW_APPLE_MUSIC_DIR}")
        else:
            print(
                f"Raw Apple Music CSV age: {age_days:.1f} days "
                f"(threshold: {RAW_FILE_STALENESS_DAYS} days)"
            )
            if stale_files:
                _emit_staleness_warning(age_days, RAW_FILE_STALENESS_DAYS, strict_freshness)
    else:
        print(f"WARNING: Raw Apple Music directory not found: {RAW_APPLE_MUSIC_DIR}")

    # 2. Check metadata-based freshness
    try:
        metadata, age_days_meta = check_export_freshness(
            EXPORT_METADATA_PATH, max_staleness_days=MAX_STALENESS_DAYS
        )
        print(
            "Apple Music export freshness check passed: "
            f"latest_play_date={metadata.latest_play_date.isoformat()} "
            f"last_export_date={metadata.last_export_date.isoformat()} "
            f"age_days={age_days_meta}"
        )
    except AppleMusicExportGuardError as exc:
        msg = str(exc)
        if strict_freshness:
            print(f"ERROR: {msg}")
            raise SystemExit(2) from exc
        print(f"WARNING: {msg}")

    print(f"Ingesting {JSONL_PATH}...")

    # Ensure target directory exists
    CURATED_ROOT.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(CATALOG_DB))

    # 1. Read JSONL and Transform
    # We parse the ISO timestamp.
    # We handle null played_at by defaulting to epoch 0 or staying null (and partitioning accordingly).

    query = f"""
    SELECT
        name as track,
        artist,
        album,
        play_count,
        TRY_CAST(played_at AS TIMESTAMP) as played_at_utc,
        source,
        CASE
            WHEN played_at IS NOT NULL THEN strftime(TRY_CAST(played_at AS TIMESTAMP), '%Y')
            ELSE 'unknown'
        END as year,
        CASE
            WHEN played_at IS NOT NULL THEN strftime(TRY_CAST(played_at AS TIMESTAMP), '%m')
            ELSE 'unknown'
        END as month,
        CAST({int(time.time())} AS BIGINT) as ingested_at
    FROM read_json_auto('{JSONL_PATH}')
    """

    # 2. Write to Parquet (Partitioned)
    print(f"Writing partitioned parquet to {CURATED_ROOT}...")
    con.sql(f"""
        COPY ({query})
        TO '{CURATED_ROOT}'
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE)
    """)

    # 3. Create a View in the Catalog
    print("Updating Catalog View...")
    con.sql(f"""
        CREATE OR REPLACE VIEW apple_music_library AS
        SELECT * EXCLUDE (year, month)
        FROM read_parquet('{CURATED_ROOT}/year=*/month=*/*.parquet', hive_partitioning=true)
    """)

    # Verify count
    count = con.sql("SELECT COUNT(*) FROM apple_music_library").fetchone()[0]
    print(f"Total tracks in Apple Music catalog: {count}")

    con.close()
    print("Done.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Apple Music export data into the datalake."
    )
    parser.add_argument(
        "--strict-freshness",
        action="store_true",
        default=False,
        help=(
            "Abort ingestion when raw CSV files or export metadata are stale "
            f"(default: warn and continue; raw threshold {RAW_FILE_STALENESS_DAYS} days)"
        ),
    )
    args = parser.parse_args(argv)
    ingest(strict_freshness=args.strict_freshness)


if __name__ == "__main__":
    main()
