import argparse
import csv
import shutil
import time
from pathlib import Path

import duckdb


DEFAULT_RAW_ROOT = Path.home() / "datalake.me" / "raw" / "apple-music"
DEFAULT_CURATED_ROOT = Path.home() / "datalake.me" / "curated" / "apple-music" / "play-activity"


TRACK_COLUMNS = [
    "Track Description",
    "Track Name",
    "Song Name",
    "Name",
]
ARTIST_COLUMNS = [
    "Artist Name",
    "Artist",
]
ALBUM_COLUMNS = [
    "Container Description",
    "Album Name",
    "Album",
]
PLAYED_AT_COLUMNS = [
    "Event Start Timestamp",
    "Play Date UTC",
    "Last Played Date",
    "Event Start Date",
]
PLAY_COUNT_COLUMNS = [
    "Play Count",
]


def discover_csv(raw_root: Path, explicit_file: Path | None) -> Path:
    if explicit_file:
        if not explicit_file.exists():
            raise FileNotFoundError(f"CSV file not found: {explicit_file}")
        return explicit_file

    candidates = sorted(
        raw_root.rglob("*Play Activity*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No Apple Music Play Activity CSV found under {raw_root}. "
            "Expected file name containing 'Play Activity'."
        )
    return candidates[0]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _first_present(headers: set[str], options: list[str]) -> str | None:
    for option in options:
        if option in headers:
            return option
    return None


def _read_headers(csv_path: Path) -> set[str]:
    with csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        header_row = next(reader, None)
        if not header_row:
            raise ValueError(f"CSV appears empty: {csv_path}")
        return {header.strip() for header in header_row if header and header.strip()}


def _build_timestamp_expr(column_sql: str) -> str:
    # Apple exports can vary between ISO and locale date formats.
    return f"""
COALESCE(
    TRY_CAST({column_sql} AS TIMESTAMP),
    try_strptime({column_sql}, '%Y-%m-%d %H:%M:%S %Z'),
    try_strptime({column_sql}, '%Y-%m-%d %H:%M:%S'),
    try_strptime({column_sql}, '%m/%d/%Y %I:%M:%S %p'),
    try_strptime({column_sql}, '%m/%d/%Y %H:%M:%S')
)
""".strip()


def _build_normalized_query(csv_path: Path) -> str:
    headers = _read_headers(csv_path)

    track_col = _first_present(headers, TRACK_COLUMNS)
    artist_col = _first_present(headers, ARTIST_COLUMNS)
    album_col = _first_present(headers, ALBUM_COLUMNS)
    played_at_col = _first_present(headers, PLAYED_AT_COLUMNS)
    play_count_col = _first_present(headers, PLAY_COUNT_COLUMNS)

    if not played_at_col:
        raise ValueError(
            "Could not find a played timestamp column. "
            f"Expected one of: {', '.join(PLAYED_AT_COLUMNS)}"
        )

    track_expr = _quote_ident(track_col) if track_col else "NULL"
    artist_expr = _quote_ident(artist_col) if artist_col else "NULL"
    album_expr = _quote_ident(album_col) if album_col else "NULL"
    played_expr = _build_timestamp_expr(_quote_ident(played_at_col))
    play_count_expr = f"TRY_CAST({_quote_ident(play_count_col)} AS BIGINT)" if play_count_col else "1"

    escaped_path = str(csv_path).replace("'", "''")
    ingested_at = int(time.time())

    return f"""
WITH source_data AS (
    SELECT *
    FROM read_csv_auto('{escaped_path}', header=TRUE, all_varchar=TRUE, sample_size=-1, ignore_errors=TRUE)
), normalized AS (
    SELECT
        NULLIF(TRIM({track_expr}), '') AS track,
        NULLIF(TRIM({artist_expr}), '') AS artist,
        NULLIF(TRIM({album_expr}), '') AS album,
        {play_count_expr} AS play_count,
        {played_expr} AS played_at_utc,
        'apple_music_play_activity' AS source,
        '{escaped_path}' AS source_file,
        CAST({ingested_at} AS BIGINT) AS ingested_at
    FROM source_data
)
SELECT
    track,
    artist,
    album,
    COALESCE(play_count, 1) AS play_count,
    played_at_utc,
    source,
    source_file,
    ingested_at,
    strftime(played_at_utc, '%Y') AS year,
    strftime(played_at_utc, '%m') AS month
FROM normalized
WHERE played_at_utc IS NOT NULL
""".strip()


def _existing_parquet_glob(curated_root: Path) -> str:
    return str(curated_root / "year=*" / "month=*" / "*.parquet").replace("'", "''")


def process_csv(csv_path: Path, curated_root: Path) -> dict:
    curated_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        normalized_query = _build_normalized_query(csv_path)
        incoming_deduped_query = f"""
WITH incoming AS ({normalized_query}),
deduped AS (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY track, artist, album, played_at_utc
                ORDER BY ingested_at DESC
            ) AS rn
        FROM incoming
    )
    WHERE rn = 1
)
SELECT * FROM deduped
""".strip()

        existing_glob = _existing_parquet_glob(curated_root)
        has_existing = bool(list((curated_root).glob("year=*/month=*/*.parquet")))

        if has_existing:
            merged_query = f"""
WITH incoming AS ({incoming_deduped_query}),
existing AS (
    SELECT
        track,
        artist,
        album,
        play_count,
        played_at_utc,
        source,
        source_file,
        ingested_at,
        strftime(played_at_utc, '%Y') AS year,
        strftime(played_at_utc, '%m') AS month
    FROM read_parquet('{existing_glob}', hive_partitioning=TRUE)
), all_rows AS (
    SELECT * FROM existing
    UNION ALL
    SELECT * FROM incoming
), deduped AS (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY track, artist, album, played_at_utc
                ORDER BY ingested_at DESC
            ) AS rn
        FROM all_rows
    )
    WHERE rn = 1
)
SELECT * FROM deduped
""".strip()
        else:
            merged_query = incoming_deduped_query

        tmp_output = curated_root.parent / f".tmp_play_activity_{int(time.time())}"
        if tmp_output.exists():
            shutil.rmtree(tmp_output)
        tmp_output.mkdir(parents=True, exist_ok=True)

        con.sql(
            f"""
COPY ({merged_query})
TO '{str(tmp_output).replace("'", "''")}'
(FORMAT PARQUET, PARTITION_BY (year, month))
"""
        )

        total_rows = con.sql(
            f"SELECT COUNT(*) FROM read_parquet('{str(tmp_output / 'year=*' / 'month=*' / '*.parquet').replace("'", "''")}', hive_partitioning=TRUE)"
        ).fetchone()[0]

        if curated_root.exists():
            shutil.rmtree(curated_root)
        tmp_output.rename(curated_root)

        return {
            "input_csv": str(csv_path),
            "curated_root": str(curated_root),
            "total_rows": total_rows,
        }
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Apple Music Play Activity CSV into deduplicated partitioned parquet."
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path(
            str(DEFAULT_RAW_ROOT)
        ),
        help="Root folder containing Apple Music raw exports.",
    )
    parser.add_argument(
        "--csv-file",
        type=Path,
        default=None,
        help="Explicit Play Activity CSV file to process.",
    )
    parser.add_argument(
        "--curated-root",
        type=Path,
        default=Path(str(DEFAULT_CURATED_ROOT)),
        help="Output curated parquet root for play activity.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = Path(args.raw_root).expanduser()
    csv_file = Path(args.csv_file).expanduser() if args.csv_file else None
    curated_root = Path(args.curated_root).expanduser()

    selected_csv = discover_csv(raw_root=raw_root, explicit_file=csv_file)
    result = process_csv(csv_path=selected_csv, curated_root=curated_root)

    print(f"Processed CSV: {result['input_csv']}")
    print(f"Curated output: {result['curated_root']}")
    print(f"Total deduplicated rows: {result['total_rows']}")


if __name__ == "__main__":
    main()
