
import duckdb

# Config
from config import CATALOG_DB, DATALAKE_ROOT
from config import CURATED_ROOT as _CURATED_BASE

CSV_PATH = DATALAKE_ROOT / "scrobbles-clakesnapster-1699153095.csv"
CURATED_ROOT = _CURATED_BASE / "lastfm/scrobbles"

def ingest():
    print(f"Ingesting {CSV_PATH}...")

    con = duckdb.connect(str(CATALOG_DB))

    # 1. Read CSV and Transform to Target Schema
    # We use 'try_strptime' or just epoch to timestamp conversion.
    # The target schema based on main.py is:
    # uts (int), played_at_utc (timestamp), artist (str), track (str), album (str), mbid_track (str), source (str)

    query = f"""
    SELECT
        CAST(uts AS BIGINT) as uts,
        make_timestamp(
            CAST(strftime(to_timestamp(CAST(uts AS BIGINT)), '%Y') AS BIGINT),
            CAST(strftime(to_timestamp(CAST(uts AS BIGINT)), '%m') AS BIGINT),
            CAST(strftime(to_timestamp(CAST(uts AS BIGINT)), '%d') AS BIGINT),
            CAST(strftime(to_timestamp(CAST(uts AS BIGINT)), '%H') AS BIGINT),
            CAST(strftime(to_timestamp(CAST(uts AS BIGINT)), '%M') AS BIGINT),
            CAST(strftime(to_timestamp(CAST(uts AS BIGINT)), '%S') AS DOUBLE)
        ) as played_at_utc,
        artist,
        track,
        album,
        track_mbid as mbid_track,
        'lastfm_csv_export' as source,
        strftime(to_timestamp(CAST(uts AS BIGINT)), '%Y') as year,
        strftime(to_timestamp(CAST(uts AS BIGINT)), '%m') as month
    FROM read_csv('{CSV_PATH}', auto_detect=TRUE)
    """

    # 2. Write to Parquet (Partitioned)
    # DuckDB's COPY command supports partitioning
    print(f"Writing partitioned parquet to {CURATED_ROOT}...")
    con.sql(f"""
        COPY ({query})
        TO '{CURATED_ROOT}'
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE TRUE)
    """)

    # 3. Create a View in the Catalog
    # This view unifies the partitions so you can query 'scrobbles' directly
    print("Updating Catalog View...")
    con.sql(f"""
        CREATE OR REPLACE VIEW scrobbles AS
        SELECT * EXCLUDE (year, month)
        FROM read_parquet('{CURATED_ROOT}/year=*/month=*/*.parquet', hive_partitioning=true)
    """)

    # Verify count
    count = con.sql("SELECT COUNT(*) FROM scrobbles").fetchone()[0]
    print(f"Total scrobbles in catalog: {count}")

    con.close()
    print("Done.")

if __name__ == "__main__":
    ingest()
