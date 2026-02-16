import duckdb
import os
from pathlib import Path
import time
from apple_music_export_guard import (
    AppleMusicExportGuardError,
    enforce_fresh_export_or_raise,
)

# Config
from config import CURATED_ROOT as _CURATED_BASE, CATALOG_DB, EXPORT_METADATA_PATH, MAX_STALENESS_DAYS

JSONL_PATH = Path("apple_music_history.jsonl").absolute()
CURATED_ROOT = _CURATED_BASE / "apple_music/library"

def ingest():
    try:
        metadata, age_days = enforce_fresh_export_or_raise(
            EXPORT_METADATA_PATH, max_staleness_days=MAX_STALENESS_DAYS
        )
    except AppleMusicExportGuardError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(2)

    print(
        "Apple Music export freshness check passed: "
        f"latest_play_date={metadata.latest_play_date.isoformat()} "
        f"last_export_date={metadata.last_export_date.isoformat()} "
        f"age_days={age_days}"
    )

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

if __name__ == "__main__":
    ingest()
