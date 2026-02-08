import duckdb
import os
from pathlib import Path
import time

# Config
ICLOUD_ROOT = Path("/Users/mohit/Library/Mobile Documents/com~apple~CloudDocs/Data Exports/datalake")
JSONL_PATH = Path("apple_music_history.jsonl").absolute()
CURATED_ROOT = ICLOUD_ROOT / "curated/apple_music/library"
CATALOG_DB = ICLOUD_ROOT / "catalog/datalake.duckdb"

def ingest():
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
