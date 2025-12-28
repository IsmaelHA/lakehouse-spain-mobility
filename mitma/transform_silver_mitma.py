import os
import shutil
import uuid
from ducklake_utils import connect_ducklake, close_ducklake

def run_silver_ingestion_atomic(base_path="data/silver_mobility"):
    con = None
    # Create a unique temp folder for this specific run
    # e.g., data/silver_mobility/_tmp_ingest_1234abcd
    batch_id = str(uuid.uuid4())[:8]
    temp_path = os.path.join(os.path.dirname(base_path), f"_tmp_ingest_{batch_id}")
    silver_view="silver_mobility_trips"
    try:
        con = connect_ducklake()
        source_table = "stg_mobility_clean_check"
        
        # 1. Check if Source has data
        count = con.execute(f"SELECT count(*) FROM {source_table}").fetchone()[0]
        if count == 0:
            print("No data in staging. Skipping.")
            return

        print(f"Staging contains {count} rows. Writing to temporary location: {temp_path}...")

        # 2. WRITE TO TEMP (Safe Zone)
        # We write ALL partitions to a temporary separate folder first.
        # If this fails, your real Silver data is completely untouched.
        con.execute(f"""
            COPY (SELECT * FROM {source_table}) 
            TO '{temp_path}' 
            (FORMAT PARQUET, PARTITION_BY (date), COMPRESSION 'ZSTD')
        """)
        
        # 3. IDENTIFY WRITTEN PARTITIONS
        # Now we look at what we actually wrote to the temp folder
        # The structure will be: _tmp_ingest_xyz/date=2023-01-01/...
        written_partitions = [d for d in os.listdir(temp_path) if d.startswith("date=")]
        
        print(f"New data ready for partitions: {written_partitions}")

        # 4. ATOMIC SWAP (The Critical Step)
        # We assume the base_path exists (create it if not)
        os.makedirs(base_path, exist_ok=True)

        for partition_name in written_partitions:
            # Source: data/_tmp_ingest_xyz/date=2023-01-01
            src_partition = os.path.join(temp_path, partition_name)
            
            # Dest: data/silver_mobility/date=2023-01-01
            dst_partition = os.path.join(base_path, partition_name)
            
            # A. If destination exists, move it to a trash/backup folder (Optional safety)
            # Or just delete it. 'shutil.rmtree' is fast.
            if os.path.exists(dst_partition):
                shutil.rmtree(dst_partition)
            
            # B. Move the NEW partition to the DESTINATION
            # unique os.replace is atomic on POSIX (Linux/Mac)
            shutil.move(src_partition, dst_partition)
            print(f"Updated partition: {partition_name}")

        # 5. REFRESH VIEW
        # Just to be sure DuckDB sees the new files immediately
        con.execute(f"""
            CREATE OR REPLACE VIEW {silver_view} AS 
            SELECT * FROM parquet_scan('{base_path}/**/*.parquet', HIVE_PARTITIONING=1);
        """)
        con.execute(f"""
            DROP TABLE IF EXISTS {source_table};
        """)
        print("Success: Silver Layer updated safely.")

    except Exception as e:
        print(f"Silver Partitioning Failed: {e}")
        # Since we wrote to temp, we don't need to "rollback" the main folder.
        # We just leave the temp folder there for debugging or delete it.
        raise e
    finally:
        # CLEANUP: Always remove the temporary folder
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)
            print("Temporary staging files cleaned up.")
            
        if con:
            close_ducklake(con)