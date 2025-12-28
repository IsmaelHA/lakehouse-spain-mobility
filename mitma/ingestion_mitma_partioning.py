import sys
import os
import shutil
import uuid
from ducklake_utils import connect_ducklake, close_ducklake

def create_bronze_view(con, base_path):
    """
    Creates a View so you can query the Parquet files as if they were a table.
    We use HIVE_PARTITIONING=1 so DuckDB understands 'date=20230101' folders.
    """
    con.execute(f"""
        CREATE OR REPLACE VIEW bronze_raw_mobility_trips AS 
        SELECT * FROM parquet_scan('{base_path}/**/*.parquet', HIVE_PARTITIONING=1);
    """)

def ingestion_bronze_mitma_partitioned(valid_urls: list, base_path="data/bronze_mobility"):
    con = None
    try:    
        con = connect_ducklake()
        
        # 1. Ensure the base directory exists
        os.makedirs(base_path, exist_ok=True)

        batch_size = 15
        
        for i, batch_start in enumerate(range(0, len(valid_urls), batch_size)):
            batch_urls = valid_urls[batch_start:batch_start + batch_size]
            
            # Create a unique temp folder for THIS batch
            # e.g. data/bronze_mobility/_tmp_batch_0_a1b2c3d4
            batch_id = str(uuid.uuid4())[:8]
            temp_path = os.path.join(os.path.dirname(base_path), f"_tmp_batch_{i}_{batch_id}")
            
            print(f"Batch {i+1}: Processing {len(batch_urls)} files...")

            # 2. READ & WRITE TO TEMP (The Safe Zone)
            # We read the CSVs and immediately write them to the temp folder as Parquet.
            # We enforce 'all_varchar=True' for safety (Bronze rule).
            # We partition by 'date' (renamed from 'fecha').
            con.execute(f"""
                COPY (
                    SELECT 
                        fecha AS date,
                        periodo AS time_period,
                        origen AS origin_zone,
                        destino AS destination_zone,
                        distancia AS distance_range,
                        actividad_origen AS origin_activity,
                        actividad_destino AS destination_activity,
                        estudio_origen_posible AS is_origin_study_possible,
                        estudio_destino_posible AS is_destination_study_possible,
                        residencia AS residence_province_code,
                        renta AS income_range,
                        edad AS age_group,
                        sexo AS gender,
                        viajes AS trips,
                        viajes_km AS trips_km_product,
                        CURRENT_TIMESTAMP AS ingestion_date
                    FROM read_csv_auto({batch_urls}, compression='gzip', ignore_errors=true, all_varchar=true)
                ) 
                TO '{temp_path}' 
                (FORMAT PARQUET, PARTITION_BY (date), COMPRESSION 'ZSTD', OVERWRITE_OR_IGNORE 1)
            """)
            
            # 3. IDENTIFY AFFECTED PARTITIONS
            # Check what dates we actually wrote to the temp folder
            # Structure: temp_path/date=20230101/...
            if not os.path.exists(temp_path):
                 print(f"Batch {i+1}: No data found or written. Skipping.")
                 continue

            written_partitions = [d for d in os.listdir(temp_path) if d.startswith("date=")]
            
            print(f"Batch {i+1}: Swapping {len(written_partitions)} partitions...")

            # 4. ATOMIC SWAP (Overwrite logic)
            # For every date we just processed, we delete the OLD version in the main folder
            # and move the NEW version from the temp folder.
            for partition_name in written_partitions:
                src_partition = os.path.join(temp_path, partition_name)
                dst_partition = os.path.join(base_path, partition_name)
                
                # A. Delete old data for this specific date (if exists)
                if os.path.exists(dst_partition):
                    shutil.rmtree(dst_partition)
                
                # B. Move new data in
                shutil.move(src_partition, dst_partition)

            # 5. CLEANUP TEMP
            # The temp folder should be empty of partitions now, but remove the wrapper
            shutil.rmtree(temp_path)
            
        # 6. UPDATE VIEW (Final Step)
        # Now that all files are in place, we make sure the View sees them.
        create_bronze_view(con, base_path)
        
        print("MITMA Bronze ingestion completed successfully.")
        
    except Exception as e:
        print(f"Pipeline Failed: {e}")
        # Cleanup: If a temp folder was left behind due to crash, you might want to log it
        # or attempt to delete it here, though it's safer to leave it for inspection.
        raise e
    finally:
        if con:
            close_ducklake(con)