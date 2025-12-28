from ducklake_utils import connect_ducklake, close_ducklake

def run_stats_update():
    
    silver_view="silver_mobility_trips"
    con=None
    try:
        con = connect_ducklake()
        # 1. CHECK: Do we actually need to run?
        # We check if there is any date in Silver that is NOT in our "processed log"
        # This acts as our "Dirty Flag"
        new_data_exists = con.execute(f"""
            SELECT 1 
            FROM {silver_view} 
            WHERE date NOT IN (SELECT processed_date FROM silver_stats_log)
            LIMIT 1
        """).fetchone()

        if not new_data_exists:
            print("✅ Stats are already up to date. Skipping full refresh.")
            return

        print("🔄 New data detected. Starting FULL stats recalculation...")
        con.execute("BEGIN TRANSACTION;")

        # 2. RECALCULATE EVERYTHING (Full Scan)
        # We use CREATE OR REPLACE to rebuild the table entirely.
        # Note: We no longer need sum_x or sum_x2 columns, just the final stats.
        con.execute(f"""
            CREATE OR REPLACE TABLE silver_zone_stats AS
            SELECT
                day_type,
                hour_period,
                origin_zone,
                destination_zone,
                COUNT(*) as n_obs,
                -- Simple, accurate, standard SQL math:
                AVG(trips) as mean_trips,
                STDDEV(trips) as std_trips
            FROM {silver_view}
            GROUP BY 
                day_type, 
                hour_period, 
                origin_zone, 
                destination_zone
            HAVING COUNT(*) > 5 -- Optional: Only keep stats if we have enough history
        """)

        # 3. UPDATE THE LOG
        # Since we just processed the WHOLE table, the log should now match the Silver table exactly.
        con.execute("DELETE FROM silver_stats_log;") 
        
        con.execute(f"""
            INSERT INTO silver_stats_log (processed_date)
            SELECT DISTINCT date FROM {silver_view};
        """)

        con.execute("COMMIT;")
        print("✅ Stats table fully rebuilt and synchronized.")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(f"❌ Error during Stats Refresh: {e}")
        raise
    finally:
        close_ducklake(con)