from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.param import Param
# --- Import your custom functions ---
# Ensure these modules are in your PYTHONPATH or Airflow plugins folder
from fetch_url_mitma import fetch_mitma_url
from ingestion_mitma_partioning import ingestion_bronze_mitma_partitioned
from data_quality_clean_checks import run_data_quality_fixes
from transform_silver_mitma import run_silver_ingestion_atomic
from data_quality_updates import run_stats_update

# --- Default Arguments ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'end_date': datetime(2023, 1, 1),
    'retries': 1
}


@dag(
    dag_id='mitma_mobility_pipeline',
    default_args=default_args,
    description='Pipeline for MITMA Mobility: Fetch -> Bronze -> Quality -> Silver -> Stats',
    start_date=datetime(2023, 1, 1), # Adjust start date
    #schedule_interval='@daily',      # Runs once a day
    catchup=False,                   # Set True if you want to backfill past dates
    tags=['mitma', 'mobility'],
    params={
        "start_date": Param(default="None", type=["string", "null"], description="YYYY-MM-DD"),
        "end_date": Param(default=None, type=["string", "null"], description="YYYY-MM-DD"),
    }
)
def mitma_pipeline():

    # 1. TASK: Fetch URLs
    # We use the logical_date (execution date) as the target date
    @task
    def task_fetch_urls(**context):
        # 1. Try to get manual parameters from the UI Trigger
        # "params" dictionary is automatically available in context
        manual_start = context['params'].get('start_date')
        manual_end = context['params'].get('end_date')
        
        # 2. Determine which dates to use
        if manual_start and manual_end:
            print(f"Manual trigger detected: {manual_start} to {manual_end}")
            s_date = manual_start
            e_date = manual_end
        else:
            # Fallback: Use the Airflow execution date (logical_date)
            # Useful if you switch back to a daily schedule
            print(f"No manual dates provided. Using execution date: {context['ds']}")
            s_date = context['ds']
            e_date = context['ds']
        # 3. Call your function
        urls = fetch_mitma_url(s_date, e_date)
        
        if not urls:
            print(f"No URLs found between {s_date} and {e_date}.")
            return []
            
        return urls

    # 2. TASK: Ingest to Bronze
    # This task receives the list of URLs from the previous task via XComs automatically
    @task
    def task_ingest_bronze(valid_urls: list):
        if not valid_urls:
            print("Skipping ingestion: No URLs provided.")
            return
        
        print(f"Ingesting {len(valid_urls)} files into Bronze...")
        ingestion_bronze_mitma_partitioned(valid_urls)

    # 3. TASK: Data Quality Checks (Bronze Layer)
    @task
    def task_dq_checks(valid_urls):
        print("Running Data Quality Fixes/Checks...")
        run_data_quality_fixes(valid_urls)

    # 4. TASK: Silver Transformation
    @task
    def task_silver_transform():
        print("Running Silver Ingestion (Atomic Swap)...")
        run_silver_ingestion_atomic()

    # 5. TASK: Update Statistics
    @task
    def task_update_stats():
        print("Updating Data Quality Stats...")
        run_stats_update()

    # --- Define the Flow / Dependencies ---
    
    # Get the URLs
    url_list = task_fetch_urls()
    
    # Pass URLs to Ingestion
    ingestion = task_ingest_bronze(url_list)
    dq_task = task_dq_checks(url_list)
    # Define the rest of the sequence
    ingestion >> dq_task
    dq_task >> task_silver_transform() >> task_update_stats()

# Instantiate the DAG
dag_instance = mitma_pipeline()