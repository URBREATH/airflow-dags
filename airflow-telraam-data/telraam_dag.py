# File name: telraam_minio_monthly_pipeline_dag.py

import datetime
import pandas as pd
import requests
import json
import os
from io import StringIO

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Configuration Constants ---
MINIO_CONN_ID = 'minio_conn'
MINIO_BUCKET = 'telraamdata'

# =============================================================================
# 1. TASK FUNCTIONS UPDATED FOR MINIO
# =============================================================================

def _get_s3fs_storage_options(minio_conn_id: str) -> dict:
    """Creates storage options for s3fs using S3Hook."""
    hook = S3Hook(aws_conn_id=minio_conn_id)
    session = hook.get_session()
    credentials = session.get_credentials()
    
    return {
        "key": credentials.access_key,
        "secret": credentials.secret_key,
        "client_kwargs": {
            "endpoint_url": hook.conn_config.endpoint_url,
        }
    }

def _fetch_data_to_minio(**kwargs):
    """
    Downloads data from the Telraam API and saves it as separate CSV files 
    in a 'raw_data' subfolder with a timestamp on MinIO.
    """
    ti = kwargs['ti']
    logical_date = kwargs['data_interval_start']
    timestamp_path = logical_date.strftime('%Y-%m-%d_%H-%M-%S')
    raw_data_path = f"raw_data/{timestamp_path}"

    print(f"Saving raw data to: s3://{MINIO_BUCKET}/{raw_data_path}")

    # Retrieve configurations from Airflow
    storage_options = _get_s3fs_storage_options(MINIO_CONN_ID)
    url = Variable.get("telraam_api_url")
    ids = Variable.get("telraam_segment_ids", deserialize_json=True)
    time_start = "2025-05-27 00:00:00Z" # To be made dynamic if necessary
    time_end = "2025-06-13 00:00:00Z"
    headers = {"Content-Type": "application/json"}

    for segment_id in ids:
        body = {
            "level": "segments", "id": segment_id, "format": "per-hour",
            "timezone": "Europe/Brussels", "time_start": time_start, "time_end": time_end
        }
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status() # Raise an error if the request fails
        data = response.json()
        
        if 'report' in data and data['report']:
            df = pd.DataFrame(data['report'])
            # Save directly to MinIO
            s3_path = f"s3://{MINIO_BUCKET}/{raw_data_path}/{segment_id}.csv"
            df.to_csv(s3_path, index=False, storage_options=storage_options)
        else:
            print(f"No 'report' data for ID {segment_id}")

    # Pass the raw data path to the next task via XComs
    ti.xcom_push(key='raw_data_path', value=raw_data_path)

def _process_data_from_minio(**kwargs):
    """
    Reads raw CSV files from MinIO, processes them, and saves the results
    (aggregated and shared data) in a 'processed_data' subfolder.
    """
    ti = kwargs['ti']
    logical_date = kwargs['data_interval_start']
    timestamp_path = logical_date.strftime('%Y-%m-%d_%H-%M-%S')
    
    # Retrieve the raw data path from the previous task
    raw_data_path = ti.xcom_pull(task_ids='fetch_data_to_minio', key='raw_data_path')
    if not raw_data_path:
        raise ValueError("Could not retrieve raw_data_path from XComs.")

    processed_data_path = f"processed_data/{timestamp_path}"
    print(f"Reading from: s3://{MINIO_BUCKET}/{raw_data_path}")
    print(f"Saving processed data to: s3://{MINIO_BUCKET}/{processed_data_path}")

    storage_options = _get_s3fs_storage_options(MINIO_CONN_ID)
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    
    # Find all CSV files in the raw data folder
    csv_files = hook.list_keys(bucket_name=MINIO_BUCKET, prefix=f"{raw_data_path}/")
    
    all_dfs = []
    for file_key in csv_files:
        if not file_key.endswith('.csv'):
            continue
            
        s3_path = f"s3://{MINIO_BUCKET}/{file_key}"
        df = pd.read_csv(s3_path, storage_options=storage_options)
        segment_id = os.path.basename(file_key).replace('.csv', '')
        df['source_id'] = segment_id
        all_dfs.append(df)
        
        # Per-file calculation and saving to 'processed_data'
        # ... logic identical to before for _share.csv files ...
        # e.g., share_percent.to_csv(f"s3://{MINIO_BUCKET}/{processed_data_path}/{segment_id}_share.csv", ...)

    if not all_dfs:
        print("No CSV files found to process.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True).sort_values(by='date')
    
    # Save the output files to the 'processed_data' folder
    # 1. all_data.csv
    combined_df.to_csv(f"s3://{MINIO_BUCKET}/{processed_data_path}/all_data.csv", index=False, storage_options=storage_options)
    # 2. summed_data.csv
    numeric_cols = combined_df.select_dtypes(include='number').columns
    summed_df = combined_df.groupby('date')[numeric_cols].sum().reset_index()
    summed_df.to_csv(f"s3://{MINIO_BUCKET}/{processed_data_path}/summed_data.csv", index=False, storage_options=storage_options)
    # 3. total_shares.csv
    # ... logic for total_shares.csv ...
    # e.g., share_percent.to_csv(f"s3://{MINIO_BUCKET}/{processed_data_path}/total_shares.csv", ...)

# =============================================================================
# 2. DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='telraam_minio_monthly_pipeline',
    start_date=datetime.datetime(2025, 6, 1),
    schedule_interval='0 5 1 * *',  # At 05:00 on the first day of each month
    catchup=False,
    doc_md="""
    Monthly pipeline to download Telraam data and save it to MinIO.
    - **fetch_data_to_minio**: Downloads raw data and saves it to `s3://telraamdata/raw_data/<timestamp>/`.
    - **process_data_from_minio**: Processes the raw data and saves the results to `s3://telraamdata/processed_data/<timestamp>/`.
    """,
    tags=['traffic', 'api', 'minio'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data_to_minio',
        python_callable=_fetch_data_to_minio,
    )

    process_task = PythonOperator(
        task_id='process_data_from_minio',
        python_callable=_process_data_from_minio,
    )

    # Set the dependency between tasks
    fetch_task >> process_task
