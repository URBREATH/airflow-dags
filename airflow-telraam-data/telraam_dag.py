# File name: telraam_minio_monthly_pipeline_dag.py

import datetime
import pandas as pd
import requests
import json
import os
import s3fs

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

# --- Configuration Constants ---
MINIO_BUCKET = 'urbreath-public-repo'

# =============================================================================
# 1. TASK FUNCTIONS UPDATED TO USE VARIABLES
# =============================================================================

def _get_s3fs_storage_options_from_vars() -> dict:
    """Creates storage options for s3fs by reading from Airflow Variables."""
    endpoint_url = Variable.get("minio_endpoint_url")
    access_key = Variable.get("minio_access_key")
    secret_key = Variable.get("minio_secret_key")
    
    return {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {
            "endpoint_url": endpoint_url,
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
    raw_data_path = f"Leuven/Mobility/KPIs/{timestamp_path}/raw_data"

    print(f"Saving raw data to: s3://{MINIO_BUCKET}/{raw_data_path}")

    storage_options = _get_s3fs_storage_options_from_vars()
    
    url = Variable.get("telraam_api_url")
    ids = Variable.get("telraam_segment_ids", deserialize_json=True)
    headers = {"Content-Type": "application/json"}

    time_start_dt = kwargs['data_interval_start']
    time_end_dt = kwargs['data_interval_end']

    time_start = time_start_dt.strftime('%Y-%m-%d %H:%M:%SZ')
    time_end = time_end_dt.strftime('%Y-%m-%d %H:%M:%SZ')
    
    print(f"Fetching data from {time_start} to {time_end}")

    for segment_id in ids:
        body = {
            "level": "segments", "id": segment_id, "format": "per-hour",
            "timezone": "Europe/Brussels", "time_start": time_start, "time_end": time_end
        }
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        data = response.json()
        
        if 'report' in data and data['report']:
            df = pd.DataFrame(data['report'])
            s3_path = f"s3://{MINIO_BUCKET}/{raw_data_path}/{segment_id}.csv"
            df.to_csv(s3_path, index=False, storage_options=storage_options)
        else:
            print(f"No 'report' data for ID {segment_id}")

    ti.xcom_push(key='raw_data_path', value=raw_data_path)

def _process_data_from_minio(**kwargs):
    """
    Reads raw CSV files from MinIO, processes them, and saves the results
    in a 'processed_data' subfolder.
    """
    ti = kwargs['ti']
    logical_date = kwargs['data_interval_start']
    timestamp_path = logical_date.strftime('%Y-%m-%d_%H-%M-%S')
    
    raw_data_path = ti.xcom_pull(task_ids='fetch_data_to_minio', key='raw_data_path')
    if not raw_data_path:
        raise ValueError("Could not retrieve raw_data_path from XComs.")

    processed_data_path = f"Leuven/Mobility/KPIs/{timestamp_path}/processed_data"
    print(f"Reading from: s3://{MINIO_BUCKET}/{raw_data_path}")
    print(f"Saving processed data to: s3://{MINIO_BUCKET}/{processed_data_path}")

    storage_options = _get_s3fs_storage_options_from_vars()
    
    s3 = s3fs.S3FileSystem(**storage_options)
    s3_full_path = f"{MINIO_BUCKET}/{raw_data_path}/"
    csv_files_full_path = s3.glob(f"{s3_full_path}*.csv")

    all_dfs = []
    for file_path in csv_files_full_path:
        s3_uri = f"s3://{file_path}"
        df = pd.read_csv(s3_uri, storage_options=storage_options)
        segment_id = os.path.basename(file_path).replace('.csv', '')
        df['source_id'] = segment_id
        all_dfs.append(df)
        
        share_cols = ['pedestrian', 'car', 'bike', 'heavy']
        available_share_cols = [col for col in share_cols if col in df.columns]
        if available_share_cols:
            total_sum = df[available_share_cols].sum().sum()
            if total_sum > 0:
                total_shares = df[available_share_cols].sum()
                share_percent = (total_shares / total_sum * 100).reset_index()
                share_percent.columns = ['type', 'percent_share']
                share_percent['total_count'] = total_shares.values
                
                output_s3_path = f"s3://{MINIO_BUCKET}/{processed_data_path}/{segment_id}_share.csv"
                share_percent.to_csv(output_s3_path, index=False, storage_options=storage_options)

    if not all_dfs:
        print("No CSV files found to process.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True).sort_values(by='date')
    
    combined_df.to_csv(f"s3://{MINIO_BUCKET}/{processed_data_path}/all_data.csv", index=False, storage_options=storage_options)
    
    numeric_cols = combined_df.select_dtypes(include='number').columns
    summed_df = combined_df.groupby('date')[numeric_cols].sum().reset_index()
    summed_df.to_csv(f"s3://{MINIO_BUCKET}/{processed_data_path}/summed_data.csv", index=False, storage_options=storage_options)
    
    share_cols = ['pedestrian', 'car', 'bike', 'heavy']
    available_share_cols = [col for col in share_cols if col in combined_df.columns]
    if available_share_cols:
        total_sum = combined_df[available_share_cols].sum().sum()
        if total_sum > 0:
            total_shares = combined_df[available_share_cols].sum()
            share_percent = (total_shares / total_sum * 100).reset_index()
            share_percent.columns = ['type', 'percent_share']
            share_percent['total_count'] = total_shares.values
            
            output_s3_path = f"s3://{MINIO_BUCKET}/{processed_data_path}/total_shares.csv"
            share_percent.to_csv(output_s3_path, index=False, storage_options=storage_options)
    else:
        print("No share columns found in the combined data.")

# =============================================================================
# 2. DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='telraam_minio_monthly_pipeline_vars',
    start_date=datetime.datetime(2025, 6, 1),
    schedule_interval='0 5 1 * *',
    catchup=False,
    doc_md="""
    Monthly pipeline that uses **Airflow Variables** for MinIO credentials.
    - **fetch_data_to_minio**: Downloads raw data and saves it to `s3://telraamdata/raw_data/<timestamp>/`.
    - **process_data_from_minio**: Processes the raw data and saves the results (including shares) to `s3://telraamdata/processed_data/<timestamp>/`.
    """,
    tags=['traffic', 'api', 'minio', 'variables'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data_to_minio',
        python_callable=_fetch_data_to_minio,
    )

    process_task = PythonOperator(
        task_id='process_data_from_minio',
        python_callable=_process_data_from_minio,
    )

    fetch_task >> process_task