import sys
import os
from datetime import timedelta
import pendulum

proj_root = os.path.dirname(os.path.dirname(__file__))  # ../ from dags/
sys.path.append(os.path.join(proj_root, 'scripts'))
sys.path.append(proj_root)  # add project root so 'config' can be found

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

from tasks.ensure_schema import ensure_db_table
from tasks.download_csv import download_full_csv_func
from tasks.extract_chunk import extract_next_month_chunk_func
from tasks.upload_and_mark import upload_and_mark_processed_func

local_tz = pendulum.timezone("Asia/Qatar")
default_args = {
    'owner': 'Mohammed & Asmaa',
    'start_date': pendulum.datetime(2025, 1, 1, tz=local_tz),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

with DAG(
    'stock_trend_prediction',
    default_args=default_args,
    description='Stock Trend prediction DAG',
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1,
) as dag:

    verify_monthly_ingestions_schema = PythonOperator(
        task_id='verify_monthly_ingestions_schema',
        python_callable=ensure_db_table
    )

    download_historical_stock_csv = PythonOperator(
        task_id="download_historical_stock_csv",
        python_callable=download_full_csv_func
    )

    extract_next_month_data_chunk = PythonOperator(
        task_id='extract_next_month_data_chunk',
        python_callable=extract_next_month_chunk_func
    )

    upload_chunk_and_update_ingestion_log = PythonOperator(
        task_id='upload_chunk_and_update_ingestion_log',
        python_callable=upload_and_mark_processed_func
    )

    spark_task = BashOperator(
    task_id='transform_stock_data_and_load_warehouse',
    bash_command='docker exec spark-master spark-submit /opt/spark/work-dir/spark_transform.py',
    )

    verify_monthly_ingestions_schema >> download_historical_stock_csv >> extract_next_month_data_chunk >> upload_chunk_and_update_ingestion_log >> spark_task
