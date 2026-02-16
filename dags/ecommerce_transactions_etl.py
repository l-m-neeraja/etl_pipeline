from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add scripts folder to path
sys.path.append("/opt/airflow/scripts")

from generate_data import main as generate_data
from ingest import main as ingest_data
from load_fact import main as load_fact

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_transactions_etl",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_source_data",
        python_callable=generate_data,
    )

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_fact,
    )

    generate_task >> ingest_task >> load_task
