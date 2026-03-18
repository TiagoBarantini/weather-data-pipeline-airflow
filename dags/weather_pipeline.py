from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
sys.path.insert(0, "/opt/airflow/scripts")

from extract_weather import extract_weather
from transform_weather import transform_weather
from load_weather import load_raw, load_trusted

default_args = {
    "owner": "tiago",
    "retries": 2
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_weather
    )

    load_raw_task = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_weather
    )

    load_trusted_task = PythonOperator(
        task_id="load_trusted",
        python_callable=load_trusted
    )

    extract >> load_raw_task >> transform >> load_trusted_task
