from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_pipeline import run_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="superstore_etl_dag",
    default_args=default_args,
    description="ETL пайплайн для Superstore: CSV → SQLite",
    schedule_interval="0 2 * * *",  # каждый день в 02:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["superstore", "etl", "sqlite"],
) as dag:

    etl_task = PythonOperator(
        task_id="run_superstore_etl",
        python_callable=run_etl,
    )