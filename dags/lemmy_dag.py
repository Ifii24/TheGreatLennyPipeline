import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make project root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.lemmy_pipeline import lemmy_to_postgres_pipeline

default_args = {
    "owner": "Ifigeneia Tziola",
    "start_date": datetime(2026, 2, 21),
}

dag = DAG(
    dag_id="etl_lemmy_to_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["lemmy", "etl", "postgres"],
)

etl = PythonOperator(
    task_id="lemmy_extract_transform_load",
    python_callable=lemmy_to_postgres_pipeline,
    op_kwargs={
        "community": "asklemmy@lemmy.world",
        "limit": 50,
        "sort": "New",
    },
    dag=dag,
)