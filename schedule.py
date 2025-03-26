from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, declarative_base
from prefect import flow

Base = declarative_base(cls=Base)


def run_batch_load():
    import subprocess

    subprocess.run(["python", "uv.py"])


with DAG(
    "batch_load", schedule_interval="@daily", start_date=datetime(2025, 3, 26)
) as dag:
    task = PythonOperator(task_id="load_data", python_callable=run_batch_load)

"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl_pipeline import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sensor_data_etl',
    default_args=default_args,
    description='ETL pipeline to load sensor data into MongoDB',
    schedule_interval=timedelta(days=1)
)

etl_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag
)

etl_task
"""
