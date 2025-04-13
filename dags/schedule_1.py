from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from extract import load, update, check

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="myflow",
    default_args=default_args,
    schedule="*/2 * * * *",
    catchup=False,
) as dag:

    def branch_function():
        if not check():
            return "initial_load_task"
        else:
            return "update_db_task"

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
    )

    initial_load_task = PythonOperator(
        task_id="initial_load_task",
        python_callable=load,
    )

    update_db_task = PythonOperator(
        task_id="update_db_task",
        python_callable=update,
    )

    branch_task >> [initial_load_task, update_db_task]
