from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/python_scripts')
from insert_records import main

default_args = {
    'Description': 'DAG to orchestrate data',
    'start_date': datetime(2025, 7, 30),
    'catchup': False
}

with DAG(
    dag_id='weather_api_orchestrator',
    default_args=default_args,
    schedule=timedelta(minutes=1),
    description="A simple DAG with start, branch tasks, merge, and end",
    tags=["example"]
) as dag:

    start = EmptyOperator(task_id='start')

    task1 = PythonOperator(
        task_id = 'example_task',
        python_callable = main
    )

    end = EmptyOperator(task_id='end')

    # Set dependencies: start -> branches -> merge -> end
    start >> task1 >> end