from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
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
    dag_id = 'weather_api_dbt_orchestrator',
    default_args = default_args,
    schedule = timedelta(minutes = 1),
    description = "A simple DAG to pick up data from the api and load it to the DB",
    tags = ["example"]
) as dag:

    start = EmptyOperator(task_id='start')

    task1 = PythonOperator(
        task_id = 'api_req_db_insert',
        python_callable = main
    )

    task2 = DockerOperator( # this is to basically perform `docker-compose run ___` with the following params 
        task_id = 'data_transform',
        image = 'ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = 'run',
        working_dir = '/usr/app',
        mounts = [ 
            # new containers are spun up and auto closed everytime this task runs
            # this is done for simplicity, security, scalability, and isolation 
            # so that it is consistent on every run
            Mount(
                source = '/home/dhasharadhareddyb/projects/custom-de-pipeline/dbt/dbt_project',
                target = '/usr/app',
                type = 'bind'
            ),
            Mount(
                source = '/home/dhasharadhareddyb/projects/custom-de-pipeline/dbt/profiles.yml',
                target = '/root/.dbt/profiles.yml',
                type = 'bind'
            )
        ],
        network_mode = 'custom-de-pipeline_my-network', # the root folder name is prefixed by default
        docker_url = 'unix://var/run/docker.sock', # the url to docker socket
        auto_remove = 'success'
    )

    end = EmptyOperator(task_id='end')

    # Set dependencies: start -> branches -> merge -> end
    start >> task1 >> task2 >> end