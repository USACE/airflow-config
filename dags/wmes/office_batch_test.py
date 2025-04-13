from airflow import DAG
from datetime import datetime, timedelta, timezone

import helpers.batch as batch


# Define default_args, DAG start date, and schedule
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 13),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "my_docker_task_dag",
    default_args=default_args,
    description="A simple DAG to test the batch operator",
    schedule_interval="@hourly",  # Run hourly
    catchup=False,  # Set to False to avoid running past dates
    tags=["testing"],
    max_active_runs=1,
    max_active_tasks=1,
)

batch_task1 = batch.batch_operator(
    dag=dag,
    task_id="run_local_docker_container1",
    local_image="district-tasks",  # The Docker image you want to run
    command="hello_world",
)

batch_task2 = batch.batch_operator(
    dag=dag,
    task_id="run_local_docker_container2",
    local_image="district-tasks",  # The Docker image you want to run
    command="second_script",
)
