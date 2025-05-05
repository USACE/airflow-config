from airflow import DAG
from datetime import datetime, timedelta, timezone

import helpers.batch as batch
from airflow.providers.amazon.aws.sensors.batch import BatchSensor


# Define default_args, DAG start date, and schedule
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 3),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "wmes_batch_jobs",
    default_args=default_args,
    description="A simple DAG to test the batch operator",
    schedule="@hourly",  # Run hourly
    catchup=False,  # Set to False to avoid running past dates
    tags=["testing", "batch"],
    max_active_runs=1,
    max_active_tasks=1,
)

# batch_task1 = batch.batch_operator(
#     dag=dag,
#     task_id="run_local_docker_container1",
#     command=["hello_world"],
#     local_image="district-tasks",  # The local Docker image you want to run
# )

# The BatchOperator will wait and return SUCCESS or FAIL

submit_job = lrl_hourly_batch_task = batch.batch_operator(
    dag=dag,
    task_id="lrl-hourly-job",
    command=[],
    local_image="mock_job",  # Local Only - The local Docker image you want to run
    job_queue="wmes-lrd-jq",
    job_definition="wmes-lrl-jobs-jobdef",
    deferrable=True,
    container_overrides={
        # "cpu": 1,  # vCPUs
        # "memory": 2048,  # memory (MB)
        # "command": [],
    },
    tags={"Office": "lrl"},
)

submit_job
