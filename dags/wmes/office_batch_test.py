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

submit_job = lrl_hourly_batch_task = batch.batch_operator(
    dag=dag,
    task_id="lrl-hourly-job",
    command=[],
    local_image="mock_job",  # Local Only - The local Docker image you want to run
    job_queue="wmes-lrd-jq",
    job_definition="wmes-lrl-jobs-jobdef",
    # container_overrides={},
)

wait_for_job = BatchSensor(
    task_id="wait_for_lrl_job",
    job_id=submit_job.output,  # Pull the job ID from XCom
    aws_conn_id="aws_default",  # Use your AWS connection id
    deferrable=True,
    # region_name="us-west-2",  # Set the appropriate AWS region
    poke_interval=30,  # Check every 30 seconds
    timeout=3600,  # Timeout after 1 hour
)

submit_job >> wait_for_job
