from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "schedule": "@hourly",
    "start_date": datetime(2025, 3, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "test_aws_batch",
    default_args=default_args,
    description="Submit an AWS Batch job to LocalStack",
    schedule_interval=None,
    catchup=False,
)

# AWS Batch Job Submission
submit_batch_job = BatchOperator(
    task_id="submit_batch_job",
    job_name="test-job",
    job_queue="test-queue",
    job_definition="test-job-def",
    # aws_conn_id="aws_default",  # Uses environment variable AIRFLOW_CONN_AWS_DEFAULT
    dag=dag,
)

# DAG Structure
submit_batch_job
