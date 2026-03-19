from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task

# from airflow.providers.amazon.aws.operators.batch import BatchOperator
import helpers.batch as batch
from helpers.batch import get_office_groups
from airflow.operators.python import get_current_context
from airflow.models.dag import DagContext
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 3),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
OFFICES = Variable.get("BATCH_HOURLY_OFFICES").split(",")


@dag(
    default_args=default_args,
    schedule_interval="15 * * * *",
    start_date=datetime(2025, 5, 3),
    catchup=False,
    tags=["batch", "jobs", "district"],
    max_active_runs=1,
    max_active_tasks=30,
)
def cwms_hourly_jobs():

    groups = get_office_groups(OFFICES)
    for group_name, configs in groups.items():
        with TaskGroup(group_id=group_name) as tg:
            for jc in configs:
                @task(task_id=f"{jc['office']}-jobs")
                def launch_batch(job_config):
                    logical_date = get_current_context()["logical_date"]
                    dag = DagContext.get_current_dag()
                    job_name = f"cwms-{job_config['office']}-hourly-job-{logical_date.strftime('%Y%m%d-%H%M')}"
                    return batch.batch_operator(
                        dag=dag,
                        task_id=job_name,
                        deferrable=True,
                        container_overrides={
                            "environment": [
                                {"name": "OFFICE",
                                    "value": job_config["office"]},
                            ],
                            "command": ["/jobs/bin/hourly.sh"],
                        },
                        job_name=job_name,
                        job_queue=f"cwms-{job_config['office_group']}-jq",
                        job_definition=f"cwms-{job_config['office']}-jobs-jobdef",
                        local_command=[],  # local docker mock only
                        tags={"Office": job_config["office"]},
                    ).execute({})
                launch_batch(jc)


cwms_jobs_dag = cwms_hourly_jobs()
