from datetime import datetime, timedelta
from airflow.decorators import dag, task

# from airflow.providers.amazon.aws.operators.batch import BatchOperator
import helpers.batch as batch
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


@dag(
    default_args=default_args,
    schedule_interval="15 * * * *",
    start_date=datetime(2025, 5, 3),
    catchup=False,
    tags=["batch", "jobs", "district"],
    max_active_runs=1,
    max_active_tasks=4,
)
def wmes_hourly_jobs():
    job_configs = [
        {
            "office": "lrc",
            "office_group": "lrd",
            "enabled": False,
        },
        {
            "office": "lre",
            "office_group": "lrd",
            "enabled": False,
        },
        {
            "office": "lrh",
            "office_group": "lrd",
            "enabled": True,
        },
        {
            "office": "lrl",
            "office_group": "lrd",
            "enabled": False,
        },
        {
            "office": "lrn",
            "office_group": "lrd",
            "enabled": False,
        },
        {
            "office": "lrp",
            "office_group": "lrd",
            "enabled": False,
        },
        {
            "office": "swt",
            "office_group": "swd",
            "enabled": True,
        },
    ]

    # Organize configs by office_group
    groups = {}
    for config in job_configs:
        groups.setdefault(config["office_group"], []).append(config)

    for group_name, configs in groups.items():
        with TaskGroup(group_id=group_name) as tg:
            for jc in configs:

                @task
                def launch_batch(job_config):

                    if job_config["enabled"] is False:
                        raise AirflowSkipException(
                            f"Skipping task  - job is not enable in config"
                        )

                    logical_date = get_current_context()["logical_date"]
                    dag = DagContext.get_current_dag()
                    job_name = f"wmes-{job_config['office']}-hourly-job-{logical_date.strftime('%Y%m%d-%H%M')}"
                    return batch.batch_operator(
                        dag=dag,
                        task_id=job_name,
                        deferrable=True,
                        container_overrides={
                            "environment": [
                                {"name": "OFFICE", "value": job_config["office"]},
                            ],
                            "command": ["/jobs/bin/hourly.sh"],
                        },
                        job_name=job_name,
                        job_queue=f"wmes-{job_config['office_group']}-jq",
                        job_definition=f"wmes-{job_config['office']}-jobs-jobdef",
                        local_command=[],  # local docker mock only
                        tags={"Office": job_config["office"]},
                    ).execute({})

                # Use override to set task_id with office name
                launch_batch.override(task_id=f"{jc['office']}-jobs")(jc)


wmes_jobs_dag = wmes_hourly_jobs()
