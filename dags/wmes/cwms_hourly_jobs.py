from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.models import Variable

import helpers.batch_events as batch_events


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 3),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def configured_offices(name: str) -> list[str]:
    return [
        office.strip()
        for office in Variable.get(name, default_var="").split(",")
        if office.strip()
    ]


@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 5, 3),
    catchup=False,
    tags=["batch-events", "jobs", "district", "manual"],
    max_active_runs=1,
    max_active_tasks=30,
)
def cwms_hourly_jobs():
    @task(task_id="get-hourly-scripts")
    def get_hourly_scripts():
        scripts = batch_events.scheduled_scripts_for_offices(
            "hourly",
            configured_offices("BATCH_HOURLY_OFFICES"),
        )
        print(json.dumps(scripts, indent=2))
        return scripts

    @task(task_id="trigger-script")
    def trigger_script(script: dict):
        job = batch_events.trigger_job(script["id"])
        result = {
            "jobId": job["id"],
            "scriptId": script["id"],
            "office": script["office"],
            "slug": script["slug"],
            "scheduleType": script["scheduleType"],
            "resourceProfile": script["resourceProfile"],
            "runtime": script["runtime"],
        }
        print(json.dumps(result, indent=2))
        return result

    trigger_script.expand(script=get_hourly_scripts())


cwms_jobs_dag = cwms_hourly_jobs()
