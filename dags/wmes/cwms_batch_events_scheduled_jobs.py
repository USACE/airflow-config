from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import helpers.batch_events as batch_events

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    schedule="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch-events", "jobs", "scheduled"],
    max_active_runs=2,
    max_active_tasks=30,
)
def cwms_batch_events_scheduled_jobs():
    @task(task_id="get-due-scripts")
    def get_due_scripts():
        logical_date = get_current_context()["logical_date"]
        scripts = batch_events.scripts_due_at(logical_date)
        print(json.dumps(scripts, indent=2))
        return scripts

    @task(task_id="trigger-script")
    def trigger_script(script: dict):
        job = batch_events.trigger_job(script["id"], office=script["office"])
        result = {
            "jobId": job["id"],
            "scriptId": script["id"],
            "office": script["office"],
            "slug": script["slug"],
            "resourceProfile": script["resourceProfile"],
            "runtime": script["runtime"],
        }
        print(json.dumps(result, indent=2))
        return result

    trigger_script.expand(script=get_due_scripts())


DAG_ = cwms_batch_events_scheduled_jobs()
