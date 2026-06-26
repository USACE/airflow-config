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
    max_active_runs=5,
)
def cwms_batch_events_scheduled_jobs():
    @task(task_id="trigger-due-jobs")
    def trigger_due_jobs():
        minute = get_current_context()["logical_date"].minute
        due_scripts = batch_events.scripts_due_at_minute(minute)
        jobs = []
        for script in due_scripts:
            job = batch_events.trigger_job(script["id"])
            jobs.append(
                {
                    "jobId": job["id"],
                    "scriptId": script["id"],
                    "office": script["office"],
                    "slug": script["slug"],
                    "resourceProfile": script["resourceProfile"],
                    "runtime": script["runtime"],
                }
            )
        print(json.dumps(jobs, indent=2))
        return jobs

    trigger_due_jobs()


DAG_ = cwms_batch_events_scheduled_jobs()
