from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task

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
    schedule="15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch-events", "jobs", "swt"],
    max_active_runs=1,
)
def cwms_batch_events_swt_hourly():
    @task(task_id="trigger-swt-hourly")
    def trigger_swt_hourly():
        script_id = batch_events.get_config("BATCH_EVENTS_SWT_HOURLY_SCRIPT_ID")
        job = batch_events.trigger_job(script_id)
        print(json.dumps(job, indent=2))
        return job["id"]

    trigger_swt_hourly()


DAG_ = cwms_batch_events_swt_hourly()
