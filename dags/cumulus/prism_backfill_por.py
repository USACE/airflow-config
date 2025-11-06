import json
from calendar import monthrange
from datetime import datetime, timedelta, timezone

import helpers.cumulus as cumulus
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

default_args_backfill = {
    "owner": "airflow",
    "depends_on_past": False,
    ### to backfill POR
    "start_date": datetime(1981, 1, 1),  # Start from 1981
    "end_date": (datetime.now(timezone.utc) - timedelta(days=180)).replace(
        minute=0, second=0
    ),  # Stop 6 months ago
    "catchup": True,  # Enable backfill
    ### to force backfill past 6 months
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

@dag(
    default_args=default_args_backfill,
    schedule="30 01 1 * *",  # monthly schedule
    tags=["cumulus", "backfill", 'prism'],
    max_active_runs=1,  # Limit concurrent runs
    max_active_tasks=6,  # Limit concurrent tasks
)
def cumulus_prism_backfill_por():
    """Backfill historical PRISM data month-by-month."""

    URL_ROOT = f"https://data.prism.oregonstate.edu/time_series/us/an/4km"

    @task()
    def download_historical_prism_month(short_name='ppt'):
        product_slug = f"prism-{short_name}-early"
        logical_date = get_current_context()["logical_date"]
        execution_date = logical_date.date()
        year = execution_date.year
        month = execution_date.month
        results = []

        # Get the number of days in the month
        num_days = monthrange(year, month)[1]

        for day in range(1, num_days + 1):
            dt = datetime(year, month, day)
            file_dir = f'{URL_ROOT}/{short_name}/daily/{dt.strftime("%Y")}'
            filename = f'prism_{short_name}_us_25m_{dt.strftime("%Y%m%d")}.zip'
            s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"
            print(f"Downloading {filename}")
            try:
                output = trigger_download(
                    url=f"{file_dir}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key,
                )
                results.append(
                    {
                        "datetime": logical_date.isoformat(),
                        "s3_key": s3_key,
                        "product_slug": product_slug,
                        "filename": filename,
                    }
                )
            except:
                print(f'Error downloading {filename}')
        return json.dumps(results)

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for item in payload:
            print("Notifying Cumulus: " + item["filename"])
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[item["product_slug"]],
                datetime=item["datetime"],
                s3_key=item["s3_key"],
            )

    notify_cumulus(download_historical_prism_month(short_name='ppt'))
    notify_cumulus(download_historical_prism_month(short_name='tmax'))
    notify_cumulus(download_historical_prism_month(short_name='tmin'))


backfill_dag = cumulus_prism_backfill_por()