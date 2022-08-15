"""
SNODAS Assimilation Layers
"""

import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta

from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2021, 12, 13, 0, 0, 0),
    "start_date": (datetime.utcnow() - timedelta(hours=72)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 24,
    "retry_delay": timedelta(hours=3),
}


@dag(
    default_args=default_args,
    schedule_interval="30 20 * * *",
    tags=["cumulus", "snow"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_snodas_assimilation():
    """This pipeline handles download, processing, and derivative product creation for NOHRSC SNODAS Assimilation\n
    Product timestamp is currently unknown, but is typically around between 20:10 - 22:40 UTC.  This DAG will retry for up to 72 hours (3 days).
    """

    PRODUCT_SLUG = "nohrsc-snodas-assimilated"

    @task()
    def snodas_download_assimilation():

        # In order to get the current day's file, set execution forward 1 day
        execution_date = get_current_context()["logical_date"] + timedelta(hours=24)

        URL_ROOT = f"https://www.nohrsc.noaa.gov/pub/data/assim"
        filename = f'assim_layers_{execution_date.strftime("%Y%m%d")}12.tar'
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        output = trigger_download(
            url=f"{URL_ROOT}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps({"datetime": execution_date.isoformat(), "s3_key": s3_key})

    @task()
    def notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

    notify_cumulus(snodas_download_assimilation())


snodas_dag = cumulus_snodas_assimilation()
