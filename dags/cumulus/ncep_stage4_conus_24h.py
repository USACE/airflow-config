"""
Acquire and Process NCEP Stage 4 MOSAIC QPE 24h
"""

import json
from datetime import datetime, timedelta
import calendar

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=7)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=60),
}


@dag(
    default_args=default_args,
    schedule="5 12 * * *",
    tags=["cumulus", "precip", "QPE", "CONUS", "stage4", "NCEP"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_ncep_stage4_conus_24h():
    """This pipeline handles download, processing, and derivative product creation for \n
    NCEP Stage 4 MOSAIC QPE\n
    URL Dir - https://nomads.ncep.noaa.gov/pub/data/nccf/com/pcpanl/prod/pcpanl.20220808/st4_conus.YYYYMMHHMM.24h.grb2\n
    Files matching st4_conus.YYYYMMHHMM.24h.grb2 - 24 hour\n
    This DAG will try to get today's file using timedelta(days=1) instead of file from prev period (yesterday).
    """

    URL_ROOT = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/pcpanl/prod"
    PRODUCT_SLUG = "ncep-stage4-mosaic-24h"

    @task()
    def download_raw_stage4_qpe():
        logical_date = get_current_context()["logical_date"] + timedelta(days=1)
        dirpath = f'pcpanl.{logical_date.strftime("%Y%m%d")}'
        filename = f'st4_conus.{logical_date.strftime("%Y%m%d")}12.24h.grb2'
        filepath = f"{dirpath}/{filename}"
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        print(f"Downloading file: {filepath}")
        trigger_download(
            url=f"{URL_ROOT}/{filepath}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps(
            {"datetime": logical_date.isoformat(), "s3_key": s3_key},
        )

    @task()
    def notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

    notify_cumulus(download_raw_stage4_qpe())


stage4_dag = cumulus_ncep_stage4_conus_24h()
