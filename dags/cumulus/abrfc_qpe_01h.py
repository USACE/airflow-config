"""
Acquire and Process ABRFC 01h
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
    "start_date": (datetime.utcnow() - timedelta(hours=48)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 12,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    schedule_interval="10 * * * *",
    tags=["cumulus", "precip", "QPE", "ABRFC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_abrfc_qpe_01h():
    """This pipeline handles download, processing, and derivative product creation for \n
    ABRFC QPE\n
    URL Dir - https://tgftp.nws.noaa.gov/data/rfc/abrfc/xmrg_qpe/
    Files matching abrfc_qpe_01hr_YYYYMMDDHHZ.nc - 1 hour\n
    Note: Delay observed when watching new product timestamp on file at source.
    Example: timestamp said 15:50, but was pushed to server at 16:07
    """

    URL_ROOT = f"https://tgftp.nws.noaa.gov/data/rfc/abrfc/xmrg_qpe"
    PRODUCT_SLUG = "abrfc-qpe-01h"

    @task()
    def download_raw_qpe():
        logical_date = get_current_context()["logical_date"]
        filename = f'abrfc_qpe_01hr_{logical_date.strftime("%Y%m%d%H")}Z.nc'
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        print(f"Downloading file: {filename}")
        trigger_download(
            url=f"{URL_ROOT}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
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

    notify_cumulus(download_raw_qpe())


stage4_dag = cumulus_abrfc_qpe_01h()
