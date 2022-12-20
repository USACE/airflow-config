"""
## NERFC QPE

Browser view: https://tgftp.nws.noaa.gov/data/rfc/nerfc/xmrg_qpe/

## QPE
Example file: xmrg1208202223z.grb.gz
    
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=24)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 16,  # try up to 4 hours
    "retry_delay": timedelta(minutes=15),
}


@dag(
    default_args=default_args,
    schedule="15 * * * *",
    tags=["cumulus", "precip", "QPE", "NERFC"],
    max_active_runs=2,
    max_active_tasks=4,
    doc_md=__doc__,
)
def cumulus_nerfc_qpe():

    URL_ROOT = f"https://tgftp.nws.noaa.gov/data/rfc/nerfc/xmrg_qpe"
    PRODUCT_SLUG = "nerfc-qpe-01h"

    @task()
    def download_nerfc_qpe():
        logical_date = get_current_context()["logical_date"]

        dirpath = ""
        filename = f'xmrg{logical_date.strftime("%m%d%Y%Hz")}.grb.gz'
        filepath = f"{dirpath}/{filename}"
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        print(f"Downloading {filepath}")

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

    notify_cumulus(download_nerfc_qpe())


DAG_ = cumulus_nerfc_qpe()
