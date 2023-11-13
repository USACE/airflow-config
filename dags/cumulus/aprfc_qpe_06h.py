"""
Acquire and Process APRFC QPE 06h
"""

import json
from datetime import datetime, timedelta
import calendar

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=72)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


# ALR QPF filename generator
def get_filenames(edate):
    times = ["00_06", "06_12", "12_18", "18_00"]
    d = edate.strftime("%Y%m%d")
    for time in times:
        yield f"precip_acr_grid_{time}_{d}.grb.gz"


@dag(
    default_args=default_args,
    schedule="40 14,5 * * *",
    tags=["cumulus", "precip", "QPE", "APRFC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_aprfc_qpe_06h():
    """This pipeline handles download, processing, and derivative product creation for \n
    APRFC QPE\n
    URL Dir - https://cbt.crohms.org/akgrids
    Files matching precip_acr_grid_00_06_YYYYMMDD.grb.gz - 6 hour\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = f"https://cbt.crohms.org/akgrids"
    PRODUCT_SLUG = "aprfc-qpe-06h"

    @task()
    def download_raw_qpe():
        logical_date = get_current_context()["logical_date"]

        return_list = list()
        for filename in get_filenames(logical_date):
            url = f"{URL_ROOT}/{filename}"
            s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{filename}"
            print(f"Downloading file: {filename}")
            try:
                trigger_download(url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
                return_list.append(
                    {
                        "execution": logical_date.isoformat(),
                        "s3_key": s3_key,
                        "filename": filename,
                    }
                )
            except:
                print(f"{filename} is not available to download")

        return json.dumps(return_list)

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for item in payload:
            print("Notifying Cumulus: " + item["filename"])
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=item["execution"],
                s3_key=item["s3_key"],
            )

    notify_cumulus(download_raw_qpe())


aprfc_qpe_dag = cumulus_aprfc_qpe_06h()
