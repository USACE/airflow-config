"""
Acquire and Process APRFC QTE 01h

Returns
-------
Airflow DAG
    Directed Acyclic Graph
"""

from datetime import datetime, timedelta, timezone
import json
from string import Template
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=48)).replace(
        minute=0, second=0
    ),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    default_args=default_args,
    tags=["cumulus", "AIRTEMP", "QTE", "APRFC"],
    schedule="45 * * * *",
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_aprfc_qte_01h():
    """
    # APRFC hourly estimated temps

    This pipeline handles download, processing, and derivative product creation for APRFC hourly estimated temps
    Raw data downloaded to S3 and notifies the Cumulus API of new product(s)

    URLs:
    - BASE - https://nomads.ncep.noaa.gov/pub/data/nccf/com/urma/prod/akurma.YYYYMMDD/

    Filename/Dir Pattern:

    URL Dir - https://nomads.ncep.noaa.gov/pub/data/nccf/com/urma/prod/akurma.YYYYMMDD/
    Files matching akurma.tHHz.2dvaranl_ndfd_3p0.grb2 - 1 hour\n
    """
    s3_bucket = cumulus.S3_BUCKET
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    URL_ROOT = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/urma/prod/"
    PRODUCT_SLUG = "aprfc-qte-01h"

    filename_template = Template("akurma.t${hr_}z.2dvaranl_ndfd_3p0.grb2 ")

    url_suffix_template = Template("akurma.${date_}")

    @task()
    def download_raw_qte():
        logical_date = get_current_context()["logical_date"]
        date_only = logical_date.strftime("%Y%m%d")

        url_suffix = url_suffix_template.substitute(
            date_=date_only,
        )

        filename = filename_template.substitute(
            hr_=logical_date.strftime("%H"),
        )

        file_dir = f"{URL_ROOT}{url_suffix}"

        s3_filename = f"{date_only}_{filename}"
        s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{s3_filename}"

        print(f"Downloading file: {filename}")

        trigger_download(
            url=f"{file_dir}/{filename}", s3_bucket=s3_bucket, s3_key=s3_key
        )
        return json.dumps(
            {
                "execution": logical_date.isoformat(),
                "s3_key": s3_key,
                "filename": s3_filename,
            }
        )

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        print("Notifying Cumulus: " + payload["filename"])
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
            datetime=payload["execution"],
            s3_key=payload["s3_key"],
        )

    notify_cumulus(download_raw_qte())


aprfc_qte_dag = cumulus_aprfc_qte_01h()
