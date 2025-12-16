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
from helpers.downloads import s3_file_exists, trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=72)).replace(
        minute=0, second=0
    ),
    "catchup": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    default_args=default_args,
    tags=["cumulus", "AIRTEMP", "QTE", "APRFC"],
    schedule="45 * * * *",
    max_active_runs=5,
    max_active_tasks=5,
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
    LOOKBACK_HOURS = 12 # number of hours from runtime to look back for

    filename_template = Template("akurma.t${hr_}z.2dvaranl_ndfd_3p0.grb2")

    url_suffix_template = Template("akurma.${date_}")

    @task()
    def download_raw_qte():
        logical_date = get_current_context()["logical_date"]
        anchor = get_current_context()["data_interval_end"].replace(minute=0, second=0, microsecond=0)

        results = []

        for offset in range(LOOKBACK_HOURS):
            ts = anchor - timedelta(hours=1 + offset)  # last complete hour, then look back
            date_only = ts.strftime("%Y%m%d")
            hour_str = ts.strftime("%H")


            url_suffix = url_suffix_template.substitute(
                date_=date_only,
            )

            filename = filename_template.substitute(
                hr_=hour_str,
            )

            file_dir = f"{URL_ROOT}{url_suffix}"

            s3_filename = f"{date_only}_{filename}"
            s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{s3_filename}"

            if s3_file_exists(cumulus.S3_BUCKET, s3_key):
               print(f"Skipping existing S3 object: s3://{cumulus.S3_BUCKET}/{s3_key}")
               continue  # Skip to the next file

            print(f"Downloading file: {url_suffix}/{filename}")

            try:

                trigger_download(
                    url=f"{file_dir}/{filename}", s3_bucket=s3_bucket, s3_key=s3_key
                )
            except:
                print(f'Failed downloading {filename}')

            results.append(
                {
                    "execution": ts.isoformat(),
                    "s3_key": s3_key,
                    "filename": s3_filename,
                }
            )
        return json.dumps(results)

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

    notify_cumulus(download_raw_qte())


aprfc_qte_dag = cumulus_aprfc_qte_01h()
