"""
Acquire and Process NCEP Stage 4 MOSAIC QPE
"""

import os, json, logging, time
from datetime import datetime, timedelta

# import calendar

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# from airflow.exceptions import AirflowSkipException
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=2)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    schedule="0 * * * *",
    tags=["cumulus", "precip", "QPE", "CONUS", "stage4", "NCEP"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_ncep_stage4_conus_01h():
    """This pipeline handles download, processing, and derivative product creation for \n
    NCEP Stage 4 MOSAIC QPE\n
    URL Dir - https://nomads.ncep.noaa.gov/pub/data/nccf/com/pcpanl/prod/pcpanl.20220808/st4_conus.YYYYMMHHMM.01h.grb2\n
    Files matching st4_conus.YYYYMMHHMM.01h.grb2 - 1 hour
    """

    URL_ROOT = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/pcpanl/prod"
    PRODUCT_SLUG = "ncep-stage4-mosaic-01h"

    def download_product(filepath):

        filename = os.path.basename(filepath)
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        # logging.info(f"Downloading file: {filepath}")
        trigger_download(
            url=f"{URL_ROOT}/{filepath}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )
        return s3_key

    @task()
    def download_stage4_qpe():

        logical_date = get_current_context()["logical_date"]
        dirpath = f'pcpanl.{logical_date.strftime("%Y%m%d")}'
        filename = f'st4_conus.{logical_date.strftime("%Y%m%d%H")}.01h.grb2'
        filepath = f"{dirpath}/{filename}"

        s3_key = download_product(filepath)

        return json.dumps(
            [{"datetime": logical_date.isoformat(), "s3_key": s3_key}],
        )

    @task()
    def notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        for p in payload:
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=p["datetime"],
                s3_key=p["s3_key"],
            )

    @task()
    def download_last_n_products():

        n = 120  # how many products back to download
        logical_date = get_current_context()["logical_date"]
        payload = []

        for h in range(1, n + 1):
            # Skip current hour, we just downloaded it above
            lookback_date = logical_date - timedelta(hours=h)
            # logging.info(f"lookback_date: {lookback_date}")
            dirpath = f'pcpanl.{lookback_date.strftime("%Y%m%d")}'
            filename = f'st4_conus.{lookback_date.strftime("%Y%m%d%H")}.01h.grb2'
            filepath = f"{dirpath}/{filename}"
            try:
                s3_key = download_product(filepath)
                payload.append(
                    {"datetime": lookback_date.isoformat(), "s3_key": s3_key}
                )
            except Exception as err:
                logging.error(f"Failed to download {filepath}.")
                logging.error(err)
                # prevent a single failure from stopping the whole task
                pass

            # sleep to avoid hitting a rate limit on src server
            time.sleep(2)

        return json.dumps(payload)

    notify_cumulus(download_stage4_qpe())
    notify_cumulus(download_last_n_products())


stage4_dag = cumulus_ncep_stage4_conus_01h()
