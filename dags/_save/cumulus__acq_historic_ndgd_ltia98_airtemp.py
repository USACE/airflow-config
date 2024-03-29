"""
Acquire and Process Historic NDGD LTIA98 Temps
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download, read_s3_file

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    "end_date": datetime(2020, 5, 15),
}


@dag(
    default_args=default_args,
    schedule="0 * * * *",
    tags=["cumulus", "historic", "airtemp"],
)
def cumulus_historic_ndgd_ltia98():
    """This pipeline handles download and processing for \n
    URL Dir - https://www.ncei.noaa.gov/data/national-digital-guidance-database/access/
    Files matching LTIA98_KWBR_YYYYMMDDHHMM
    """

    URL_ROOT = (
        f"https://www.ncei.noaa.gov/data/national-digital-guidance-database/access"
    )
    PRODUCT_SLUG = "ndgd-ltia98-airtemp"

    @task()
    def download_raw_ltia98():

        execution_date = get_current_context()["execution_date"]
        file_dir = f'{URL_ROOT}/historical/{execution_date.strftime("%Y%m")}/{execution_date.strftime("%Y%m%d")}'
        filename = f'LTIA98_KWBR_{execution_date.strftime("%Y%m%d%H%M")}'
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        print(f"Downloading {filename}")
        output = trigger_download(
            url=f"{file_dir}/{filename}", s3_bucket="cwbi-data-stable", s3_key=s3_key
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
            conn_type="stable",
        )

    notify_cumulus(download_raw_ltia98())


ltia98_dag = cumulus_historic_ndgd_ltia98()
