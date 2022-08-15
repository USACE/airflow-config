"""
Acquire and Process CNRFC QPE
"""

import json, logging
from datetime import datetime, timedelta
import calendar

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download
from airflow.exceptions import AirflowSkipException

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=48)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    # "end_date": datetime(2022, 8, 5),
}


@dag(
    default_args=default_args,
    schedule_interval="15 12,18 * * *",
    tags=["cumulus", "precip", "QPF", "CNRFC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_cnrfc_qpf():
    """This pipeline handles download, processing, and derivative product creation for \n
    CNRFC QPF\n
    URL Dir - https://www.cnrfc.noaa.gov/archive/YYYY/MMM/netcdfqpf/qpe.YYYYMMdd_HHMM.nc.gz\n
    Files matching qpf.YYYYMMdd_HHMM.nc.gz - 6 hour\n
    Updated 1x/day summer and 2x/day winter except 1x/day weekends/holidays\n
    Skip failed attempts for 1800 if weekend day or is_summer (see code for logic)
    """

    URL_ROOT = f"https://www.cnrfc.noaa.gov/archive"
    PRODUCT_SLUG = "cnrfc-qpf-06h"

    @task()
    def download_raw_cnrfc_qpf():
        logical_date = get_current_context()["logical_date"]

        # This isn't exactly summer/winter seasons, but relects approx when
        # NCRFC stops/starts issuing second forecast (1800) of the day
        is_summer = (
            True if logical_date.month > 4 and logical_date.month < 10 else False
        )

        dirpath = (
            f'{logical_date.strftime("%Y")}/{logical_date.strftime("%b")}/netcdfqpf'
        )
        filename = f'qpf.{logical_date.strftime("%Y%m%d_%H00")}.nc.gz'
        filepath = f"{dirpath}/{filename}"
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        print(f"Downloading {filepath}")

        try:
            trigger_download(
                url=f"{URL_ROOT}/{filepath}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
            )
        except:
            # if it fails on the 1800 product on a weekend or is summer season, skip the task
            if logical_date.hour == 18 and (logical_date.weekday() > 4 or is_summer):
                logging.info("Skipping 1800 attempt.")
                raise AirflowSkipException

            logging.error(f"File not found: {filepath}")
            raise ValueError("File not available")

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

    notify_cumulus(download_raw_cnrfc_qpf())


cbrfc_dag = cumulus_cnrfc_qpf()
