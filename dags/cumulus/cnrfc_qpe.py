"""
Acquire and Process CNRFC QPE
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
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 24,
    "retry_delay": timedelta(minutes=60),
}


@dag(
    default_args=default_args,
    schedule_interval="15 0,6,12,18 * * *",
    tags=["cumulus", "precip", "QPE", "CNRFC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_cnrfc_qpe():
    """This pipeline handles download, processing, and derivative product creation for \n
    CNRFC QPE\n
    URL Dir - https://www.cnrfc.noaa.gov/archive/YYYY/MMM/netcdfqpe/qpe.YYYYMMdd_HHMM.nc.gz\n
    Files matching qpe.YYYYMMdd_HHMM.nc.gz - 6 hour
    """

    URL_ROOT = f"https://www.cnrfc.noaa.gov/archive"
    PRODUCT_SLUG = "cnrfc-qpe-06h"

    @task()
    def download_raw_cnrfc_qpe():
        logical_date = get_current_context()["logical_date"]

        last_day_of_current_month = calendar.monthrange(
            logical_date.year, logical_date.month
        )[1]

        # The last qpe file for the month (1800 UTC) is stored in the next month
        # If logical_date is 2022-07-31 and time is 1800, file will be stored
        # in August dir, but with July filename

        # If last day of month and 18th hour
        if logical_date.day == last_day_of_current_month and logical_date.hour == 18:
            # determine the next day (first day of next month)
            next_datetime = logical_date + timedelta(days=1)
            dirpath = f'{next_datetime.strftime("%Y")}/{next_datetime.strftime("%b")}/netcdfqpe'
            # filename will be from prev month, stored in next months folder
            filename = f'qpe.{logical_date.strftime("%Y%m%d_1800")}.nc.gz'
            filepath = f"{dirpath}/{filename}"
            s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
            print(f"Downloading prev month file: {filepath}")
            dt = next_datetime

        else:

            dirpath = (
                f'{logical_date.strftime("%Y")}/{logical_date.strftime("%b")}/netcdfqpe'
            )
            filename = f'qpe.{logical_date.strftime("%Y%m%d_%H00")}.nc.gz'
            filepath = f"{dirpath}/{filename}"
            s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
            print(f"Downloading {filepath}")
            dt = logical_date

        output = trigger_download(
            url=f"{URL_ROOT}/{filepath}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps(
            {"datetime": dt.isoformat(), "s3_key": s3_key},
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

    notify_cumulus(download_raw_cnrfc_qpe())


cbrfc_dag = cumulus_cnrfc_qpe()
