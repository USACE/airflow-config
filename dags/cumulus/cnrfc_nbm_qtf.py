"""
## California Nevada River Forecast Center (CNRFC) NBM QTF (1 hour)

6 Hour files are downloaded, hourly bands are extracted.

URL Structure: https://www.cnrfc.noaa.gov/archive/YYYY/MMM/netcdfFcstTemp/fcstTemperature.YYYYMMdd_HHMM.nc.gz

01/07/13/19Z runs of NBM, updated within about 1.5 hours of model time

Additional Information: https://cnrfc.noaa.gov/documents/CNRFC_Archive_Download_Instructions.pdf

Comment: Availability is at 0000, 0700, 1200, 1900. This is slightly different than the expected 01/07/13/19Z.

Comment 2: Hourly data, issued every 6 hours, NetCDF, 4.7 km resolution. This was preferred to the NOMADS data; tradeoff of hourly timescale vs. spatial resolution.

"""

import json, logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download
from airflow.exceptions import AirflowSkipException

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
    "retries": 6,
    "retry_delay": timedelta(minutes=60),
    # "end_date": datetime(2022, 8, 5),
}


@dag(
    default_args=default_args,
    schedule="3 0,7,12,19 * * *",
    tags=["cumulus", "airtemp", "QTF", "NBM", "CNRFC"],
    max_active_runs=2,
    max_active_tasks=4,
    doc_md=__doc__,
)
def cumulus_cnrfc_nbm_qtf():
    URL_ROOT = f"https://www.cnrfc.noaa.gov/archive"
    PRODUCT_SLUG = "cnrfc-nbm-qtf-01h"

    @task()
    def download_cnrfc_nbm_qtf():
        logical_date = get_current_context()["logical_date"]
        # logical_date = logical_date + timedelta(hours=6)

        dirpath = f'{logical_date.strftime("%Y")}/{logical_date.strftime("%b")}/netcdfFcstTemp'

        filename = f'fcstTemperature.{logical_date.strftime("%Y%m%d_%H00")}.nc.gz'
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

    notify_cumulus(download_cnrfc_nbm_qtf())


DAG_ = cumulus_cnrfc_nbm_qtf()
