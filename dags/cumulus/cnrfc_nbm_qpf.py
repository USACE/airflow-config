"""
## California Nevada River Forecast Center (CNRFC) NBM QPF (6 hourly)

URL Structure: https://www.cnrfc.noaa.gov/archive/YYYY/MMM/netcdfQpfNbm/QPF.YYYYMMdd_HHMM.nc.gz

01/07/13/19Z runs of NBM, updated within about 1.5 hours of model time

Index: Not found

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
    schedule="2 1,7,13,19 * * *",
    tags=["cumulus", "precip", "QPF", "NBM", "CNRFC"],
    max_active_runs=2,
    max_active_tasks=4,
    doc_md=__doc__,
)
def cumulus_cnrfc_nbm_qpf():

    URL_ROOT = f"https://www.cnrfc.noaa.gov/archive"
    PRODUCT_SLUG = "cnrfc-nbm-qpf-06h"

    @task()
    def download_cnrfc_nbm_qpf():
        logical_date = get_current_context()["logical_date"]
        # logical_date = logical_date + timedelta(hours=6)

        dirpath = (
            f'{logical_date.strftime("%Y")}/{logical_date.strftime("%b")}/netcdfQpfNbm'
        )
        filename = f'QPF.{logical_date.strftime("%Y%m%d_%H00")}.nc.gz'
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

    notify_cumulus(download_cnrfc_nbm_qpf())


DAG_ = cumulus_cnrfc_nbm_qpf()
