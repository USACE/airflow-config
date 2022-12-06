"""
Acquire and Process CBRFC MPE (for SPL)
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=48)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 4, 4),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 24,
    "retry_delay": timedelta(minutes=60),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 4, 4),
}


@dag(
    default_args=default_args,
    schedule="15 * * * *",
    tags=["cumulus", "precip"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_cbrfc_mpe():
    """This pipeline handles download, processing, and derivative product creation for \n
    CBRFC Multisensor Precipitation Estimates (MPE)\n
    URL Dir - https://www.cbrfc.noaa.gov/outgoing/usace_la/\n
    Files matching xmrgMMDDYYYYHHz.grb - Hourly
    """

    URL_ROOT = f"https://www.cbrfc.noaa.gov/outgoing/usace_la"
    PRODUCT_SLUG = "cbrfc-mpe"

    @task()
    def download_raw_cbrfc_mpe():
        execution_date = get_current_context()["logical_date"]
        filename = f'xmrg{execution_date.strftime("%m%d%Y%H")}z.grb'
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        print(f"Downloading {filename}")
        output = trigger_download(
            url=f"{URL_ROOT}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
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
        )

    notify_cumulus(download_raw_cbrfc_mpe())


cbrfc_dag = cumulus_cbrfc_mpe()
