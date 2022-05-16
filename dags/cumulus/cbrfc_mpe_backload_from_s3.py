"""
Acquire and Process CBRFC MPE (for SPL)
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download, copy_s3_file

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 25),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    "end_date": datetime(2019, 12, 31),
    "max_active_runs": 5,
}


@dag(
    default_args=default_args,
    schedule_interval="@hourly",
    tags=["cumulus", "precip"],
)
def cumulus_cbrfc_mpe_backload_from_s3():
    """This pipeline handles reading of existing S3 acquirables, processing, and derivative product creation for \n
    CBRFC Multisensor Precipitation Estimates (MPE)\n
    Files matching xmrgMMDDYYYYHHz.grb - Hourly
    """

    # URL_ROOT = f"https://www.cbrfc.noaa.gov/outgoing/usace_la"
    PRODUCT_SLUG = "cbrfc-mpe"

    @task()
    def copy_acquirable_file():
        execution_date = get_current_context()["execution_date"]

        filename = f'xmrg{execution_date.strftime("%m%d%Y%H")}z.grb'
        src_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/cbrfc_mpe_from_CPC/{filename}"
        dst_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"

        print(f"Copying {src_s3_key} to {dst_s3_key}")
        copy_s3_file(cumulus.S3_BUCKET, src_s3_key, cumulus.S3_BUCKET, dst_s3_key)

        return json.dumps(
            {"datetime": execution_date.isoformat(), "s3_key": dst_s3_key}
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

    notify_cumulus(copy_acquirable_file())


cbrfc_dag = cumulus_cbrfc_mpe_backload_from_s3()
