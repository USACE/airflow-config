import os, json
from datetime import datetime, timedelta, timezone

# from dateutil.relativedelta import relativedelta
# from airflow.exceptions import AirflowSkipException

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import copy_s3_file

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow() - timedelta(hours=48)).replace(minute=0, second=0),
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2019, 12, 30),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}


@dag(
    default_args=default_args,
    schedule_interval="@hourly",
    tags=["cumulus", "airtemp"],
    max_active_runs=4,
)
def cumulus_ndgd_rtma_airtemp_backload():
    """Copy from CPC Dir and rename placing into correct S3 acquirable dir and notifying API to process.\n
    Files will be processed by hour."""

    PRODUCT_SLUG = "ndgd-ltia98-airtemp"

    @task()
    def copy_raw_airtemp():

        execution_date = get_current_context()["logical_date"]

        month_dir = f'NCEP-rtma_airtemp-{execution_date.strftime("%Y.%m")}'
        # old name looks like: 2019.10.31.23--NCEP-rtma_precip--RT.23.ds.precipa.bin
        src_filename = f'{execution_date.strftime("%Y.%m.%d.%H")}--NCEP-rtma_airtemp--RT.{execution_date.strftime("%H")}.ds.temp.bin'
        src_key = (
            f"{cumulus.S3_ACQUIRABLE_PREFIX}/ltia98_from_CPC/{month_dir}/{src_filename}"
        )
        # convert to name like: ds.precipa_20211017_11.bin
        dst_filename = f'ds.temp_{execution_date.strftime("%Y%m%d_%H")}.bin'
        dst_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{dst_filename}"

        print(f"Copying {src_key} to {dst_key}")
        copy_s3_file(cumulus.S3_BUCKET, src_key, cumulus.S3_BUCKET, dst_key)

        return json.dumps({"datetime": execution_date.isoformat(), "s3_key": dst_key})

    @task()
    def notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

        print(f"Sent {len(payload)} notifications to API")

        return

    # copy_raw_precip()
    notify_cumulus(copy_raw_airtemp())


ndgd_rtma_dag = cumulus_ndgd_rtma_airtemp_backload()
