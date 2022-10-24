"""
Acquire and Process PRISM Stable
"""

import json
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, PythonOperator
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(1981, 5, 31),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    "end_date": datetime(2022, 3, 31),
}


@dag(
    default_args=default_args,
    schedule_interval="0 12 * * *",
    tags=["cumulus", "historic"],
    max_active_runs=2,
)
def cumulus_prism_stable():
    """This pipeline handles download, processing, and derivative product creation for \n
    PRISM: Min Temp (tmin) stable, Max Temp (tmax) stable and Precip (ppt) stable
    URL Dir - ftp://prism.nacse.org/daily/
    Files matching PRISM_tmin_stable_4kmD2_YYYYMMDD_bil.zip
    """

    URL_ROOT = f"ftp://prism.nacse.org/daily"

    @task()
    def download_raw_tmin_stable():
        product_slug = "prism-tmin-stable"
        execution_date = get_current_context()["logical_date"]
        file_dir = f'{URL_ROOT}/tmin/{execution_date.strftime("%Y")}'
        filename = (
            f'PRISM_tmin_stable_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        )
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"
        print(f"Downloading {filename}")
        output = trigger_download(
            url=f"{file_dir}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": s3_key,
                "product_slug": product_slug,
            }
        )

    @task()
    def download_raw_tmax_stable():
        product_slug = "prism-tmax-stable"
        execution_date = get_current_context()["logical_date"]
        file_dir = f'{URL_ROOT}/tmax/{execution_date.strftime("%Y")}'
        filename = (
            f'PRISM_tmax_stable_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        )
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"
        print(f"Downloading {filename}")
        output = trigger_download(
            url=f"{file_dir}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": s3_key,
                "product_slug": product_slug,
            }
        )

    @task()
    def download_raw_ppt_stable():
        product_slug = "prism-ppt-stable"
        execution_date = get_current_context()["logical_date"]
        file_dir = f'{URL_ROOT}/ppt/{execution_date.strftime("%Y")}'
        filename = f'PRISM_ppt_stable_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"
        print(f"Downloading {filename}")
        output = trigger_download(
            url=f"{file_dir}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": s3_key,
                "product_slug": product_slug,
            }
        )

    # Notify Tasks
    #################################################
    @task()
    def notify_cumulus(payload):
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[payload["product_slug"]],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

    @task()
    def delay_task():
        PythonOperator(task_id="python_delay", python_callable=lambda: time.sleep(15))

    notify_cumulus(download_raw_tmin_stable())
    notify_cumulus(download_raw_tmax_stable())
    notify_cumulus(download_raw_ppt_stable())
    delay_task()


prism_dag = cumulus_prism_stable()
