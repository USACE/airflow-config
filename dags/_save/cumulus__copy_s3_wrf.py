"""
Copy and Process Columbia WRF Data
"""

import os, json, logging
import requests
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import copy_s3_file

import helpers.cumulus as cumulus


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow()-timedelta(hours=48)).replace(minute=0, second=0),
    "start_date": datetime(1928, 1, 1),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    "end_date": datetime(2018, 1, 1),
}


@dag(
    default_args=default_args, schedule="@yearly", tags=["cumulus", "precip", "airtemp"]
)
def cumulus_copy_columbia_wrf():
    """This pipeline handles download, processing, and derivative product creation for \n
    Columbia WRF\n
    Existing in S3 bucket.  Will be copied to Cumulus and Processed
    """

    S3_SRC_BUCKET = "columbia-river"
    S3_SRC_KEY_PREFIX = "wrfout/d03fmt/reconstruction"
    S3_DST_BUCKET = "cwbi-data-stable"

    @task()
    def copy_precip_s():
        execution_date = get_current_context()["execution_date"]
        product_slug = "wrf-columbia-precip"

        src_filename = "PRECIPAH.nc"
        dst_filename = f'precipah_{execution_date.strftime("%Y")}_s.nc'
        src_s3_key = (
            f'{S3_SRC_KEY_PREFIX}/{execution_date.strftime("%Y")}s/ncf/{src_filename}'
        )
        dst_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{dst_filename}"
        print(f"Copying {src_s3_key} to {dst_s3_key}")
        copy_s3_file(S3_SRC_BUCKET, src_s3_key, S3_DST_BUCKET, dst_s3_key)

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": dst_s3_key,
                "product_slug": product_slug,
            }
        )

    @task()
    def copy_precip_w():
        execution_date = get_current_context()["execution_date"]
        product_slug = "wrf-columbia-precip"

        src_filename = "PRECIPAH.nc"
        dst_filename = f'precipah_{execution_date.strftime("%Y")}_w.nc'
        src_s3_key = (
            f'{S3_SRC_KEY_PREFIX}/{execution_date.strftime("%Y")}w/ncf/{src_filename}'
        )
        dst_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{dst_filename}"
        print(f"Copying {src_s3_key} to {dst_s3_key}")
        copy_s3_file(S3_SRC_BUCKET, src_s3_key, S3_DST_BUCKET, dst_s3_key)

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": dst_s3_key,
                "product_slug": product_slug,
            }
        )

    @task()
    def copy_airtemp_s():
        execution_date = get_current_context()["execution_date"]
        product_slug = "wrf-columbia-airtemp"

        src_filename = "T2______.nc"
        dst_filename = f't2_airtemp_{execution_date.strftime("%Y")}_s.nc'
        src_s3_key = (
            f'{S3_SRC_KEY_PREFIX}/{execution_date.strftime("%Y")}s/ncf/{src_filename}'
        )
        dst_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{dst_filename}"
        print(f"Copying {src_s3_key} to {dst_s3_key}")
        copy_s3_file(S3_SRC_BUCKET, src_s3_key, S3_DST_BUCKET, dst_s3_key)

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": dst_s3_key,
                "product_slug": product_slug,
            }
        )

    @task()
    def copy_airtemp_w():
        execution_date = get_current_context()["execution_date"]
        product_slug = "wrf-columbia-airtemp"

        src_filename = "T2______.nc"
        dst_filename = f't2_airtemp_{execution_date.strftime("%Y")}_w.nc'
        src_s3_key = (
            f'{S3_SRC_KEY_PREFIX}/{execution_date.strftime("%Y")}w/ncf/{src_filename}'
        )
        dst_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{dst_filename}"
        print(f"Copying {src_s3_key} to {dst_s3_key}")
        copy_s3_file(S3_SRC_BUCKET, src_s3_key, S3_DST_BUCKET, dst_s3_key)

        return json.dumps(
            {
                "datetime": execution_date.isoformat(),
                "s3_key": dst_s3_key,
                "product_slug": product_slug,
            }
        )

    @task()
    def notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[payload["product_slug"]],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
            conn_type="stable",
        )

    # notify_cumulus(copy_precip_s())
    # notify_cumulus(copy_precip_w())

    notify_cumulus(copy_airtemp_s())
    notify_cumulus(copy_airtemp_w())


wrf_dag = cumulus_copy_columbia_wrf()
