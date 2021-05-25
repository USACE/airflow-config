"""
Acquire and Process CBRFC MPE (for SPL)
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
    'end_date': datetime(1941, 1, 1)
}

@dag(default_args=default_args, schedule_interval='@yearly', tags=['cumulus', 'precip', 'develop'], concurrency=2, max_active_runs=2)
def develop_cumulus_copy_columbia_wrf():
    """This pipeline handles download, processing, and derivative product creation for \n
    Columbia WRF\n
    Existing in S3 bucket.  Will be copied to Cumulus and Processed
    """

    S3_SRC_BUCKET = 'columbia-river'
    S3_SRC_KEY_PREFIX = 'wrfout/d03fmt/reconstruction'
    S3_DST_BUCKET = 'cwbi-data-develop'
    PRODUCT_SLUG = 'wrf-columbia-precip'

    @task()
    def copy_precip_s():
        execution_date = get_current_context()['execution_date']

        src_filename = 'PRECIPAH.nc'
        dst_filename = f'precipah_{execution_date.strftime("%Y")}_s.nc'
        src_s3_key = f'{S3_SRC_KEY_PREFIX}/{execution_date.strftime("%Y")}s/ncf/{src_filename}'
        dst_s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{dst_filename}'
        print(f'Copying {src_s3_key} to {dst_s3_key}')
        copy_s3_file(S3_SRC_BUCKET, src_s3_key, S3_DST_BUCKET, dst_s3_key)                

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":dst_s3_key})

    @task()
    def copy_precip_w():
        execution_date = get_current_context()['execution_date']

        src_filename = 'PRECIPAH.nc'
        dst_filename = f'precipah_{execution_date.strftime("%Y")}_w.nc'
        src_s3_key = f'{S3_SRC_KEY_PREFIX}/{execution_date.strftime("%Y")}w/ncf/{src_filename}'
        dst_s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{dst_filename}'
        print(f'Copying {src_s3_key} to {dst_s3_key}')
        copy_s3_file(S3_SRC_BUCKET, src_s3_key, S3_DST_BUCKET, dst_s3_key) 

        time.sleep(1800)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":dst_s3_key})

    @task()
    def notify_cumulus(payload):
        
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key'],
            conn_type='develop'
            )


    notify_cumulus(copy_precip_s())
    notify_cumulus(copy_precip_w())


wrf_dag = cumulus_copy_columbia_wrf()