"""
Acquire and Process Historic NDGD RTMA Precip
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 16),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 12,
    "retry_delay": timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 3, 14),
}

@dag(default_args=default_args, schedule_interval='5 * * * *', tags=['cumulus', 'precip'])
def cumulus_ndgd_leia98():
    """This pipeline handles download and processing for \n
    URL Dir - https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndgd/GT.rtma/AR.conus/ \n
    Files matching RT.HH/ds.precipa.bin\n
    Note: This source does not support prior day/month/year data
    """

    URL_ROOT = f'https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndgd/GT.rtma/AR.conus'
    PRODUCT_SLUG = 'ndgd-leia98-precip'

    @task()
    def download_raw_leia98():
        
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/RT.{execution_date.strftime("%H")}'
        filename = 'ds.precipa.bin'
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/ds.precipa_{execution_date.strftime("%Y%m%d_%H")}.bin'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='cwbi-data-stable', s3_key=s3_key)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key})

    @task()
    def notify_cumulus(payload):
        
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key'],
            conn_type='stable'
            )

    notify_cumulus(download_raw_leia98())

leia98_dag = cumulus_ndgd_leia98()