"""
Acquire and Process PRISM Early
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
    # "start_date": (datetime.utcnow()-timedelta(hours=36)).replace(minute=0, second=0),
    "start_date": datetime(2021, 4, 1),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

@dag(default_args=default_args, schedule_interval='30 12 * * *', tags=['cumulus'])
def cumulus_prism_early():
    """This pipeline handles download, processing, and derivative product creation for \n
    PRISM: Min Temp (tmin) early, Max Temp (tmax) early and Precip (ppt) early
    URL Dir - ftp://prism.nacse.org/daily/tmin/YYYY/
    Files matching PRISM_tmin_early_4kmD2_YYYYMMDD_bil.zip - Daily around 12:30-14:30 UTC
    """

    URL_ROOT = f'ftp://prism.nacse.org/daily'
    S3_BUCKET = 'cwbi-data-develop'

    # Download Tasks
    #################################################
    @task()
    def download_raw_tmin_early():
        product_slug = 'prism-tmin-early'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/tmin/{execution_date.strftime("%Y")}'
        filename = f'PRISM_tmin_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key, "product_slug":product_slug})

    @task()
    def download_raw_tmax_early():
        product_slug = 'prism-tmax-early'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/tmax/{execution_date.strftime("%Y")}'
        filename = f'PRISM_tmax_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key, "product_slug":product_slug})

    @task()
    def download_raw_ppt_early():
        product_slug = 'prism-ppt-early'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/ppt/{execution_date.strftime("%Y")}'
        filename = f'PRISM_ppt_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key, "product_slug":product_slug})

    # Notify Tasks
    #################################################
    @task()
    def notify_cumulus(payload):        
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[payload['product_slug']], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key']
            )
    
    notify_cumulus(download_raw_tmin_early())
    notify_cumulus(download_raw_tmax_early())
    notify_cumulus(download_raw_ppt_early())

prism_dag = cumulus_prism_early()