"""
Acquire and Process PRISM Early
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import task, get_current_context
from helpers.downloads import trigger_download

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow()-timedelta(hours=36)).replace(minute=0, second=0),
    "start_date": datetime(2021, 2, 20),
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
def download_and_process_prism_early():
    """This pipeline handles download, processing, and derivative product creation for \n
    PRISM: Min Temp (tmin) early, Max Temp (tmax) early and Precip (ppt) early
    URL Dir - ftp://prism.nacse.org/daily/tmin/YYYY/
    Files matching PRISM_tmin_early_4kmD2_YYYYMMDD_bil.zip - Daily around 12:30-14:30 UTC
    """

    URL_ROOT = f'ftp://prism.nacse.org/daily'

    @task()
    def download_raw_tmin_early():
        s3_key_dir = f'cumulus/prism_tmin_early'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/tmin/{execution_date.strftime("%Y")}'
        filename = f'PRISM_tmin_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'{s3_key_dir}/{filename}')

    @task()
    def download_raw_tmax_early():
        s3_key_dir = f'cumulus/prism_tmax_early'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/tmax/{execution_date.strftime("%Y")}'
        filename = f'PRISM_tmax_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'{s3_key_dir}/{filename}')

    @task()
    def download_raw_ppt_early():
        s3_key_dir = f'cumulus/prism_ppt_early'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/ppt/{execution_date.strftime("%Y")}'
        filename = f'PRISM_ppt_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'{s3_key_dir}/{filename}')

    
    download_raw_tmin_early()
    download_raw_tmax_early()
    download_raw_ppt_early()

prism_dag = download_and_process_prism_early()