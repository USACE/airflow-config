"""
Acquire and Process PRISM Stable
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
    "start_date": datetime(1981, 1, 1),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(1981, 1, 5),
}

@dag(default_args=default_args, schedule_interval='0 12 * * *', tags=['cumulus', 'historic'])
def cumulus_prism_stable():
    """This pipeline handles download, processing, and derivative product creation for \n
    PRISM: Min Temp (tmin) stable, Max Temp (tmax) stable and Precip (ppt) stable
    URL Dir - ftp://prism.nacse.org/daily/
    Files matching PRISM_tmin_stable_4kmD2_YYYYMMDD_bil.zip
    """

    URL_ROOT = f'ftp://prism.nacse.org/daily'

    @task()
    def download_raw_tmin_stable():
        s3_key_dir = f'cumulus/prism_tmin_stable'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/tmin/{execution_date.strftime("%Y")}'
        filename = f'PRISM_tmin_stable_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='cwbi-data-develop', s3_key=f'{s3_key_dir}/{filename}')

    @task()
    def download_raw_tmax_stable():
        s3_key_dir = f'cumulus/prism_tmax_stable'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/tmax/{execution_date.strftime("%Y")}'
        filename = f'PRISM_tmax_stable_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='cwbi-data-develop', s3_key=f'{s3_key_dir}/{filename}')

    @task()
    def download_raw_ppt_stable():
        s3_key_dir = f'cumulus/prism_ppt_stable'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/ppt/{execution_date.strftime("%Y")}'
        filename = f'PRISM_ppt_stable_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='cwbi-data-develop', s3_key=f'{s3_key_dir}/{filename}')

    
    download_raw_tmin_stable()
    download_raw_tmax_stable()
    download_raw_ppt_stable()

prism_dag = cumulus_prism_stable()