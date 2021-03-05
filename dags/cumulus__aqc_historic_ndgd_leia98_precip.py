"""
Acquire and Process Historic NDGD LTEA98 Precip
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import task, get_current_context
from helpers.downloads import trigger_download, read_s3_file

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2011, 1, 1),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2012, 1, 1),
}

@dag(default_args=default_args, schedule_interval='0 * * * *', tags=['cumulus', 'historic', 'precip'])
def download_and_process_historic_leia98():
    """This pipeline handles download and processing for \n
    URL Dir - https://www.ncei.noaa.gov/data/national-digital-guidance-database/access/
    Files matching LEIA98_KWBR_YYYYMMDDHHMM
    """

    URL_ROOT = f'https://www.ncei.noaa.gov/data/national-digital-guidance-database/access'

    @task()
    def download_raw_leia98():
        s3_key_dir = f'cumulus/ndgd_leia98_precip'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/historical/{execution_date.strftime("%Y%m")}/{execution_date.strftime("%Y%m%d")}'
        filename = f'LEIA98_KWBR_{execution_date.strftime("%Y%m%d%H%M")}'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'{s3_key_dir}/{filename}')

    download_raw_leia98()

leia98_dag = download_and_process_historic_leia98()