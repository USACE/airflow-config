"""
Acquire and Process CBRFC MPE (for SPL)
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
    "start_date": datetime(2021, 3, 7),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 72,
    "retry_delay": timedelta(minutes=60),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

@dag(default_args=default_args, schedule_interval='15 * * * *', tags=['cumulus', 'precip'])
def download_and_process_cbrfc_mpe():
    """This pipeline handles download, processing, and derivative product creation for \n
    CBRFC Multisensor Precipitation Estimates (MPE)\n
    URL Dir - https://www.cbrfc.noaa.gov/outgoing/usace_la/\n
    Files matching xmrgMMDDYYYYHHz.grb - Hourly
    """

    URL_ROOT = f'https://www.cbrfc.noaa.gov/outgoing/usace_la'

    @task()
    def download_raw_cbrfc_mpe():
        s3_key_dir = f'cumulus/cbrfc_mpe'
        execution_date = get_current_context()['execution_date']
        # file_dir = f'{URL_ROOT}/{execution_date.strftime("%Y")}'
        filename = f'xmrg{execution_date.strftime("%m%d%Y%H")}z.grb'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{URL_ROOT}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'{s3_key_dir}/{filename}')

    @task()
    def cbrfc_mpe_process_cogs(output):
        result = trigger_sqs(queue_name="cumulus-test", message=json.dumps(output))

    download_raw_cbrfc_mpe()

cbrfc_dag = download_and_process_cbrfc_mpe()