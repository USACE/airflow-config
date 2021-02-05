"""
Acquire and Process SNODAS Dataset
"""

import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import task, get_current_context

from datetime import datetime, timedelta

from helpers.downloads import trigger_download
from helpers.sqs import trigger_sqs

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 10, 0, 0, 0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# An Example Using the Taskflow API
@dag(default_args=default_args, schedule_interval='0 3 * * *', tags=['cumulus'])
def cumulus_download_and_process_snodas():
    """This pipeline handles download, processing, and derivative product creation for NOHRSC SNODAS Products"""

    @task()
    def snodas_download_unmasked():
        execution_date = get_current_context()['execution_date']
        URL_ROOT = f'ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/unmasked/{execution_date.year}/{execution_date.strftime("%m_%b")}'
        FILENAME = f'SNODAS_unmasked_{execution_date.strftime("%Y%m%d")}.tar'
        output = trigger_download(url=f'{URL_ROOT}/{FILENAME}', s3_bucket='corpsmap-data-incoming', s3_key=f'cumulus/nohrsc_snodas_unmasked/{FILENAME}')
        return output
    
    @task()
    def snodas_process_cogs(output):
        result = trigger_sqs(queue_name="cumulus-test", message=json.dumps(output))


    downloaded = snodas_download_unmasked()
    printit = snodas_process_cogs(downloaded)


snodas_dag = cumulus_download_and_process_snodas()