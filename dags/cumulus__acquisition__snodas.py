"""
Acquire and Process SNODAS Dataset
"""

import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import task, get_current_context

from datetime import datetime, timedelta

from helpers.downloads import trigger_download
# from helpers.sqs import trigger_sqs

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2021, 1, 10, 0, 0, 0),
    "start_date": (datetime.utcnow()-timedelta(hours=96)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 16,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# An Example Using the Taskflow API
@dag(default_args=default_args, schedule_interval='20 8 * * *', tags=['cumulus'])
def cumulus_download_and_process_snodas():
    """This pipeline handles download, processing, and derivative product creation for NOHRSC SNODAS Products\n
    Product timestamp is usually around 0320 AM EST (0820 UTC), but may not be actual time published to FTP site.
    """

    PRODUCT_SLUG = 'nohrsc-snodas-unmasked'

    @task()
    def snodas_download_unmasked():
        
        # In order to get the current day's file, set execution forward 1 day
        execution_date = get_current_context()['execution_date']+timedelta(hours=24)
        
        URL_ROOT = f'ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/unmasked/{execution_date.year}/{execution_date.strftime("%m_%b")}'        
        filename = f'SNODAS_unmasked_{execution_date.strftime("%Y%m%d")}.tar'
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}'
        output = trigger_download(url=f'{URL_ROOT}/{filename}', s3_bucket='corpsmap-data', s3_key=s3_key)
        
        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key})
    
    @task()
    def snodas_unmasked_notify_cumulus(payload):
        
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key']
            )


    snodas_unmasked_notify_cumulus(snodas_download_unmasked())

snodas_dag = cumulus_download_and_process_snodas()