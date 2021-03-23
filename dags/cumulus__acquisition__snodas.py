"""
Acquire and Process SNODAS Dataset
"""

import json

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.decorators import task

from datetime import datetime, timedelta, timezone

from helpers.downloads import trigger_download
from helpers.sqs import trigger_sqs
import time

from helpers.config import SERVICE_DISOVERY_NAME

default_args = {
    "owner": "geoprocess_user",
    "depends_on_past": False,
    "start_date": datetime(2014, 1, 1, 1, 0, 0),
    #"start_date": (datetime.utcnow()-timedelta(hours=96)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 16,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2014, 1, 2, 2, 0, 0),
}

# An Example Using the Taskflow API
@dag(default_args=default_args, schedule_interval='0 1 * * *', tags=['cumulus'])
def cumulus_download_and_process_snodas():
    """This pipeline handles download, processing, and derivative product creation for NOHRSC SNODAS Products\n
    Product timestamp is usually around 0320 AM EST (0820 UTC), but may not be actual time published to FTP site.
    """


    # @task()
    # def snodas_download_unmasked():
        
    #     # In order to get the current day's file, set execution forward 1 day
    #     execution_date = get_current_context()['execution_date']+timedelta(hours=24)
        
    #     URL_ROOT = f'ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/unmasked/{execution_date.year}/{execution_date.strftime("%m_%b")}'
    #     FILENAME = f'SNODAS_unmasked_{execution_date.strftime("%Y%m%d")}.tar'
    #     output = trigger_download(url=f'{URL_ROOT}/{FILENAME}', s3_bucket='corpsmap-data-incoming', s3_key=f'cumulus/nohrsc_snodas_unmasked/{FILENAME}')
    #     return output
    
    @task()
    def snodas_process_cogs():
        dag_id = get_current_context()['dag_run'].dag_id
        message = {
            "process": "incoming-file-to-cogs",
            "airflow":{
                "callback_url": f"http://{SERVICE_DISOVERY_NAME}/api/v1/dags/{dag_id}/updateTaskInstancesState",
                "payload": {
                    "dag_id":dag_id,
                    "dag_run_id": get_current_context()['dag_run'].run_id,
                    "task_id": get_current_context()['task'].task_id,
                    "dry_run": False,
                    "execution_date": get_current_context()['execution_date'].isoformat(),
                    "include_downstream": False,
                    "include_future": False,
                    "include_past": False,
                    "include_upstream": False,
                    "new_state": "success"
                }
            },             
            "process_config": {
                "bucket": "corpsmap-data-incoming",
                "key": "cumulus/nohrsc_snodas_unmasked/SNODAS_unmasked_20140101.tar"
            }
        }
        result = trigger_sqs(queue_name="cumulus-geoprocess", message=json.dumps(message))


    # downloaded = snodas_download_unmasked()
    # printit = snodas_process_cogs(downloaded)
    snodas_process_cogs()


snodas_dag = cumulus_download_and_process_snodas()