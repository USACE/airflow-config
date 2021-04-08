"""
Acquire and Process NCEP Real-Time Mesoscale Analysis (RTMA)
2.5km Rapid Update (RU) ANL - Observed CONUS Air Temperatures
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download, read_s3_file

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow()-timedelta(hours=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

@dag(default_args=default_args, schedule_interval='00,15,30,45 * * * *', tags=['cumulus'])
def cumulus_rtma_ru_anl_airtemp():
    """This pipeline handles download, processing, and derivative product creation for \n
    NCEP Real-Time Mesoscale Analysis (RTMA) 2.5km Rapid Update (RU) ANL - Observed CONUS Air Temperatures
    URL Dir - https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5_ru.YYYYMMDD/
    Files matching rtma2p5_ru.t{HH}{MM}z.2dvaranl_ndfd.grb2 - Four times an hour - "00z", "15z", "30z", "45z"
    ---------------------------------------------------------------------------------------------------------
    Notes: 
    SCHED_INTERVAL should match the cron minute interval
    LOOKBACK_INTERVALS allows for lookback, but failure to download will result in 2 retries 5min apart. 
    """

    URL_ROOT = f'https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod'
    PRODUCT_SLUG = 'ncep-rtma-ru-anl'
    # S3_KEY_DIR = f'cumulus/ncep_rtma_ru_anl'
    SCHED_INTERVAL = 15
    LOOKBACK_INTERVALS = 0

    @task()
    def download_raw_data():

        datetimes = []
        execution_date = get_current_context()['execution_date']
        
        '''
        Because Airflow is always delayed due to the way it processes the previous 
        period, add the current execution time to get the most recent product that
        we can.  Example: an execution for 1545 would run at 1600 (using 15min interval)
        '''
        datetimes.append(execution_date)

        '''
        Add datetimes based on the LOOKBACK_INTERVAL setting above (if any).
        '''
        for lb_interval in range(1, LOOKBACK_INTERVALS+1):
            print((execution_date-timedelta(minutes=lb_interval*SCHED_INTERVAL)))
            datetimes.append((execution_date-timedelta(minutes=lb_interval*SCHED_INTERVAL)))

        for dt in sorted(datetimes):
            file_dir = f'{URL_ROOT}/rtma2p5_ru.{dt.strftime("%Y%m%d")}'
            filename = f'rtma2p5_ru.t{dt.strftime("%H%M")}z.2dvaranl_ndfd.grb2'
            s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}'
            print(f'Downloading {filename}')
            output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='corpsmap-data', s3_key=s3_key)

            return json.dumps({"datetime":dt.isoformat(), "s3_key":s3_key})

    @task()
    def notify_cumulus(payload):
        
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key']
            )

    notify_cumulus(download_raw_data())

airtemp_dag = cumulus_rtma_ru_anl_airtemp()