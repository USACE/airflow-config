"""
Acquire and Process Weather Prediction Center QPF
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
    "start_date": (datetime.utcnow()-timedelta(days=6)).replace(hour=0, minute=0, second=0),
    "end_date": (datetime.utcnow()-timedelta(days=1)).replace(hour=23, minute=59, second=59),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

# An Example Using the Taskflow API
@dag(default_args=default_args, schedule_interval='0 00,06,12,18 * * *', tags=['cumulus','forecast', 'backfill'])
def download_and_process_wpc_qpf_backfill():
    """This pipeline handles backfill download, processing, and derivative product creation for Weather Prediction Center QPF\n
    0000 Hours Forecast : 00f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    0600 Hours Forecast : 06f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    1200 Hours Forecast : 12f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    1800 Hours Forecast : 18f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174

    The WPC Archive appears to only go back 5-6 days.
    Backfill for WPC QPF Products will use time range: Start:now-6days, End:now-1day
    """

    URL_ROOT = f'https://ftp.wpc.ncep.noaa.gov'
    #STATUS_SOURCE = f'{URL_ROOT}/pqpf/pqpf_status.txt'
    #STATUS_S3_KEY = f'cumulus/wpc_2p5km_qpf_status/{os.path.basename(STATUS_SOURCE)}'
    PROD_SOURCE_DIR = '2p5km_qpf'


    def generate_str_numbers(start, end, interval, char_len):
        str_numbers = []
        for h in range(start, end+1, interval):
            # print(str(h).zfill(char_len))
            str_numbers.append(str(h).zfill(char_len))
        return str_numbers 
    
    
    @task()
    def download_raw_data():
                
        execution_date = get_current_context()['execution_date']
        forecast_datetime = execution_date.strftime("%Y%m%d%H")
        logging.info(f'Downloading for Forecast Set: {forecast_datetime}')

        fcst_hrs = {
            '00': generate_str_numbers(start=6, end=168, interval=6, char_len=3),
            '06': generate_str_numbers(start=6, end=174, interval=6, char_len=3),
            '12': generate_str_numbers(start=6, end=168, interval=6, char_len=3),
            '18': generate_str_numbers(start=6, end=174, interval=6, char_len=3),
        }

        forecast_hour = forecast_datetime[-2:]
        logging.info(f'Forecast hour is: {forecast_hour}')

        for hour in fcst_hrs[forecast_hour]:
            filename = f'p06m_{forecast_datetime}f{hour}.grb'
            output = trigger_download(url=f'{URL_ROOT}/{PROD_SOURCE_DIR}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'cumulus/wpc_{PROD_SOURCE_DIR}/{filename}')
      
        return

    
    download_raw_data()

wpc_qpf_dag = download_and_process_wpc_qpf_backfill()