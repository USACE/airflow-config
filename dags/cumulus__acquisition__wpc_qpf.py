"""
Acquire and Process Weather Prediction Center QPF
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task

from helpers.downloads import trigger_download, read_s3_file

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow()-timedelta(hours=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
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
@dag(default_args=default_args, schedule_interval='@hourly', tags=['cumulus'])
def download_and_process_wpc_qpf():
    """This pipeline handles download, processing, and derivative product creation for Weather Prediction Center QPF\n
    0000 Hours Forecast : 00f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    0600 Hours Forecast : 06f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    1200 Hours Forecast : 12f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    1800 Hours Forecast : 18f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    """

    URL_ROOT = f'https://ftp.wpc.ncep.noaa.gov'
    STATUS_SOURCE = f'{URL_ROOT}/pqpf/pqpf_status.txt'
    STATUS_S3_KEY = f'cumulus/wpc_2p5km_qpf_status/{os.path.basename(STATUS_SOURCE)}'
    PROD_SOURCE_DIR = '2p5km_qpf'

    @task()
    def check_new_forecast():
        
        s3_forecast_datetime = None
        # Check S3 status file for last datetime string saved from prev request
        s3_status_contents = read_s3_file(STATUS_S3_KEY, 'corpsmap-data-incoming')

        if s3_status_contents:
            s3_forecast_datetime = s3_status_contents[0].strip()
            logging.info(f'S3 Forecast datetime is: {s3_forecast_datetime}')
        else:
            logging.warning('Status file not found in S3.')
            # S3 forecast has no content (no file) which should only happen on
            # first run or if somewhere deletes the file manually.
        
        # Get the remote status file from WPC        
        r = requests.get(f'{STATUS_SOURCE}')
        wpc_forecast_datetime = r.text.strip()
        logging.info(f'WPC Forecast datetime is: {wpc_forecast_datetime}')

        if s3_forecast_datetime == wpc_forecast_datetime:
            return False
        else:
            return wpc_forecast_datetime


    def generate_str_numbers(start, end, interval, char_len):
        str_numbers = []
        for h in range(start, end+1, interval):
            # print(str(h).zfill(char_len))
            str_numbers.append(str(h).zfill(char_len))
        return str_numbers 
    
    
    @task()
    def download_raw_data(forecast_datetime):
        # print(f'download_raw_data(): {forecast_datetime}')
        # print(type(forecast_datetime))

        # Exit early, nothing new to download
        if forecast_datetime == False:
            logging.info(f'Exiting early, nothing new to download.')
            return

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
      
        # Replace the status file with new datetime contents
        output = trigger_download(url=f'{STATUS_SOURCE}', s3_bucket='corpsmap-data-incoming', s3_key=STATUS_S3_KEY)
        return

    
    download_raw_data(check_new_forecast())

wpc_qpf_dag = download_and_process_wpc_qpf()