"""
Acquire and Process Weather Prediction Center QPF
"""

import os, json, logging
import requests
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import dag, task

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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# An Example Using the Taskflow API
@dag(default_args=default_args, schedule_interval='@hourly', tags=['cumulus','forecast'])
def cumulus_wpc_qpf():
    """This pipeline handles download, processing, and derivative product creation for Weather Prediction Center QPF\n
    0000 Hours Forecast : 00f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    0600 Hours Forecast : 06f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    1200 Hours Forecast : 12f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    1800 Hours Forecast : 18f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    """

    URL_ROOT = f'https://ftp.wpc.ncep.noaa.gov'
    PRODUCT_SLUG = 'wpc-qpf-2p5km'
    S3_BUCKET = 'cwbi-data-stable'
    
    STATUS_SOURCE = f'{URL_ROOT}/pqpf/pqpf_status.txt'
    # S3_KEY_DIR = f'cumulus/wpc_qpf_2p5km'
    STATUS_S3_KEY = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}_status/{os.path.basename(STATUS_SOURCE)}'
    PROD_SOURCE_DIR = '2p5km_qpf'

    @task()
    def check_new_forecast():
        
        s3_forecast_datetime = None
        # Check S3 status file for last datetime string saved from prev request
        s3_status_contents = read_s3_file(STATUS_S3_KEY, S3_BUCKET)

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

    
    def notify_cumulus(payload):        
        
        print(f'Notify Cumulus was called with a payload of: {payload}')
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[payload['product_slug']], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key'],
            conn_type='stable'
            ) 
    
    
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
            print('#'*60)
            filename = f'p06m_{forecast_datetime}f{hour}.grb'
            s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}'
            output = trigger_download(url=f'{URL_ROOT}/{PROD_SOURCE_DIR}/{filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)
            notify_cumulus(json.dumps({"datetime":datetime.strptime(forecast_datetime, '%Y%m%d%H').replace(tzinfo=timezone.utc).isoformat(), "s3_key":s3_key, "product_slug":PRODUCT_SLUG}))
      
        # Replace the status file with new datetime contents
        output = trigger_download(url=f'{STATUS_SOURCE}', s3_bucket=S3_BUCKET, s3_key=STATUS_S3_KEY)
        return

    
    download_raw_data(check_new_forecast())

wpc_qpf_dag = cumulus_wpc_qpf()