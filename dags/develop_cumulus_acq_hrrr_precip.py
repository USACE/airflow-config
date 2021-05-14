
import json
import requests
import logging
import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow import AirflowException
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import task, get_current_context

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from helpers.downloads import trigger_download
import helpers.cumulus as cumulus

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'retries': 6,
    'retry_delay': timedelta(minutes=10)
}
with DAG(
    'develop_cumulus_hrrr_precip',
    default_args=default_args,
    description='HRRR Forecast Precip',
    # start_date=(datetime.utcnow()-timedelta(hours=72)).replace(minute=0, second=0),
    start_date=(datetime.utcnow()-timedelta(hours=2)).replace(minute=0, second=0),
    tags=['cumulus', 'precip', 'develop', 'forecast'],    
    # schedule_interval='*/15 * * * *'
    schedule_interval='@hourly',
    catchup=False
    
) as dag:
    dag.doc_md = """This pipeline handles download and API notification for HRRR hourly forecast products. \n
    High-Resolution Rapid Refresh (HRRR) \n
    Info: https://rapidrefresh.noaa.gov/hrrr/\n
    Multiple sources:\n
    - https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/\n
    - https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.20210414/conus/\n
    Files matching hrrr.t{HH}z.wrfsfcf{HH}.grib2 - Multiple hourly files (second variable) per forecast file (first variable)
    """

    URL_ROOT = f'https://noaa-hrrr-bdp-pds.s3.amazonaws.com'
    S3_BUCKET = 'cwbi-data-develop'
    PRODUCT_SLUG = 'hrrr-total-precip'
    ##############################################################################
    def download_precip_fcst_hour(hour):

        exec_dt = get_current_context()['execution_date']

        directory = f'hrrr.{exec_dt.strftime("%Y%m%d")}/conus'
        src_filename = f'hrrr.t{exec_dt.strftime("%H")}z.wrfsfcf{str(hour).zfill(2)}.grib2'
        dst_filename = f'hrrr.{exec_dt.strftime("%Y%m%d")}.t{exec_dt.strftime("%H")}z.wrfsfcf{str(hour).zfill(2)}.grib2'
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{dst_filename}'
        print(f'Downloading {src_filename}')
        output = trigger_download(url=f'{URL_ROOT}/{directory}/{src_filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)

        return json.dumps({"datetime":exec_dt.isoformat(), "s3_key":s3_key})
    ##############################################################################
    def notify_api(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
        # print(f'payload is: {payload}')

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key'],
            conn_type='develop'
            )        

        return
    ##############################################################################
    for fcst_hour in range(0, 19):
        
        print(f"Forecast Hour: {fcst_hour}")
        
        download_task_id = f"download_fcst_hr_{str(fcst_hour).zfill(2)}"
        
        download_task = PythonOperator(
            task_id=download_task_id, 
            python_callable=download_precip_fcst_hour, 
            op_kwargs={
                'hour': str(fcst_hour).zfill(2),
            }
        )

        notify_api_task = PythonOperator(
            task_id=f"notify_api_fcst_hr_{str(fcst_hour).zfill(2)}",           
            python_callable=notify_api,
            op_kwargs={               
                'payload': "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(download_task_id)
                }
        )

        download_task >> notify_api_task