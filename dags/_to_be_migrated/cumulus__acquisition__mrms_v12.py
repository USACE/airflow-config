"""
ACQUIRE MRMS V12 Raw Data and Save to S3;
"""

import os, json, logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow()-timedelta(hours=24)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 4, 1),
    "catchup_by_default": False,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

@dag(default_args=default_args, schedule_interval='0 * * * *', tags=['cumulus', 'precip'])
def cumulus_mrms_v12_qpe_pass1_pass2():
    """This pipeline handles download, processing, and derivative product creation for \n
    MultiRadar MultiSensor_QPE_01H Pass1 and Pass2\n
    URL Dir - https://mrms.ncep.noaa.gov/data/2D\n
    Files matching MRMS_MultiSensor_QPE_01H_Pass1_00.00_YYYYMMDD-HH0000.grib2.gz (Hourly data)
    """

    URL_ROOT = f'https://mrms.ncep.noaa.gov/data/2D'
    S3_BUCKET = 'cwbi-data-stable'

    # Download Tasks
    #################################################
    @task()
    def download_mrms_v12_qpe_pass1():
        product_slug = 'ncep-mrms-v12-multisensor-qpe-01h-pass1'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/MultiSensor_QPE_01H_Pass1'        
        # filename = f'PRISM_tmin_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        filename = f'MRMS_MultiSensor_QPE_01H_Pass1_00.00_{execution_date.strftime("%Y%m%d-%H0000")}.grib2.gz'        
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key, "product_slug":product_slug})


    @task()
    def download_mrms_v12_qpe_pass2():
        product_slug = 'ncep-mrms-v12-multisensor-qpe-01h-pass2'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/MultiSensor_QPE_01H_Pass2'        
        # filename = f'PRISM_tmin_early_4kmD2_{execution_date.strftime("%Y%m%d")}_bil.zip'
        filename = f'MRMS_MultiSensor_QPE_01H_Pass2_00.00_{execution_date.strftime("%Y%m%d-%H0000")}.grib2.gz'        
        s3_key = f'{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket=S3_BUCKET, s3_key=s3_key)

        return json.dumps({"datetime":execution_date.isoformat(), "s3_key":s3_key, "product_slug":product_slug})

    # Notify Tasks
    #################################################
    @task()
    def notify_cumulus(payload):        
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
    
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[payload['product_slug']], 
            datetime=payload['datetime'], 
            s3_key=payload['s3_key'],
            conn_type='stable'
            )

    notify_cumulus(download_mrms_v12_qpe_pass1())
    notify_cumulus(download_mrms_v12_qpe_pass2())

mrms_v12_qpe_dag = cumulus_mrms_v12_qpe_pass1_pass2()
