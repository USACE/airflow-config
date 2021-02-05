"""
ACQUIRE MRMS V12 Raw Data and Save to S3;
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from helpers.downloads import trigger_download

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 9, 18, 0, 0),
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


# MRMS VERSION 12
MRMS_ROOT_URL = 'https://mrms.ncep.noaa.gov/data/2D'
MRMS_QPE_PASS1_FILENAME = 'MRMS_MultiSensor_QPE_01H_Pass1_00.00_{{ execution_date.strftime("%Y%m%d-%H0000") }}.grib2.gz'
MRMS_QPE_PASS2_FILENAME = 'MRMS_MultiSensor_QPE_01H_Pass2_00.00_{{ (execution_date - macros.timedelta(hours=1)).strftime("%Y%m%d-%H0000") }}.grib2.gz'

dag_mrms_v12 = DAG(
    'cumulus_download_mrms_v12',
    tags=['cumulus'],
    default_args=default_args,
    schedule_interval='0 * * * *'
)

mrms_v12_qpe_pass1 = PythonOperator(
    task_id='download_mrms_v12_qpe_pass1',
    python_callable=trigger_download,
    op_kwargs={
        'url': MRMS_ROOT_URL + '/MultiSensor_QPE_01H_Pass1/' + MRMS_QPE_PASS1_FILENAME,
        's3_bucket': 'corpsmap-data-incoming',
        's3_key': 'cumulus/ncep_mrms_v12_MultiSensor_QPE_01H_Pass1/' + MRMS_QPE_PASS1_FILENAME,
    },
    dag=dag_mrms_v12
)

mrms_v12_qpe_pass2 = PythonOperator(
    task_id='download_mrms_v12_qpe_pass2',
    python_callable=trigger_download,
    op_kwargs={
        'url': MRMS_ROOT_URL + '/MultiSensor_QPE_01H_Pass2/' + MRMS_QPE_PASS2_FILENAME,
        's3_bucket': 'corpsmap-data-incoming',
        's3_key': 'cumulus/ncep_mrms_v12_MultiSensor_QPE_01H_Pass2/' + MRMS_QPE_PASS2_FILENAME,
    },
    dag=dag_mrms_v12
)
