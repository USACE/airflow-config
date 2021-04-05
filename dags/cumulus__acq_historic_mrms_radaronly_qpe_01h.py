"""
ACQUIRE Historic MRMS RadarOnly_QPE_01H and Save to S3;
"""
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, task, get_current_context
from datetime import datetime, timedelta

from helpers.downloads import trigger_download

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2014, 11, 1),
    # "start_date": (datetime.utcnow()-timedelta(hours=36)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 4, 1),
    'end_date': datetime(2015, 11, 1),
}

@dag(default_args=default_args, schedule_interval='0 * * * *', tags=['cumulus', 'historic', 'precip'])
def download_and_process_historic_mrms_qpe_1hr():
    """This pipeline handles download and processing for \n
    URL Dir - https://mtarchive.geol.iastate.edu/2021/04/01/mrms/ncep/RadarOnly_QPE_01H/
    Files matching RadarOnly_QPE_01H_00.00_YYYMMDD-000000.grib2.gz	
    """

    URL_ROOT = f'https://mtarchive.geol.iastate.edu'

    @task()
    def download_raw_radaronly_qpe_1hr():
        s3_key_dir = f'cumulus/ncep_mrms_radaronly_qpe_01h'
        execution_date = get_current_context()['execution_date']
        file_dir = f'{URL_ROOT}/{execution_date.strftime("%Y")}/{execution_date.strftime("%m")}/{execution_date.strftime("%d")}/mrms/ncep/RadarOnly_QPE_01H'
        filename = f'RadarOnly_QPE_01H_00.00_{execution_date.strftime("%Y%m%d-%H%M")}00.grib2.gz'
        print(f'Downloading {filename}')
        output = trigger_download(url=f'{file_dir}/{filename}', s3_bucket='corpsmap-data-incoming', s3_key=f'{s3_key_dir}/{filename}')

    download_raw_radaronly_qpe_1hr()

qpe_1hr_dag = download_and_process_historic_mrms_qpe_1hr()