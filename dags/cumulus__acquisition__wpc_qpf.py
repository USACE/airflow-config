"""
Acquire and Process Weather Prediction Center QPF
"""

import json

from airflow import DAG
from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 10, 0, 0, 0),
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

# An Example Using the Taskflow API
@dag(default_args=default_args, schedule_interval='0 3 * * *', tags=['cumulus'])
def download_and_process_wpc_qpf():
    """This pipeline handles download, processing, and derivative product creation for Weather Prediction Center QPF
    0000 Hours Forecast : 00f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    0600 Hours Forecast : 06f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    1200 Hours Forecast : 12f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168
    1800 Hours Forecast : 18f  -  006, 012, 018, 024, 030, 036, 042, 048, 054, 060, 066, 072, 078, 084, 090, 096, 102, 108, 114, 120, 126, 132, 138, 144, 150, 156, 162, 168, 174
    """

    def download_raw_data():
        print('TODO...')
