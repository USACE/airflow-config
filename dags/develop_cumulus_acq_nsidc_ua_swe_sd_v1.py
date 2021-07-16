"""
Download and Process NSIDC 4 km SWE and Snow Depth
"""

import os, json, logging
from airflow.exceptions import AirflowException
import requests
from requests.structures import CaseInsensitiveDict

import xml.etree.ElementTree as ET
import shutil
from datetime import datetime, timedelta
from string import Template
import tempfile

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import upload_file

import helpers.cumulus as cumulus

_method = "develop"

url_root = "https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0719_SWE_Snow_Depth_v1"
file_template = Template("4km_SWE_Depth_WY${WY}_v01.nc")

s3_bucket = f'cwbi-data-{_method}'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow()-timedelta(hours=48)).replace(minute=0, second=0),
    "start_date": datetime(2018, 1, 1),
    "end_date": datetime.now(),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

@dag(default_args=default_args, schedule_interval='@yearly', tags=["NSIDC",])
def nsidc_ua_swe_sd_v1():
    """
    Download 4 km SWE and Snow Depth netCDF files from NSIDC by water year
    """

    product_slug = "nsidc_ua_swe_sd_v1"

    @task()
    def acquire_file():
        execution_date = get_current_context()['execution_date']
        dst_filename = file_template.substitute(WY=execution_date.strftime("%Y"))
        url = f"{url_root}/{dst_filename}"
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{dst_filename}"
        
        token = cumulus.nsidc_token()
        if token is not None:
            headers = CaseInsensitiveDict()
            headers["Accept"] = "application/json"
            headers["Authorization"] = "Bearer {token}"
            with requests.get(url, stream=True, headers=headers) as req:
                with tempfile.NamedTemporaryFile() as tf:
                    print(tf.name)
                    shutil.copyfileobj(req.raw, tf)
                    upload_file(tf.name, s3_bucket, s3_key)

            return {"datetime": execution_date.isoformat(), "s3_key": s3_key}
        else:
            raise AirflowException

    @task()
    def notify_cumulus(payload):
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[product_slug], 
            datetime=payload["datetime"], 
            s3_key=payload["s3_key"],
            conn_type=_method
            )

    notify_cumulus(acquire_file())

# Execute
nsidc_ua_swe_sd_v1_dag = nsidc_ua_swe_sd_v1()