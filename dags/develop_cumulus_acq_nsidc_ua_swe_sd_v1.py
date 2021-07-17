"""
Download and Process NSIDC 4 km SWE and Snow Depth
"""

import os, json, logging
from airflow.exceptions import AirflowException, AirflowSkipException
# import requests
# from requests.structures import CaseInsensitiveDict
# import xml.etree.ElementTree as ET
# import socket

# import xml.etree.ElementTree as ET
# import shutil
from datetime import datetime, timedelta
from string import Template
# import tempfile

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import upload_file, s3_file_exists

import helpers.cumulus as cumulus

_method = "develop"

url_root = "https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0719_SWE_Snow_Depth_v1"
file_template = Template("4km_SWE_Depth_WY${WY}_v01.nc")

s3_bucket = f'cwbi-data-{_method}'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow()-timedelta(hours=48)).replace(minute=0, second=0),
    "start_date": datetime(2010, 1, 1),
    "end_date": datetime(2020, 1, 1),
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

@dag(default_args=default_args, schedule_interval='@yearly', tags=['NSIDC', 'snow', 'develop'])
def develop_cumulus_nsidc_ua_swe_sd_v1():
    """
    Download 4 km SWE and Snow Depth netCDF files from NSIDC by water year
    """

    product_slug = "nsidc-ua-swe-sd-v1"

    # def nsidc_token() -> str:
    #     url = "https://cmr.earthdata.nasa.gov/legacy-services/rest/tokens"

    #     hostname = socket.gethostname()
    #     ip = socket.gethostbyname(hostname)
    #     headers = CaseInsensitiveDict()
    #     headers["Content-Type"] = "application/xml"

    #     payload = f"""
    #     <token>
    #     <username>USERNAME</username>
    #     <password>PASSWORD</password>
    #     <client_id>NSIDC_client_id</client_id>
    #     <user_ip_address>{ip}</user_ip_address>
    #     </token>"""

    #     req = requests.post(url, data=payload, headers=headers)
    #     if req.status_code == 201:
    #         root = ET.fromstring(req.text)
    #         for child in root:
    #             if child.tag == "id": return child.text
    #         else:
    #             return None



    # @task()
    # def acquire_file():
    #     execution_date = get_current_context()['execution_date']
    #     dst_filename = file_template.substitute(WY=execution_date.strftime("%Y"))
    #     url = f"{url_root}/{dst_filename}"
    #     s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{dst_filename}"
        
    #     token = cumulus.nsidc_token()
    #     if token is not None:
    #         headers = CaseInsensitiveDict()
    #         headers["Accept"] = "application/json"
    #         headers["Authorization"] = "Bearer {token}"
    #         with requests.get(url, stream=True, headers=headers) as req:
    #             with tempfile.NamedTemporaryFile() as tf:
    #                 print(tf.name)
    #                 shutil.copyfileobj(req.raw, tf)
    #                 upload_file(tf.name, s3_bucket, s3_key)

    #         return {"datetime": execution_date.isoformat(), "s3_key": s3_key}
    #     else:
    #         raise AirflowException

    @task()
    def check_s3_file():
        execution_date = get_current_context()['execution_date']
        filename = file_template.substitute(WY=execution_date.strftime("%Y"))
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"

        print('-'*30)
        print(f'Checking for {s3_bucket}/{s3_key}')
        print('-'*30)

        if not s3_file_exists(bucket=s3_bucket, key=s3_key):
            raise AirflowException

        return {"datetime": execution_date.isoformat(), "s3_key": s3_key}


    @task()
    def notify_cumulus(payload):
        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[product_slug], 
            datetime=payload["datetime"], 
            s3_key=payload["s3_key"],
            conn_type=_method
            )

    notify_cumulus(check_s3_file())

# Execute
nsidc_ua_swe_sd_v1_dag = develop_cumulus_nsidc_ua_swe_sd_v1()