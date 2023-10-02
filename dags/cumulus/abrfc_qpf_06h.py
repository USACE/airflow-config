"""
Acquire and Process ABRFC QPF 6hr
    This pipeline handles download, processing, and derivative product creation for \n
    ABRFC QPE\n
    URL Dir - https://tgftp.nws.noaa.gov/data/rfc/abrfc/xmrg_qpf/
    Files matching QPF6_YYYYMMDDHHf0HH.cdf - 6 hour\n
    Note: Delay observed when watching new product timestamp on file at source.
    Example: timestamp said 15:50, but was pushed to server at 16:07
"""

import json
from datetime import datetime, timedelta
import calendar

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=12)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}


# ALR QPF filename generator
def qpf_filenames(edate):
    hh = edate.hour
    print(hh)
    if 0 >= hh < 12:
        hh = 0
    elif 12 >= hh < 18:
        hh = 12
    else:
        hh = 18
    d = edate.strftime("%Y%m%d")
    # d = "20230926"
    # hh = 12
    for fff in range(6, 72, 6):
        # forecast_date = (edate + timedelta(hours=fff)).strftime("%Y%m%d%H")
        yield f"QPF6_{d}{hh:02d}f{fff:03d}.cdf"


@dag(
    default_args=default_args,
    schedule="8 */3 * * *",
    tags=["cumulus", "precip", "QPF", "ABRFC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_abrfc_qpf_06h():
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    URL_ROOT = "https://tgftp.nws.noaa.gov/data/rfc/abrfc/xmrg_qpf"

    PRODUCT_SLUG = "abrfc-qpf-06h"

    @task()
    def download_raw_qpf():
        logical_date = get_current_context()["logical_date"]

        return_list = list()
        for filename in qpf_filenames(logical_date):
            url = f"{URL_ROOT}/{filename}"
            s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{filename}"
            print(f"Downloading file: {filename}")
            trigger_download(url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
            return_list.append(
                {
                    "execution": logical_date.isoformat(),
                    "s3_key": s3_key,
                    "filename": filename,
                }
            )

        return json.dumps(return_list)

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for item in payload:
            print("Notifying Cumulus: " + item["filename"])
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=item["execution"],
                s3_key=item["s3_key"],
            )

    notify_cumulus(download_raw_qpf())


abrfc_qpf_dag = cumulus_abrfc_qpf_06h()
