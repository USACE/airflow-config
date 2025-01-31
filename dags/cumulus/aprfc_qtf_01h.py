"""
Acquire and Process APRFC qtf 01h
"""

import json
from datetime import datetime, timedelta
import calendar
from bs4 import BeautifulSoup
import re
import requests

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=36)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


# ALR qtf filename generator
def get_filenames(edate, url):
    """
    date at end of filename hour and min can not be predicted
    scraping data from website and finding all matching filenames
    for the sprcified date.
    """
    d_t1 = edate.strftime("%Y%m%d")
    d_t2 = (edate - timedelta(hours=24)).strftime("%Y%m%d")

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    links = [node.get("href") for node in soup.find_all("a")]
    filenames = []
    for d in [d_t2, d_t1]:
        regex = r"^ta01f_has_\d+f_\d{8}_\d{2}_awips.*?\.grb(\.gz)?$"
        filenames = filenames + [link for link in links if re.match(regex, link)]

    return filenames


@dag(
    default_args=default_args,
    schedule="40 22,5 * * *",
    tags=["cumulus", "temp", "QTF", "APRFC"],
    max_active_runs=1,
    max_active_tasks=1,
)
def cumulus_aprfc_qtf_01h():
    """This pipeline handles download, processing, and derivative product creation for \n
    APRFC QTF\n
    URL Dir - https://cbt.crohms.org/akgrids
    Files matching ta01f_has_92f_20241219_08_awips_202412150008.grb. - 1 hour\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = f"https://cbt.crohms.org/akgrids"
    PRODUCT_SLUG = "aprfc-qtf-01h"

    @task()
    def download_raw_qtf():
        logical_date = get_current_context()["logical_date"]

        return_list = list()
        filenames = get_filenames(logical_date, URL_ROOT)
        for filename in filenames:
            url = f"{URL_ROOT}/{filename}"
            s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{filename}"
            print(f"Downloading file: {filename}")
            try:
                trigger_download(url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
                return_list.append(
                    {
                        "execution": logical_date.isoformat(),
                        "s3_key": s3_key,
                        "filename": filename,
                    }
                )
            except:
                print(f"{filename} is not available to download")

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

    notify_cumulus(download_raw_qtf())


aprfc_qtf_dag = cumulus_aprfc_qtf_01h()
