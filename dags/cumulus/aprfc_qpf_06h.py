"""
Acquire and Process APRFC QPF 06h
"""

import json
from datetime import datetime, timedelta, timezone
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
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=36)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}

def get_latest_files(filenames):
    # Dictionary to store the latest file for each unique timestamp
    latest_files = {}
    
    # Regular expression to extract the timestamp
    pattern = r'qpf06f_has_\d+f_(\d{8}_\d{2})_awips_(\d+)'
    
    for filename in filenames:
        match = re.search(pattern, filename)
        if match:
            key = match.group(1) + '_' + match.group(2)
            if key not in latest_files or filename > latest_files[key]:
                latest_files[key] = filename
    
    # Return the list of latest files
    return list(latest_files.values())

# ALR QPF filename generator
def get_filenames(edate, url):
    """
    date at end of filename hour and min can not be predicted
    scraping data from website and finding all matching filenames
    for the sprcified date.
    """
    d_t1 = edate.strftime("%Y%m%d")


    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    links = [node.get("href") for node in soup.find_all("a")]
    filenames = []
    regex = f"^qpf06f_has_\\d+f_\\d{{8}}_\\d{{2}}_awips_{d_t1}.*\\.grb(\\.gz)?$"
    filenames = [link for link in links if re.match(regex, link)]

    return get_latest_files(filenames)


@dag(
    default_args=default_args,
    schedule="20 9,15,19 * * *",
    tags=["cumulus", "precip", "QPF", "APRFC"],
    max_active_runs=1,
    max_active_tasks=1,
)
def cumulus_aprfc_qpf_06h():
    """This pipeline handles download, processing, and derivative product creation for \n
    APRFC QPE\n
    URL Dir - https://cbt.crohms.org/akgrids
    Files matching qpf06f_has_6f_20200917_18_awips_202009170949.grb - 6 hour\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = f"https://cbt.crohms.org/akgrids"
    PRODUCT_SLUG = "aprfc-qpf-06h"

    @task()
    def download_raw_qpf():
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

    notify_cumulus(download_raw_qpf())


aprfc_qpf_dag = cumulus_aprfc_qpf_06h()
