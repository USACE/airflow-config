from airflow.decorators import dag, task

import pendulum
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from airflow.operators.python import get_current_context
from typing import List, Literal

import helpers.cumulus as cumulus
import helpers.downloads as downloads

import subprocess
import certifi

AorcType = Literal["precip", "temp"]


def get_aorc_url(base_url: str, rfc: str, type: AorcType) -> str:
    type_url_str = "precipitation" if type == "precip" else "temperature"
    return f"{base_url}{rfc}_4km/{type_url_str}/"


def get_filenames(base_url: str, rfc: str, type: AorcType) -> List[str]:
    """Retrieve all hosted AORC .zip filenames for the given RFC and data type

    Args:
        base_url (str): Base URL of AORC data host, including version
        rfc (str): 5-letter river forecast center (RFC) identifier
        type (AorcType): AORC data type (precip or temp)

    Returns:
        List[str]: A list of filenames available for the given RFC and data type
    """
    # TODO: Fix certs in container so that verify=False isn't needed for request
    page = requests.get(get_aorc_url(base_url, rfc, type))
    soup = BeautifulSoup(page.content, "html.parser")
    links = [node.get("href") for node in soup.find_all("a")]
    filenames = []
    regex = "AORC_APCP_4KM_\w{2}RFC_\d{6}.zip"
    filenames = filenames + [link for link in links if re.match(regex, link)]

    return filenames


# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    default_args=default_args,
    schedule="@daily",
    tags=["cumulus", "precip", "AORC", "OHRFC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_aorc_precip_ohrfc():
    """This pipeline handles download, processing, and derivative product creation for \n
    Analysis of Record for Calibration (AORC) precipitation data\n
    URL Dir - https://hydrology.nws.noaa.gov/pub/AORC/V1.1/XXRFC_4km/precipitation/
    Files matching AORC_APCP_4KM_XXRFC_YYYYMM.zip - monthly\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = "https://hydrology.nws.noaa.gov/pub/AORC/V1.1/"
    PRODUCT_SLUG = "aorc_precip"

    def get_hosted_rfc_precip_files(rfc: str):
        logical_date = get_current_context()["logical_date"]
        check_date = logical_date.add(months=-6)
        print(f"check_date = {check_date}")
        filenames = get_filenames(URL_ROOT, rfc, "precip")
        new_files = []
        for filename in filenames:
            dt_str = filename.split(".")[0][-6:]
            dt = pendulum.from_format(dt_str, "YYYYMM", tz="UTC")
            if dt >= check_date:
                new_files.append(filename)
        return new_files

    @task()
    def download_new_rfc_precip_files(rfc: str):
        rfc_prefix = f"{key_prefix}/{PRODUCT_SLUG}/{rfc}"
        hosted_files = get_hosted_rfc_precip_files(rfc)
        stored_files = downloads.s3_list_keys(cumulus.S3_BUCKET, rfc_prefix)
        new_files = [file for file in hosted_files if file not in stored_files]
        zip_keys = []
        for file in new_files:
            url = get_aorc_url(URL_ROOT, rfc, "precip") + file
            s3_key = f"{rfc_prefix}/{file}"
            downloads.trigger_download(
                url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
            )
            zip_keys.append(s3_key)
        return zip_keys

    download_new_rfc_precip_files("OHRFC")


aorc_precip_dag = cumulus_aorc_precip_ohrfc()

if __name__ == "__main__":
    from airflow.utils.state import State

    aorc_precip_dag.clear()
    aorc_precip_dag.run()
