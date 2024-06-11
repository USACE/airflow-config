from airflow.decorators import dag, task

import pendulum
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from airflow.operators.python import get_current_context
from typing import List, Literal

import helpers.cumulus as cumulus

AorcType = Literal["precip", "temp"]
rfc_list = [
    "ABRFC",
    # "APRFC",
    "CBRFC",
    "CNRFC",
    # "LMRFC",
    # "MARFC",
    # "MBRFC",
    # "NCRFC",
    # "NERFC",
    # "NWRFC",
    # "OHRFC",
    # "SERFC",
    # "WGRFC",
]


def get_filenames(base_url: str, rfc_string: str, type: AorcType) -> List[str]:
    """Retrieve all hosted AORC .zip filenames for the given RFC and data type

    Args:
        base_url (str): Base URL of AORC data host, including version
        rfc_string (str): 5-letter river forecast center (RFC) identifier
        type (AorcType): AORC data type (precip or temp)

    Returns:
        List[str]: A list of filenames available for the given RFC and data type
    """
    type_url_str = "precipitation" if type == "precip" else "temperature"
    # TODO: Fix certs in container so that verify=False isn't needed for request
    page = requests.get(f"{base_url}{rfc_string}_4km/{type_url_str}/", verify=False)
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
    tags=["cumulus", "precip", "AORC"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_aorc_precip():
    """This pipeline handles download, processing, and derivative product creation for \n
    Analysis of Record for Calibration (AORC) precipitation data\n
    URL Dir - https://hydrology.nws.noaa.gov/pub/AORC/V1.1/XXRFC_4km/precipitation/
    Files matching AORC_APCP_4KM_XXRFC_YYYYMM.zip - monthly\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = "https://hydrology.nws.noaa.gov/pub/AORC/V1.1/"
    PRODUCT_SLUG = "aorc-precip"

    @task()
    def get_new_rfc_precip_files(rfc: str):
        logical_date = get_current_context()["logical_date"]
        check_date = logical_date.add(months=-6)
        print(f"check_date = {check_date}")
        filenames = get_filenames(URL_ROOT, rfc, "precip")
        for filename in filenames:
            dt_str = filename.split(".")[0][-6:]
            dt = pendulum.from_format(dt_str, "YYYYMM", tz="UTC")
            if dt >= check_date:
                print(filename)

    for rfc in rfc_list:
        get_new_rfc_precip_files.override(task_id=f"get_new_{rfc}_precip_files")(rfc)


aorc_precip_dag = cumulus_aorc_precip()

if __name__ == "__main__":
    from airflow.utils.state import State

    aorc_precip_dag.clear()
    aorc_precip_dag.run()
