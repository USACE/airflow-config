import io
import requests
import pendulum

from datetime import datetime, timedelta, timezone
from shef import shef_parser
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.models import Variable
import logging

CDA_API_KEY = Variable.get("API_KEY")
CDA_URL = Variable.get("CDA_URL")
S3_BUCKET = Variable.get("s3_bucket")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(minutes=15)).replace(
        minute=0, second=0
    ),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule="15 * * * *",
    tags=["wmes", "lpms", "lrl"],
    max_active_runs=1,
    max_active_tasks=1,
)
def lpms_process_shef_lrd():
    """This pipeline will retrieve data for LRD from the LPMS REST API and store it in the CWMS database."""

    @task()
    def fetch_lpms_data():
        # LPMS API call is not timezone-aware -- therefore, a lookback of 4 hours
        # in ET will effectively be a lookback of 3 hours in CT when using a
        # request timestamp of "now".
        now_et = pendulum.now("America/New_York").format("YYYYMMDDHHmm")
        lookback_hours = 4

        # LPMS SHEF LRD endpoint: https://ndc-navapps.ops.usace.army.mil/ords/lpms2/shef/LRD/yyyymmddhhmm/lookbackhours
        lpms_url = "http://nav-app1-prod1.cwbi.lan:8080/ords/lpms2/shef/LRD"
        if S3_BUCKET == "wmes-airflow-test":
            lpms_url = "http://nav-app3-test.cwbi.us:8080/ords/lpms2/shef/LRD/"
        elif S3_BUCKET == "wmes-airflow-dev":
            lpms_url = "https://ndc-navapps.ops.usace.army.mil/ords/lpms2/shef/LRD/"
        request_url = f"{lpms_url}/{now_et}/{lookback_hours}"
        logging.info(f"grabbing file from: {request_url}")
        response = requests.get(request_url)
        response.raise_for_status()
        input = io.StringIO(response.text)
        shef_parser.parse(
            input_stream=input,
            loader_spec=f"cda[{CDA_URL}][{CDA_API_KEY}]",
        )

    fetch_lpms_data()


lpms_process_shef_lrd()
