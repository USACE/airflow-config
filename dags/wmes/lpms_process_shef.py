import io
import requests
import pendulum

from datetime import datetime, timedelta, timezone
from shef import shef_parser
from airflow.models import Variable
from airflow.decorators import dag, task
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
def lpms_process_shef():
    """This pipeline will retrieve data for districts from the LPMS REST API and store it in the CWMS database."""
    # Load district->offices mapping from an Airflow Variable.
    # Expected format (JSON): {"LRD": "LRD", "SAD": ["SAD1","SAD2"]}
    district_offices = Variable.get(
        "LPMS_DISTRICT_OFFICES", default_var="{}", deserialize_json=True
    )

    @task()
    def fetch_lpms_data(district: str, offices: str):
        # LPMS API call is not timezone-aware -- therefore, a lookback of 4 hours
        # in ET will effectively be a lookback of 3 hours in CT when using a
        # request timestamp of "now".
        now_et = pendulum.now("America/New_York").format("YYYYMMDDHHmm")
        lookback_hours = 4

        # LPMS SHEF endpoint per district
        lpms_url = f"http://nav-app1-prod1.cwbi.lan:8080/ords/lpms2/shef/{district}"
        if S3_BUCKET == "wmes-airflow-test":
            lpms_url = f"http://nav-app3-test.cwbi.lan:8080/ords/lpms2/shef/{district}"
        elif S3_BUCKET == "wmes-airflow-dev":
            lpms_url = f"https://ndc-navapps.ops.usace.army.mil/ords/lpms2/shef/{district}"

        request_url = f"{lpms_url}/{now_et}/{lookback_hours}"
        logging.info(f"grabbing file from: {request_url}")
        response = requests.get(request_url)
        response.raise_for_status()
        input = io.StringIO(response.text)
        shef_parser.parse(
            input_stream=input,
            loader_spec=f"cda[{CDA_URL}][{CDA_API_KEY}][{offices}]",
        )

    # Create a single task for each district defined in the Variable
    for district, offices in (district_offices or {}).items():
        # Normalize offices to a comma-separated string for loader_spec
        if isinstance(offices, (list, tuple)):
            offices_str = ",".join([str(o) for o in offices])
        else:
            offices_str = str(offices)

        # Create a uniquely identified task per district
        fetch_lpms_data.override(task_id=f"fetch_lpms_data_{district}")(
            district, offices_str
        )


lpms_process_shef()
