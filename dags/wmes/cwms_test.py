"""
## CWMS Test
    
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from CWMS import CWMS

# from helpers.downloads import trigger_download

# import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=24)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule="15 * * * *",
    tags=["WMES"],
    max_active_runs=2,
    max_active_tasks=4,
    doc_md=__doc__,
)
def wmes_cda_test():

    API_ROOT = f"https://cwms-data.usace.army.mil/cwms-data"

    @task()
    def download_timeseries():
        logical_date = get_current_context()["logical_date"]

        cwms = CWMS()
        cwms.connect(API_ROOT)

        end = logical_date
        start = end - timedelta(days=10)
        ts_df = cwms.retrieve_ts(
            p_office_id="LRH",
            p_tsId="Bluestone-Lake.Elev.Inst.15Minutes.0.OBS",
            p_start_date=start,
            p_end_date=end,
        )
        ts_df.head()

        # print(f"Downloading {filepath}")

        # trigger_download(
        #     url=f"{URL_ROOT}/{filepath}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        # )

        return json.dumps(
            {"datetime": logical_date.isoformat(), "data": ts_df},
        )
        return

    @task()
    def do_something(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        print(payload)

        # cumulus.notify_acquirablefile(
        #     acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
        #     datetime=payload["datetime"],
        #     s3_key=payload["s3_key"],
        # )

    do_something(download_timeseries())


DAG_ = wmes_cda_test()
