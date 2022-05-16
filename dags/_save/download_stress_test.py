"""
Request Downloads from Cumulus service for purposes of testing capabilities/resources.
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow() - timedelta(hours=48)).replace(minute=0, second=0),
    "start_date": datetime(2022, 3, 9),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # "end_date": datetime(2022, 3, 12),
}


@dag(
    default_args=default_args,
    schedule_interval="0 5 * * *",
    tags=["cumulus", "testing", "downloads"],
)
def cumulus_download_stress_test():
    """This dag handles submission of a download request for stress testing purposes. \n"""

    @task()
    def request_download():
        execution_date = get_current_context()["execution_date"]

        # kansas-river - NWDM
        watershed_id = "b31d4708-0f31-4799-9041-b60dba433b18"

        # MRMS-V12 QPE Pass 1 PRECIP 1hr = 30a6d443-80a5-49cc-beb0-5d3a18a84caa
        # MBRFC KRF QPE PRECIP 1hr = 9890d81e-04c5-45cc-b544-e27fde610501
        products = [
            "30a6d443-80a5-49cc-beb0-5d3a18a84caa",
            "9890d81e-04c5-45cc-b544-e27fde610501",
        ]

        # format: 2020-12-01T01:00:00Z
        dt_format = "%Y-%m-%dT%H:00:00Z"
        datetime_start = (execution_date - timedelta(days=14)).strftime(dt_format)
        datetime_end = execution_date.strftime(dt_format)

        r = cumulus.request_download(
            watershed_id, products, datetime_start, datetime_end
        )

        return json.dumps(r.json)

    request_download()


download_dag = cumulus_download_stress_test()
