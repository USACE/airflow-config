import json
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

from helpers.radar import api_request
from helpers.sharedApi import get_static_offices, get_nwd_group
import helpers.water as water

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=6)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    tags=["a2w"],
    schedule_interval="0 14 * * *",
    max_active_runs=2,
    max_active_tasks=4,
    catchup=False,
    description="Extract Project Timeseries Paths from RADAR, Post to Water API",
)
def a2w_sync_timeseries_paths():
    """Comments here"""

    def get_office_id_by_symbol(symbol: str, offices: list):
        for o in offices:
            if o["symbol"].upper() == symbol.upper():
                return o["id"]
        return None

    @task
    def get_cwms_offices():
        offices = water.get_offices()
        # print(offices)
        return json.dumps(offices)

    offices = get_cwms_offices()

    for office in get_static_offices():

        @task(task_id=f"extract_and_load_{office}_tsids")
        def extract_and_load_tsids(office: str, offices: list):
            offices = json.loads(offices)
            print(offices)

            if get_nwd_group(office.upper()) in ["NWDM", "NWDP"]:
                # do some nonsense here
                raise AirflowSkipException
                # return

            # If not NWDP or NWDM offices
            else:

                tsids = json.loads(api_request("timeseries", f"name=@&office={office}"))

                ts_obj_list = tsids["time-series-catalog"]["time-series"]

                if len(ts_obj_list) == 0:
                    raise ValueError(f"RADAR payload for {office} has no tsids")

                a2w_payload = []
                for ts_obj in ts_obj_list:
                    if ts_obj["office"].upper() == office.upper():
                        payload = {}
                        payload["key"] = ts_obj["name"]
                        payload["datasource_type"] = "cwms-timeseries"
                        payload["provider"] = ts_obj["office"].upper()
                        print(payload)
                        a2w_payload.append(payload)

                water.post_cwms_timeseries(a2w_payload)

                return

        extract_and_load_tsids(office, offices)

    # _ = [create_task_group(office=office) for office in get_static_offices()]


timeseries_dag = a2w_sync_timeseries_paths()
