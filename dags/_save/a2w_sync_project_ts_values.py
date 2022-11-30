import json
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

from helpers.radar import get_timeseries as get_radar_timeseries
from helpers.water import get_cwms_timeseries as get_a2w_cwms_timeseries
from helpers.water import post_cwms_timeseries_measurements
from helpers.sharedApi import get_static_offices

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
    schedule="8 * * * *",
    max_active_runs=2,
    max_active_tasks=4,
    catchup=False,
    description="Extract Project Timeseries for Project Charts",
)
def a2w_sync_project_ts():
    """Comments here"""

    @task
    def get_a2w_config(office):

        r = get_a2w_cwms_timeseries(provider=office, datasource_type="cwms-timeseries")
        if len(r) == 0:
            raise AirflowSkipException

        return json.dumps(r)

    def create_task_group(**kwargs):
        office = kwargs["office"]

        with TaskGroup(group_id=f"{office}") as task_group:

            # Simplify the config payload to tsids only for
            # querying against RADAR
            @task(task_id=f"prep_{office}_tsids")
            def prep_tsids(office, config):
                tsids = []
                data = json.loads(config)
                for obj in data:
                    if obj["provider"].lower() == office.lower():
                        tsids.append(obj["key"])
                return tsids

            # Extract a single timeseries from RADAR
            # Load/POST that single timeseries data back to Water API
            # Note: This was done due to resource limits on RADAR
            @task(task_id=f"extract_and_load{office}")
            def extract_and_load(tsid):

                if tsid is None:
                    logging.warning("tsid is None")
                    raise AirflowSkipException

                # Define the extract time-windows based on the task datetime
                logical_date = get_current_context()["logical_date"]
                begin = logical_date.strftime("%Y-%m-%dT%H:%M")
                end = (logical_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")

                # begin = "2022-09-06T14:00"
                # end = "2022-09-06T16:00"
                r = get_radar_timeseries([tsid], begin, end, office)
                r = json.loads(r)
                # print(r)

                # Grab the time-series list object which can be iterated over
                # when multiple tsids are requested
                ts_obj_list = r["time-series"]["time-series"]

                # it may be possible for the extract task to return an empty
                # list if the tsid was not valid (not found in RADAR).
                # Ensure erray is not empty before trying to extract items
                if len(ts_obj_list) == 0:
                    logging.warning(
                        "No data returned from RADAR.  Skipping POST to Water API"
                    )
                    raise AirflowSkipException

                for ts_obj in ts_obj_list:

                    a2w_payload = {}
                    a2w_payload["provider"] = ts_obj["office"].lower()
                    a2w_payload["datasource_type"] = "cwms-timeseries"
                    a2w_payload["key"] = ts_obj["name"]
                    x = ts_obj["regular-interval-values"]["segments"][0]
                    a2w_payload["measurements"] = {
                        "times": [x["last-time"]],
                        "values": [x["values"][x["value-count"] - 1][0]],
                    }

                # Post to the A2W API
                post_cwms_timeseries_measurements([a2w_payload])

                return

            # Dynamic Task Mapping
            extract_and_load.expand(
                tsid=prep_tsids(office=office, config=get_a2w_config((office)))
            )

            return task_group

    _ = [create_task_group(office=office) for office in get_static_offices()]


project_ts_dag = a2w_sync_project_ts()
