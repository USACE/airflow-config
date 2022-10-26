import json
from datetime import datetime, timedelta
import logging
from socket import timeout
from uuid import uuid4

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import _LazyXComAccess

import helpers.radar as radar
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
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    tags=["a2w", "radar"],
    schedule_interval="20 * * * *",
    max_active_runs=1,
    max_active_tasks=8,
    catchup=False,
    description="Extract Project Timeseries Measurments from RADAR, Post to Water API",
)
def a2w_sync_timeseries_measurements():
    def create_task_group(**kwargs):
        office = kwargs["office"]

        with TaskGroup(group_id=f"{office}") as task_group:

            @task(task_id=f"extract_{office}_a2w_timeseries")
            def extract_a2w_office_timeseries(office: str):

                office_timeseries = water.get_cwms_timeseries(
                    provider=office, datasource_type="cwms-timeseries", mapped=1
                )
                # if len(office_timeseries) == 0:
                #     raise AirflowSkipException(f"No records found for {office}")

                # Convert list of objects to simple list of tsids
                tsids = []
                for tsobj in office_timeseries:
                    tsids.append(tsobj["key"])

                if len(tsids) == 0:
                    raise AirflowSkipException(
                        f"No timeseries measurements retrieved from a2w"
                    )

                return tsids

            # -------------------------------------------

            @task(task_id=f"extract_{office}_radar_timeseries")
            def extract_radar_timeseries(tsid):

                logging.info(f"Getting values for tsid -->  {tsid}")

                # Define the extract time-windows based on the task datetime
                logical_date = get_current_context()["logical_date"]
                begin = (logical_date - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
                end = (logical_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")

                # Special case for NWDP, NWDM
                radar_office = office
                if get_nwd_group(office) is not None:
                    radar_office = get_nwd_group(office)

                r = radar.get_timeseries([tsid], begin, end, radar_office)
                if r == None:
                    raise ValueError(f"Invalid Response: {r}")

                r = json.loads(r)

                # Grab the time-series list object which can be iterated over
                # when multiple tsids are requested
                ts_obj_list = r["time-series"]["time-series"]

                tsid_results = []

                # it may be possible for the extract task to return an empty
                # list if the tsid was not valid (not found in RADAR).
                # Ensure list is not empty before trying to extract items
                if len(ts_obj_list) == 0:
                    logging.warning(f"No data returned from RADAR for {tsid}.")
                    raise AirflowSkipException
                    # return location_results

                for ts_obj in ts_obj_list:

                    loc_payload = {}
                    # loc_payload["provider"] = ts_obj["office"].lower()
                    loc_payload["provider"] = office
                    loc_payload["datasource_type"] = "cwms-timeseries"
                    loc_payload["key"] = ts_obj["name"]
                    try:
                        x = ts_obj["regular-interval-values"]["segments"][0]
                        # Note: "values" [[value, quality_code]]
                        loc_payload["measurements"] = {
                            "times": [x["last-time"]],
                            "values": [x["values"][x["value-count"] - 1][0]],
                        }

                    except:
                        x = ts_obj["irregular-interval-values"]["values"]
                        # Note: "values" [["time", value, quality_code]]
                        loc_payload["measurements"] = {
                            "times": [x[len(x) - 1][0]],
                            "values": [x[len(x) - 1][1]],
                        }

                    tsid_results.append(loc_payload)

                return tsid_results

            # -------------------------------------------

            @task(
                task_id=f"load_{office}_timeseries_measurements_into_a2w",
                trigger_rule="all_done",
            )
            def load_timeseries_measurements_into_a2w(
                location_timeseries_list: _LazyXComAccess,
            ):

                payload = []
                # This should be a list of lists.
                # Each outer list represents a base location search results
                # Inner list is all timeseries found for that base location
                for loc_ts_list in location_timeseries_list:
                    # print(loc_ts)
                    # Loop over the list of timeseries related to a given base location
                    for ts_obj in loc_ts_list:
                        print(ts_obj)
                        payload.append(ts_obj)

                if len(payload) > 0:
                    print(f"Posting {len(payload)} timeseries measurment objects")
                    water.post_cwms_timeseries_measurements(payload)
                else:
                    raise AirflowSkipException(f"No timeseries measurements to post")

            # -------------------------------------------

            # extract_radar_timeseries = Dynamic Task Mapping - each tsid run through extract function
            load_timeseries_measurements_into_a2w(
                extract_radar_timeseries.expand(
                    tsid=extract_a2w_office_timeseries(office),
                )
            )

            return task_group

    task_groups = [create_task_group(office=office) for office in get_static_offices()]
    # task_groups = [
    #     create_task_group(office=office) for office in ["LRH", "LRP", "NWO", "NWP"]
    # ]


timeseries_measurements_dag = a2w_sync_timeseries_measurements()
