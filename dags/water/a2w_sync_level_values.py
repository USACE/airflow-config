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
    "start_date": (datetime.utcnow() - timedelta(hours=24)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    default_args=default_args,
    tags=["a2w", "radar"],
    schedule_interval="0 10,20 * * *",
    max_active_runs=1,
    max_active_tasks=8,
    catchup=False,
    description="Extract Level Values from RADAR, Post to Water API",
)
def a2w_sync_level_values():
    def create_task_group(**kwargs):
        office = kwargs["office"]

        with TaskGroup(group_id=f"{office}") as task_group:

            @task(task_id=f"extract_{office}_a2w_levels")
            def extract_a2w_office_levels(office: str):

                # Caution: If you set mapped=0, you will get more than 1024 items
                # for some offices.  The initial default max limit for dynamic
                # mapped tasks is 1024.  Going over will cause the task to fail.
                office_levels = water.get_cwms_timeseries(
                    provider=office, datasource_type="cwms-levels", mapped=1
                )

                # If the office has less than 2 mapped tasks, try to get the values
                # for all Elevation levels to have them ready for chart display.
                if len(office_levels) < 2:
                    office_elev_levels = water.get_cwms_timeseries(
                        provider=office,
                        datasource_type="cwms-levels",
                        mapped=0,
                        query="elev",
                    )
                    office_levels.extend(office_elev_levels)

                # if len(office_timeseries) == 0:
                #     raise AirflowSkipException(f"No records found for {office}")

                # Convert list of objects to simple list of tsids
                levels = []
                for lvlobj in office_levels:
                    levels.append(lvlobj["key"])

                if len(levels) == 0:
                    raise AirflowSkipException(f"No level values retrieved from a2w")

                return levels

            # -------------------------------------------

            @task(task_id=f"extract_{office}_radar_levels")
            def extract_radar_levels(level_path):

                logging.info(f"Getting values for level -->  {level_path}")

                # Define the extract time-windows based on the task datetime
                # logical_date = get_current_context()["logical_date"]
                # begin = (logical_date - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
                # end = (logical_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")

                # Special case for NWDP, NWDM
                radar_office = office
                if get_nwd_group(office) is not None:
                    radar_office = get_nwd_group(office)

                r = radar.get_levels([level_path], radar_office)
                if r == None:
                    raise ValueError(f"Invalid Response: {r}")

                r = json.loads(r)

                # Grab the levels list object which can be iterated over
                # when multiple tsids are requested
                lvl_obj_list = r["location-levels"]["location-levels"]

                level_results = []

                # it may be possible for the extract task to return an empty
                # list if the level_path was not valid (not found in RADAR).
                # Ensure list is not empty before trying to extract items
                if len(lvl_obj_list) == 0:
                    logging.warning(f"No data returned from RADAR for {level_path}.")
                    raise AirflowSkipException
                    # return location_results

                for lvl_obj in lvl_obj_list:

                    # Catch odd cases where the RADAR results may have more than one
                    # level path.  Example: two paths with same name, but one has all upper case.
                    if level_path == lvl_obj["name"]:

                        loc_payload = {}
                        # loc_payload["provider"] = ts_obj["office"].lower()
                        loc_payload["provider"] = office
                        loc_payload["datasource_type"] = "cwms-levels"
                        loc_payload["key"] = lvl_obj["name"]
                        try:
                            x = lvl_obj["values"]["segments"][0]
                            # Note: "values" [[time, value]]
                            loc_payload["measurements"] = {
                                "times": [x["values"][-1][0]],
                                "values": [x["values"][-1][1]],
                            }

                        except:

                            raise ValueError(
                                f"Unable to find the values object: {lvl_obj_list}"
                            )

                        level_results.append(loc_payload)

                return level_results

            # -------------------------------------------

            @task(
                task_id=f"load_{office}_level_values_into_a2w",
                trigger_rule="all_done",
                priority_weight=2,
            )
            def load_level_values_into_a2w(
                location_levels_list: _LazyXComAccess,
            ):

                payload = []
                # This should be a list of lists.
                # Each outer list represents a base location search results
                # Inner list is all levels found for that base location
                for loc_lvl_list in location_levels_list:
                    # print(loc_ts)
                    # Loop over the list of levels related to a given base location
                    for lvl_obj in loc_lvl_list:
                        print(lvl_obj)
                        payload.append(lvl_obj)

                if len(payload) > 0:
                    print(f"Posting {len(payload)} level values objects")
                    water.post_cwms_timeseries_measurements(payload)
                else:
                    raise AirflowSkipException(f"No level values to post")

            # -------------------------------------------

            # extract_radar_levels = Dynamic Task Mapping - each level run through extract function
            load_level_values_into_a2w(
                extract_radar_levels.expand(
                    level_path=extract_a2w_office_levels(office),
                )
            )

            return task_group

    task_groups = [create_task_group(office=office) for office in get_static_offices()]


level_values_dag = a2w_sync_level_values()
