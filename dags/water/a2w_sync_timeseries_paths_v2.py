import json
from datetime import datetime, timedelta
import logging
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
def a2w_sync_timeseries_paths_v2():
    """Comments here"""

    def get_office_id_by_symbol(symbol: str, offices: list):
        offices = json.loads(offices)
        for o in offices:
            if o["symbol"].upper() == symbol.upper():
                return o["id"]
        return None

    @task
    def get_a2w_cwms_offices():
        offices = water.get_offices()
        # print(offices)
        return offices

    # Keep this task call out of the task groups below
    # to have it run once and feed all task groups
    get_offices = get_a2w_cwms_offices()

    # -------------------------------------------

    def create_task_group(**kwargs):
        office = kwargs["office"]

        with TaskGroup(group_id=f"{office}") as task_group:

            @task(task_id=f"extract_{office}_a2w_locations")
            def extract_a2w_office_locations(office: str, offices: list):
                office_id = get_office_id_by_symbol(office, offices)

                # make call to water api for office locations
                locations = water.get_locations(
                    office_id=office_id, kind_id="460ea73b-c65e-4fc8-907a-6e6fd2907a99"
                )

                if len(locations) == 0:
                    raise AirflowSkipException(f"No records found for {office}")

                location_list = []
                for loc in locations:
                    # print(loc)
                    location_list.append(loc["name"])
                return location_list

            # -------------------------------------------

            @task(task_id=f"extract_{office}_a2w_timeseries")
            def extract_a2w_office_timeseries(office: str):
                office_timeseries = water.get_cwms_timeseries(
                    provider=office, datasource_type="cwms-timeseries"
                )
                # if len(office_timeseries) == 0:
                #     raise AirflowSkipException(f"No records found for {office}")

                return office_timeseries

            # office_a2w_timeseries = extract_a2w_office_timeseries(office)
            # -------------------------------------------

            @task(task_id=f"transform_{office}_locations")
            def transform_locations(locations: list):

                # Remove sublocations
                # Base locations will get all sub locations from RADAR
                result_locations = []
                for loc in locations:
                    loc_parts = loc.split("-")
                    base_location = loc_parts[0]
                    if base_location not in result_locations:
                        result_locations.append(base_location)

                return result_locations

            # -------------------------------------------

            @task(task_id=f"extract_{office}_radar_timeseries")
            def extract_radar_timeseries(location_name):

                logging.info(f"Getting tsids for location -->  {location_name}")

                # Define the extract time-windows based on the task datetime
                logical_date = get_current_context()["logical_date"]
                begin = (logical_date - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
                end = (logical_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")

                r = radar.get_timeseries([location_name + "*"], begin, end, office)
                print(f"Radar response: {r}")
                r = json.loads(r)

                # Grab the time-series list object which can be iterated over
                # when multiple tsids are requested
                ts_obj_list = r["time-series"]["time-series"]

                # it may be possible for the extract task to return an empty
                # list if the tsid was not valid (not found in RADAR).
                # Ensure list is not empty before trying to extract items
                if len(ts_obj_list) == 0:
                    logging.warning(f"No data returned from RADAR for {location_name}.")
                    raise AirflowSkipException

                exclude_fpart_words = ["raw", "fcst"]

                location_results = []
                for ts_obj in ts_obj_list:

                    loc_payload = {}
                    loc_payload["provider"] = ts_obj["office"].lower()
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
                    # print(loc_payload)

                    tsid_parts = loc_payload["key"].split(".")
                    fpart = tsid_parts[-1]

                    exclude_match_found = False
                    for word in exclude_fpart_words:
                        if word.lower() in fpart.lower():
                            exclude_match_found = True

                    if exclude_match_found == False:
                        location_results.append(loc_payload)

                return location_results

            # -------------------------------------------

            @task(
                task_id=f"load_{office}_timeseries_into_a2w",
                trigger_rule="none_failed_min_one_success",
            )
            def load_timeseries_into_a2w(
                location_timeseries_list: _LazyXComAccess, a2w_timeseries
            ):

                new_timeseries = []

                # This should be a list of lists.
                # Each outer list represents a base location search results
                # Inner list is all timeseries found for that base location
                for loc_ts_list in location_timeseries_list:
                    # print(loc_ts)
                    # Loop over the list of timeseries related to a given base location
                    for ts_obj in loc_ts_list:

                        # Check to see if this current timeseries in loop is in the
                        # existing a2w timeseries dataset
                        tsid_found = False
                        for existing_ts in a2w_timeseries:
                            if existing_ts["key"].lower() == ts_obj["key"].lower():
                                print(f"removing -> {ts_obj}")
                                tsid_found = True

                        if tsid_found == False:
                            new_timeseries.append(ts_obj)

                print("-- new timeseries to post --")
                for t in new_timeseries:
                    print(t)

                print("--- existing ts in a2w ------")
                for existing_ts in a2w_timeseries:
                    print(existing_ts)

                water.post_cwms_timeseries(new_timeseries)

                return

            # -------------------------------------------

            extract_a2w_ts_task = extract_a2w_office_timeseries(office)
            extract_a2w_locs_task = extract_a2w_office_locations(office, get_offices)
            transform_locations_task = transform_locations(
                locations=extract_a2w_locs_task
            )

            # Dynamic Task Mapping - each location run through function
            extract_radar_ts = extract_radar_timeseries.expand(
                location_name=transform_locations_task,
            )

            load_ts_into_a2w = load_timeseries_into_a2w(
                extract_radar_ts, extract_a2w_ts_task
            )

            # fmt: off
            extract_a2w_locs_task >> transform_locations_task >> extract_radar_ts >> load_ts_into_a2w

            extract_a2w_ts_task >> load_ts_into_a2w
            # fmt: on

            return task_group

    # _ = [create_task_group(office=office) for office in get_static_offices()]
    _ = [create_task_group(office=office) for office in ["LRN", "MVP"]]


timeseries_dag = a2w_sync_timeseries_paths_v2()
