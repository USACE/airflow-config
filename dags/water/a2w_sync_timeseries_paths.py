import json
from datetime import datetime, timedelta
import traceback
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

from helpers.sharedApi import get_static_offices, get_nwd_group
import helpers.water as water
import helpers.radar as radar

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=2)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 4, 4),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    default_args=default_args,
    tags=["a2w", "radar"],
    schedule_interval="0 14 * * *",
    max_active_runs=1,
    max_active_tasks=2,
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

    # -------------------------------------------

    def get_a2w_office_locations(office: str, offices: list):

        print(f"Office is: {office}")
        offices = json.loads(offices)

        # make call to water api for office locations
        locations = water.get_locations(
            office_id=get_office_id_by_symbol(office, offices)
        )

        result_locations = []

        for loc in locations:
            # print(loc)
            loc_parts = loc["name"].split("-")
            base_location = loc_parts[0]
            if base_location not in result_locations:
                result_locations.append(base_location)

        return result_locations

    # -------------------------------------------

    def get_radar_office_tsids_by_locations(
        radar_office: str, office: str, location: str, begin: str, end: str
    ):
        logging.info(f"Getting tsids for location -->  {location}")

        try:
            r = radar.get_timeseries([location + "*"], begin, end, radar_office)
            if r == None:
                raise ValueError(f"Invalid Response: {r}")
            r = json.loads(r.replace("\t", ""))
        except Exception as e:
            logging.error(f"Unable to retrieve {location} timeseries")
            logging.error(traceback.format_exc())

        location_results = []

        # Grab the time-series list object which can be iterated over
        # when multiple tsids are requested
        ts_obj_list = r["time-series"]["time-series"]

        # it may be possible for the extract task to return an empty
        # list if the tsid was not valid (not found in RADAR).
        # Ensure list is not empty before trying to extract items
        if len(ts_obj_list) == 0:
            print(f"{location} has no timeseries results")
            return []

        for ts_obj in ts_obj_list:
            loc_payload = {}
            loc_payload["provider"] = office
            loc_payload["datasource_type"] = "cwms-timeseries"
            loc_payload["key"] = ts_obj["name"]

            location_results.append(loc_payload)

        print(f"returning -> {location_results}")

        return location_results

    # -------------------------------------------

    @task
    def get_cwms_offices():
        offices = water.get_offices()
        # print(offices)
        return json.dumps(offices)

    offices = get_cwms_offices()

    for office in get_static_offices():

        priority_weight = 1 if get_nwd_group(office.upper()) in ["NWDM", "NWDP"] else 2

        @task(
            task_id=f"extract_and_load_{office}_tsids", priority_weight=priority_weight
        )
        def extract_and_load_tsids(office: str, offices: list):
            offices = json.loads(offices)
            print(offices)

            a2w_payload = []

            if get_nwd_group(office.upper()) in ["NWDM", "NWDP"]:
                # do some nonsense here

                # Get A2w Locations - Send actual office (NWP) not the region group (NWDP)
                a2w_office_locations = get_a2w_office_locations(office, offices)

                if len(a2w_office_locations) == 0:
                    raise AirflowSkipException("This office has no locations in a2w.")

                # Define the extract time-windows based on the task datetime
                logical_date = get_current_context()["logical_date"]
                begin = (logical_date - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
                end = (logical_date + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")

                # Use the locations in a2w for each office to get all possible timeseries
                # from the base locations
                for loc in a2w_office_locations:
                    try:
                        office_location_tsobj_list = (
                            get_radar_office_tsids_by_locations(
                                get_nwd_group(office.upper()), office, loc, begin, end
                            )
                        )
                    except:
                        # Prevent blowing up the whole task
                        continue

                    # Loop over the ts objects for each location
                    # Load into payload
                    for tsobj in office_location_tsobj_list:
                        print(tsobj)
                        a2w_payload.append(tsobj)

            # If not NWDP or NWDM offices
            else:

                r = radar.api_request("timeseries", f"name=@&office={office}")

                if r is None:
                    raise ValueError(f"Invalid Response: {r}")

                tsids = json.loads(r.replace("\t", ""))

                ts_obj_list = tsids["time-series-catalog"]["time-series"]

                if len(ts_obj_list) == 0:
                    # raise ValueError(f"RADAR payload for {office} has no tsids")
                    raise AirflowSkipException(
                        f"RADAR payload for {office} has no tsids"
                    )

                for ts_obj in ts_obj_list:
                    if ts_obj["office"].upper() == office.upper():
                        payload = {}
                        payload["key"] = ts_obj["name"]
                        payload["datasource_type"] = "cwms-timeseries"
                        payload["provider"] = ts_obj["office"].upper()
                        # print(payload)
                        a2w_payload.append(payload)

            # Post results back to a2w
            if len(a2w_payload) > 0:
                print(f"Posting {len(a2w_payload)} timeseries to a2w")
                # print(a2w_payload)
                water.post_cwms_timeseries(a2w_payload)

            return

        extract_and_load_tsids(office, offices)

    # _ = [create_task_group(office=office) for office in get_static_offices()]


timeseries_dag = a2w_sync_timeseries_paths()
