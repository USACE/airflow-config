import json
from datetime import datetime, timedelta
import traceback
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow import AirflowException
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
    "execution_timeout": timedelta(hours=4),
}


@dag(
    default_args=default_args,
    tags=["a2w", "radar"],
    schedule="0 17 * * *",
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    description="Extract Location Levels from RADAR, Post to Water API",
)
def a2w_sync_level_paths():
    """Comments here"""

    # -------------------------------------------
    def get_a2w_office_locations(office: str) -> dict:

        # make call to water api for office locations
        locations = water.api_request(
            f"/locations?datatype=cwms-location&provider={office}",
            method="GET",
            payload=None,
            expected_status_code=200,
        )

        locations = json.loads(locations)
        result_locations = {}

        for loc in locations:
            # print(loc)
            if loc["code"] not in result_locations:
                result_locations[loc["code"]] = True

        return result_locations

    # -------------------------------------------
    def get_a2w_office_levels(office: str) -> list:

        # make call to water api for office levels
        levels = water.api_request(
            f"/timeseries?datatype=cwms-level&provider={office}",
            method="GET",
            payload=None,
            expected_status_code=200,
        )

        levels = json.loads(levels)
        result_levels = []

        for lvl in levels:
            # print(loc)
            if lvl["key"] not in result_levels:
                result_levels.append(lvl["key"])

        return result_levels

    # -------------------------------------------
    def get_official_cwms_level(level_list: list, office_locations: list) -> str:

        for lvl in level_list:
            # print(f"Looking for {lvl.split('.')[0]} in {office_locations}")

            if lvl.split(".")[0] in office_locations:
                return lvl

    # -------------------------------------------
    def get_radar_office_levels_by_locations(
        radar_office: str, office: str, location: str
    ) -> list:
        logging.info(f"Getting levels for location -->  {location}")

        try:
            r = radar.api_request(
                "levels", f"name={location}*&office={radar_office}&format=json"
            )
            # r = radar.get_levels([location + "*"], radar_office)
            if r == None:
                raise ValueError(f"Invalid Response: {r}")
            r = json.loads(r)
        except Exception as e:
            logging.error(f"Unable to retrieve {location} levels")
            logging.error(traceback.format_exc())

        location_results = []

        # Grab the levels list object which can be iterated over
        # when multiple tsids are requested
        lvl_obj_list = r["location-levels"]["location-levels"]

        # it may be possible for the extract task to return an empty
        # list if the level was not valid (not found in RADAR).
        # Ensure list is not empty before trying to extract items
        if len(lvl_obj_list) == 0:
            logging.info(f"{location} has no level results")
            return []

        for lvl_obj in lvl_obj_list:
            loc_payload = {}
            loc_payload["provider"] = office
            loc_payload["datatype"] = "cwms-level"
            loc_payload["key"] = lvl_obj["name"]
            loc_payload["location"] = {
                "provider": office,
                "datatype": "cwms-location",
                "code": loc_payload["key"].split(".")[0],
            }

            location_results.append(loc_payload)

        # print(f"returning -> {location_results}")

        return location_results

    @task
    def check_radar_service():
        r = radar.api_request("offices")
        if r is None:
            raise AirflowException("Failed RADAR check")
        return

    check_radar_task = check_radar_service()

    dynamic_tasks = []

    for office in get_static_offices():

        priority_weight = 1 if get_nwd_group(office.upper()) in ["NWDM", "NWDP"] else 2

        task_id = f"extract_and_load_{office}_levels"

        @task(task_id=task_id, priority_weight=priority_weight)
        def extract_and_load_levels(office: str):

            office_locations = get_a2w_office_locations(office)

            if len(office_locations) == 0:
                logging.warning(
                    "No locations for this office in a2w. Will be unable to select correct location from RADAR.  Skipping task."
                )
                raise AirflowSkipException

            a2w_payload = []

            if get_nwd_group(office.upper()) in ["NWDM", "NWDP"]:

                # Get A2w Locations - Send actual office (NWP, NWS, NWW) not the region group (NWDP)
                # a2w_office_locations = get_a2w_office_locations(office, offices)

                # if len(a2w_office_locations) == 0:
                #     raise AirflowSkipException("This office has no locations in a2w.")

                # Use the locations in a2w for each office to get all possible timeseries
                # from the base locations
                for loc in office_locations:
                    try:
                        office_location_lvlobj_list = (
                            get_radar_office_levels_by_locations(
                                get_nwd_group(office.upper()), office, loc
                            )
                        )
                    except:
                        # Prevent blowing up the whole task
                        continue

                    # Loop over the ts objects for each location
                    # Load into payload
                    for lvlobj in office_location_lvlobj_list:
                        # print(lvlobj)
                        a2w_payload.append(lvlobj)

            # If not NWDP or NWDM offices
            else:

                r = radar.api_request("levels", f"office={office}&format=json")

                if r is None:
                    raise ValueError(f"Invalid Response: {r}")

                levels = json.loads(r.replace("\t", ""))

                lvl_obj_list = levels["location-levels-catalog"]["location-levels"]

                # it may be possible for the extract task to return an empty
                # list if the tsid was not valid (not found in RADAR).
                # Ensure erray is not empty before trying to extract items
                if len(lvl_obj_list) == 0:
                    logging.warning(
                        "No data returned from RADAR.  Skipping POST to Water API"
                    )
                    raise AirflowSkipException

                for lvl_obj in lvl_obj_list:
                    if lvl_obj["office"].upper() == office.upper():
                        payload = {}

                        # RADAR does not provide the correct CWMS named location
                        # in the 'name' field when you only query by office.
                        possible_names = lvl_obj["alternate-names"]
                        possible_names.append(lvl_obj["name"])

                        cwms_level = get_official_cwms_level(
                            possible_names, office_locations
                        )

                        # If no match found for any of the level paths
                        if cwms_level is None:
                            logging.info(
                                f"Could not find {possible_names} in locations."
                            )
                            continue

                        payload["key"] = get_official_cwms_level(
                            possible_names, office_locations
                        )

                        payload["location"] = {
                            "provider": lvl_obj["office"],
                            "datatype": "cwms-location",
                            "code": payload["key"].split(".")[0],
                        }
                        payload["datatype"] = "cwms-level"
                        payload["provider"] = lvl_obj["office"].upper()
                        # print(payload)
                        a2w_payload.append(payload)

            ## Get existing office levels
            existing_office_levels = get_a2w_office_levels(office)

            create_payload = []

            # Filter out the existing levels for this office
            # leaving only the newly discovered levels not in water-wapi
            for lvl in a2w_payload:
                if lvl["key"] not in existing_office_levels:
                    create_payload.append(lvl)

            # Post results back to a2w
            if len(create_payload) > 0:
                print(f"Posting {len(create_payload)} timeseries to a2w")

                print(create_payload)

                water.api_request(
                    path=f"/providers/{office}/timeseries",
                    method="POST",
                    payload=create_payload,
                    expected_status_code=201,
                )
            else:
                raise AirflowSkipException("no new levels to post")

            return

        dtask = extract_and_load_levels(office)
        dynamic_tasks.append(dtask)

    check_radar_task >> dynamic_tasks


timeseries_dag = a2w_sync_level_paths()
