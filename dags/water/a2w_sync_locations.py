"""
# Sync CWMS RADAR Locations
"""

import json
from termios import TIOCPKT_STOP
import pandas as pd
from datetime import datetime, timedelta

import helpers.radar as radar
import helpers.water as water
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

# Default arguments
default_args = {
    "owner": "airflow",
    "start_date": (datetime.utcnow() - timedelta(hours=6)).replace(minute=15, second=0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def json_drop_duplicates(payload):
    df = pd.json_normalize(payload)
    df = df.drop_duplicates(subset=["office_id", "name"], ignore_index=True)
    df_to_json = json.loads(df.to_json(orient="records"))
    _d = {}
    _l = []
    for d in df_to_json:
        to_pop = []
        for k, v in d.items():
            if "." in k:
                _k, _v = k.split(".")
                if _k in _d:
                    _d[_k][_v] = d[k]
                else:
                    _d[_k] = {_v: d[k]}
                to_pop.append(k)
        [d.pop(p) for p in to_pop]
        _l.append({**d, **_d})

    return _l


with DAG(
    dag_id="a2w_sync_locations",
    default_args=default_args,
    schedule_interval="15 */3 * * *",
    tags=["a2w", "radar"],
    max_active_runs=2,
    max_active_tasks=3,
) as dag:
    """
    # Sync Water database with RADAR Locations

    RADAR Locations retrieved as JSON string

    ## Parse:

    Extract schema needed for posting to Water API

    ## Post:

    Water API endpoint `/sync/locations`

    ```{
        'office_id',
        'name',
        'public_name',
        'kind_id',
        'geometry' : {
            'type',
            'coordinates': [
                101, 47
            ]
        }
    }```

    __North America centriod default for missing coordinates__
    """

    @task
    def cwms_offices():
        objects = water.get_offices()
        _objects = json.loads(objects)
        offices = [obj["symbol"] for obj in _objects if obj["symbol"] is not None]

        return None if offices is [] else offices

    @task
    def add_cwms_office(offices):
        offices[offices.index("NWD")] = "NWDM"
        offices[offices.index("NWP")] = "NWDP"
        return offices

    @task
    def get_locations(office):
        locations = radar.api_request("locations", f"office={office}&names=@")
        if locations is None or "DOCTYPE html" in locations:
            return list()
        locations_str = locations.translate(
            locations.maketrans("", "", "\n\r\t")
        ).replace("None", "")
        locations_json = json.loads(locations_str)
        locations_list = locations_json["locations"]["locations"]

        return locations_list

    @task
    def transform_location(locations):
        context = get_current_context()
        ti = context["ti"]
        locations_xcom_pull = ti.xcom_pull(task_ids=locations.task_id)

        # from water, get the location kinds, offices, and make a dict
        water_kind_ids = json.loads(water.get_location_kind())
        kind_ids = {item["name"]: item["id"] for item in water_kind_ids}
        water_office_ids = json.loads(water.get_offices())
        office_ids = {item["symbol"]: item["id"] for item in water_office_ids}

        radar_locations = []
        for locations in locations_xcom_pull:
            for location in locations:
                kind = location["classification"]["location-kind"]
                kind_id = kind_ids[kind.upper()]
                office = location["identity"]["office"]
                if office == "NWDM":
                    office_id = office_ids["NWD"]
                elif office == "NWDP":
                    office_id = office_ids["NWP"]
                else:
                    office_id = office_ids[office]

                lon = (
                    location["geolocation"]["longitude"]
                    if location["geolocation"]["latitude"] is not None
                    else 101
                )
                lat = (
                    location["geolocation"]["latitude"]
                    if location["geolocation"]["latitude"] is not None
                    else 47
                )
                public_name = (
                    location["label"]["public-name"]
                    if location["label"]["public-name"] is not None
                    else location["identity"]["name"]
                )

                # Appending dictionary to post
                radar_locations.append(
                    {
                        "office_id": office_id,
                        "name": location["identity"]["name"],
                        "public_name": public_name,
                        "kind_id": kind_id,
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                lon,
                                lat,
                            ],
                        },
                    }
                )

        return radar_locations

    @task
    def post_locations(locations):
        loc = json_drop_duplicates(locations)
        return_message = water.sync_radar_locations(loc)
        # for loc in locations:
        #     return_message = water.sync_radar_locations(
        #         payload=loc
        #     )
        # print(f"{return_message=}")

    offices = cwms_offices()
    added_offices = add_cwms_office(offices)
    locations = get_locations.expand(office=added_offices)
    transform_loc = transform_location(locations)
    post_to_water = post_locations(transform_loc)

    offices >> added_offices >> locations >> transform_loc >> post_to_water
