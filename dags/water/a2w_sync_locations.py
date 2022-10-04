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

__North America centroid default for missing coordinates__
"""

import json
from datetime import datetime, timedelta

import helpers.radar as radar
import helpers.water as water
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from helpers.sharedApi import get_nwd_group, get_static_offices

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


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    tags=["a2w", "radar"],
    max_active_runs=2,
    max_active_tasks=3,
    catchup=False,
    doc_md=__doc__,
)
def a2w_sync_locations():
    @task
    def get_a2w_office_ids():
        water_office_ids = json.loads(water.get_offices())
        office_ids = {item["symbol"]: item["id"] for item in water_office_ids}
        return office_ids

    office_ids = get_a2w_office_ids()

    @task
    def get_a2w_office_kind_ids():
        water_kind_ids = json.loads(water.get_location_kind())
        kind_ids = {item["name"]: item["id"] for item in water_kind_ids}
        return kind_ids

    office_kind_ids = get_a2w_office_kind_ids()

    def create_tasks_group(office):
        with TaskGroup(group_id=f"{office}") as task_group:

            @task(task_id=f"get_locations_{office}")
            def list_locations(office):
                the_office = (
                    office if get_nwd_group(office) is None else get_nwd_group(office)
                )

                locations = radar.api_request(
                    "locations", f"office={the_office}&names=@"
                )
                if locations is None or "DOCTYPE html" in locations:
                    return list()
                locations_str = locations.translate(
                    locations.maketrans("", "", "\n\r\t")
                ).replace("None", "")
                locations_json = json.loads(locations_str)
                locations_list = locations_json["locations"]["locations"]

                return locations_list

            @task(task_id=f"transform_locations_{office}")
            def transform_locations(office_locations, office_ids, kind_ids):

                radar_locations = []
                for location in office_locations:
                    kind = location["classification"]["location-kind"]
                    kind_id = kind_ids[kind.upper()]
                    office = location["identity"]["office"]
                    bounding_office = location["political"]["bounding-office"]

                    if office == bounding_office or bounding_office is None:
                        the_office = office
                    # If a division/region has the same location name as the district and sets the
                    # bounding office correctly to the district, we DON'T want to override the
                    # district office's meta-data with what the division/region sets UNLESS
                    # it's in the NWD regions.
                    elif office != bounding_office and office.upper() in [
                        "NWDM",
                        "NWDP",
                    ]:
                        the_office = bounding_office
                    else:
                        the_office = office

                    # All office symbols are 3 chars
                    try:
                        office_id = office_ids[the_office[:3]]
                    except KeyError as err:
                        print(err)
                        print(the_office)
                        continue

                    # Dealing with missing geo location and a public name
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

            @task(task_id=f"sync_locations_{office}")
            def sync_locations(locations):
                """
                Loop through locations
                """
                water.sync_radar_locations(locations)
                # loc = json_drop_duplicates(locations)
                # for l in loc:
                #     water.sync_radar_locations(l)

            sync_locations(
                transform_locations(
                    office_locations=list_locations(office),
                    office_ids=office_ids,
                    kind_ids=office_kind_ids,
                )
            )

        return task_group

    task_groups = [create_tasks_group(office) for office in get_static_offices()]

    # Force task groups to run in serial instead of default parallel
    for idx, tg in enumerate(task_groups):
        if idx > 0:
            last_task_group >> tg
        last_task_group = tg

    office_ids >> office_kind_ids >> task_groups


sync_locations_dag = a2w_sync_locations()
