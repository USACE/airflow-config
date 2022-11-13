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
            0, 0
        ]
    }
}```

__North America centroid default for missing coordinates__
"""
from dataclasses import asdict, astuple, dataclass, field
import json
from datetime import datetime, timedelta

import helpers.radar as radar
import helpers.water as water
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from helpers import usace_office_group
from airflow.models.baseoperator import chain

# CWMS Locations data type
DATATYPE = "cwms-location"

# dataclasses to help define defaults when RADAR results have 'None' values for attributes
@dataclass
class RadarPoint:
    lon: float = 0.0
    lat: float = 0.0


@dataclass
class RadarGeometry:
    type: str = "Point"
    coordinates: list = field(default_factory=list)


@dataclass
class RadarAttr:
    public_name: str = "No public name"
    kind: str = "SITE"


@dataclass
class RadarLocation:
    provider: str = field(default="", metadata={"alias": "office"})
    datatype: str = field(default=DATATYPE)
    code: str = field(default="", metadata={"alias": "Location Code"})
    geometry: dict = field(default_factory=dict)
    attributes: dict = field(default_factory=dict)
    state: str = "AK"


# Default arguments
default_args = {
    "owner": "airflow",
    "start_date": (datetime.utcnow() - timedelta(days=2)).replace(minute=0, second=0),
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
def a2w_sync_cwms_locations():
    def create_task_group(office):
        with TaskGroup(group_id=f"{office}_Sync_CWMS_locations") as tg:

            @task(task_id=f"radar_locations")
            def radar_locations(office):
                try:
                    water_hook = water.WaterHook(method="GET")
                    resp = water_hook.request(
                        endpoint=f"/datasources?provider={office.lower()}&datatype={DATATYPE}"
                    )
                    uri = resp[0]["datatype_uri"]
                except KeyError as ke:
                    print(ke)
                    return list()

                office = usace_office_group(office)

                resp = radar.radar_request(uri=uri, query=f"office={office}&names=@")
                response_payload = resp.text

                if response_payload is None or "DOCTYPE html" in response_payload:
                    return list()
                response_str = response_payload.translate(
                    response_payload.maketrans("", "", "\n\r\t")
                ).replace("None", "")
                response_json = json.loads(response_str)
                locations = response_json["locations"]["locations"]

                return locations

            @task(task_id=f"transform_radar")
            def transform_radar(office_locations):
                radar_locations = []
                for location in office_locations:
                    try:
                        office = location["identity"]["office"]
                        name = location["identity"]["name"]
                        public_name = location["label"]["public-name"]
                        lon = location["geolocation"]["longitude"]
                        lat = location["geolocation"]["latitude"]
                        state = location["political"]["state"]
                        bounding_office = location["political"]["bounding-office"]
                        location_kind = location["classification"]["location-kind"]
                    except KeyError as ke:
                        print(ke)

                    # If a division/region has the same location name as the district and sets the
                    # bounding office correctly to the district, we DON'T want to override the
                    # district office's meta-data with what the division/region sets UNLESS
                    # it's in the NWD regions.
                    if office != bounding_office and office.upper() in [
                        "NWDM",
                        "NWDP",
                    ]:
                        office = bounding_office

                    # Points
                    radar_pnt = RadarPoint()
                    if lon is not None:
                        radar_pnt.lon = lon
                    if lat is not None:
                        radar_pnt.lat = lat
                    # Geometry
                    radar_geo = RadarGeometry(coordinates=list(astuple(radar_pnt)))
                    # Attributes
                    radar_attr = RadarAttr()
                    if public_name is not None:
                        radar_attr.public_name = public_name
                    if location_kind is not None:
                        radar_attr.kind = location_kind
                    # Location
                    radar_loc = RadarLocation(geometry=radar_geo, attributes=radar_attr)
                    if office is not None:
                        radar_loc.provider = office[:3]
                    if name is not None:
                        radar_loc.code = name
                    if state is not None:
                        radar_loc.state = state

                    radar_locations.append(asdict(radar_loc))

                return radar_locations

            @task(task_id=f"water_locations")
            def water_locations(office):
                water_hook = water.WaterHook(method="GET")
                try:
                    resp = water_hook.request(
                        endpoint=f"/locations?provider={office}",
                    )
                    return resp
                except Exception as e:
                    print(e, f"Office ({office}) may not be a provider")
                    return list()

            @task(task_id="create_post_sets")
            def create_post_sets(radar, water):
                radar_asdict = {(item["code"]).lower(): item for item in radar}
                water_asdict = {(item["code"]).lower(): item for item in water}
                radar_keys = set(radar_asdict.keys())
                water_keys = set(water_asdict.keys())
                create_codes = radar_keys.difference(water_keys)

                try:
                    post_objects = [
                        v for k, v in radar_asdict.items() if k in create_codes
                    ]
                except TypeError as te:
                    print(te, "; No codes to create, continue with empty list")
                    post_objects = []

                return post_objects

            @task(task_id="create_update_sets")
            def create_update_sets(radar, water):
                update_attr = ["geometry", "state", "attributes"]

                radar_asdict = {(item["code"]).lower(): item for item in radar}
                water_asdict = {(item["code"]).lower(): item for item in water}
                radar_keys = set(radar_asdict.keys())
                water_keys = set(water_asdict.keys())
                shared_codes = radar_keys.intersection(water_keys)

                update_list = []
                for code in shared_codes:
                    _radar_dict = {
                        k: v for k, v in radar_asdict[code].items() if k in update_attr
                    }
                    _water_dict = {
                        k: v for k, v in water_asdict[code].items() if k in update_attr
                    }

                    if _water_dict != _radar_dict:
                        print(f"{_water_dict=}", f"{_radar_dict=}", sep="\n")
                        update_list.append({**water_asdict[code], **_radar_dict})

                print(f"{update_list=}")
                return update_list

            @task(task_id=f"post_update_sets")
            def post_update_sets(payload_office_method):
                for pom in payload_office_method:
                    payload = pom["payload"]
                    office = pom["office"]
                    method = pom["method"]
                    if len(payload) > 0:
                        water_hook = water.WaterHook(method=method)
                        resp = water_hook.request(
                            endpoint=f"/providers/{office.lower()}/locations",
                            json=pom["payload"],
                        )
                        return resp

            _radar_locations = radar_locations(office)
            _transform_radar = transform_radar(_radar_locations)
            _water_locations = water_locations(office)
            _create_post_sets = create_post_sets(_transform_radar, _water_locations)
            _create_update_sets = create_update_sets(_transform_radar, _water_locations)

            payload_office_method = [
                {"payload": _create_post_sets, "office": office, "method": "POST"},
                {"payload": _create_update_sets, "office": office, "method": "PUT"},
            ]

            _post_sets = post_update_sets(payload_office_method)


        return tg

    task_groups = [create_task_group(office) for office in ["LRB"]]

    chain(*task_groups)

    return task_groups


sync_locations_dag = a2w_sync_cwms_locations()
