"""
# A2W Sync CWMS Locations

Task groups created per District (MSC) each having the same tasks.
The `chain()` function is used to force the group order to defined by the
constant variable `MSC`, which is a list of all MSCs.  MSC list
order defines priority ranking.  Task order is as follows:

- radar_locations: get radar locations
- transform_radar: transform into usable JSON
- water_locations: get locations in Water
- create_post_sets: create a unique `set` of locations to post
- create_update_sets: create a unique `set` of locations to update
- post_sets: post the unique `set`
- update_sets: update the unique `set`
"""
import json
from dataclasses import asdict, astuple, dataclass, field
from datetime import datetime, timedelta

import helpers.radar as radar
import helpers.water as water
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from helpers import usace_office_group

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
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=4),
    "tigger_rule": TriggerRule.ALL_DONE,
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
    previous = None
    for office in ["LRB", "NWD", "NWS"]:
        with TaskGroup(group_id=f"{office}") as tg:

            @task()
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

                radar_hook = radar.RadarHook()
                radar_hook.response_type = "text"
                response_payload = radar_hook.request_(
                    method="GET",
                    url=uri + f"?office={office}&names=@",
                )

                # resp = radar.radar_request(uri=uri, query=f"office={office}&names=@")
                # response_payload = resp.text

                if response_payload is None or "DOCTYPE html" in response_payload:
                    return list()
                response_str = response_payload.translate(
                    response_payload.maketrans("", "", "\n\r\t")
                ).replace("None", "")
                response_json = json.loads(response_str)
                locations = response_json["locations"]["locations"]

                return locations

            @task()
            def transform_radar(office, office_locations):
                radar_locations = []
                for location in office_locations:
                    try:
                        identity_office = location["identity"]["office"]
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
                    if (
                        identity_office != bounding_office
                        and identity_office.upper()
                        in [
                            "NWDM",
                            "NWDP",
                        ]
                    ):
                        identity_office = bounding_office

                    # only add those objects that coorespond to the office we want
                    if identity_office != office:
                        continue

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
                    if identity_office is not None:
                        radar_loc.provider = identity_office[:3]
                    if name is not None:
                        radar_loc.code = name
                    if state is not None:
                        radar_loc.state = state

                    radar_locations.append(asdict(radar_loc))

                return radar_locations

            @task()
            def water_locations(office):
                water_hook = water.WaterHook(method="GET")
                try:
                    resp = water_hook.request(
                        endpoint=f"/locations?provider={office.lower()}",
                    )
                    return resp
                except Exception as e:
                    print(e, f"Office ({office}) may not be a provider")
                    return list()

            @task()
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

            @task()
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
                        update_list.append({**water_asdict[code], **_radar_dict})

                return update_list

            # POST or PUT data sets
            def post_put_sets(payload, office, method):
                if len(payload) > 0:
                    water_hook = water.WaterHook(method=method)
                    resp = water_hook.request(
                        endpoint=f"/providers/{office.lower()}/locations",
                        json=payload,
                    )
                    return resp

            @task()
            def post_sets(payload, office, method):
                return post_put_sets(payload, office, method)

            @task()
            def update_sets(payload, office, method):
                return post_put_sets(payload, office, method)

            _radar_locations = radar_locations(office)
            _transform_radar = transform_radar(office, _radar_locations)
            _water_locations = water_locations(office)
            _create_post_sets = create_post_sets(_transform_radar, _water_locations)
            _create_update_sets = create_update_sets(_transform_radar, _water_locations)
            _post_sets = post_sets(_create_post_sets, office, "POST")
            _put_sets = update_sets(_create_update_sets, office, "PUT")

        if previous:
            previous >> tg

        previous = tg


sync_locations_dag = a2w_sync_cwms_locations()
