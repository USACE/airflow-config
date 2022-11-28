"""
# Extract Location Levels from RADAR, Post to Water API
"""

from datetime import datetime, timedelta, timezone
import json
import logging

import helpers.radar as radar
import helpers.water as water
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

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
    description=__doc__,
)
def a2w_sync_cwms_levels():
    def create_task_group(office):
        with TaskGroup(group_id=f"{office}") as tg:

            @task()
            def water_location_codes(office):
                water_hook = water.WaterHook(method="GET")
                try:
                    locations = water_hook.request(
                        endpoint=f"/locations?datatype=cwms-location&provider={office.lower()}"
                    )
                    location_codes_asdict = {item["code"]: item for item in locations}
                    return location_codes_asdict
                except Exception:
                    raise AirflowSkipException

            @task()
            def water_level_keys(office):
                water_hook = water.WaterHook(method="GET")
                levels = water_hook.request(
                    endpoint=f"/timeseries?datatype=cwms-level&provider={office.lower()}"
                )
                level_keys_asdict = {item["key"]: item for item in levels}
                return level_keys_asdict

            @task()
            def radar_levels(office, location_codes_asdict, level_keys_asdict):
                # get the data source uri
                water_hook = water.WaterHook(method="GET")
                datasource = water_hook.request(
                    endpoint=f"/datasources?datatype=cwms-level&provider={office.lower()}"
                )
                uri = datasource[0]["datatype_uri"]

                # loop through the list of location codes to get the available levels
                # make a request for each code
                # if levels list is [], then no level available for the location code
                radar_hook = radar.RadarHook()
                create_levels = []
                update_levels = []
                for code, d in location_codes_asdict.items():
                    location_levels = radar_hook.request_(
                        method="GET",
                        url=uri
                        + f"?level-id-mask={code}.*&office={office}&format=json",
                    )
                    # None Type in location levels return or Key Error continues to the next code
                    try:
                        if location_levels is None:
                            raise TypeError
                        location_levels_list = location_levels["location-levels"][
                            "location-levels"
                        ]
                    except (KeyError, TypeError) as err:
                        print(f"{code=}; KeyError - {err}")
                        continue

                    for level in location_levels_list:
                        key = level["name"]
                        code = key.split(".")[0]
                        try:
                            dt, val = level["values"]["segments"][0]["values"][-1]
                            dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").replace(
                                tzinfo=timezone.utc
                            )
                            payload = {
                                "provider": office,
                                "datatype": "cwms-level",
                                "key": key,
                                "location": {
                                    "provider": office,
                                    "code": code,
                                },
                                "values": [[dt.isoformat(), val]],
                            }

                            update_levels.append(payload)
                            # This determines if it needs to be created first
                            if key not in level_keys_asdict:
                                create_levels.append(payload)
                        except KeyError as err:
                            print(err)
                            continue

                return (create_levels, update_levels)

            @task()
            def create_level(provider, payloads):
                # create_update_level(f"/providers/{provider}/timeseries", payloads[0])
                payload = payloads[0]
                water_hook = water.WaterHook(method="POST")
                resp = water_hook.request(
                    endpoint=f"/providers/{provider.lower()}/timeseries",
                    json=payload,
                )
                return resp

            @task()
            def update_measurement(provider, payloads):
                # create_update_level(f"/providers/{provider}/timeseries/values", payloads[1])
                payload = payloads[1]
                water_hook = water.WaterHook(method="POST")
                resp = water_hook.request(
                    endpoint=f"/providers/{provider.lower()}/timeseries/values",
                    json=payload,
                )
                return resp

            _water_locations = water_location_codes(office)
            _water_level_keys = water_level_keys(office)
            _radar_levels = radar_levels(office, _water_locations, _water_level_keys)
            _create_level = create_level(office, _radar_levels)
            _update_measurement = update_measurement(office, _radar_levels)

            _radar_levels >> [_create_level, _update_measurement]

        return tg

    task_groups = [create_task_group(office) for office in ["LRB"]]  # MSC]

    chain(*task_groups)

    return task_groups


DAG_ = a2w_sync_cwms_levels()
