"""
# A2W Sync CWMS Timeseries Paths

# Tasks

Task groups created per District (MSC) each having the same tasks.
MSC list order defines priority ranking.  Tasks are as follows:

- water_tsid_keys: get TSID keys in Water DB
- radar_timeseries: get RADAR TSIDs and compare with Water TSIDs
- create_timeseries: create TSIDs not in Water DB

## Skipping Tasks

Airflow's skip exception (`AirflowSkipException`) is used for exceptions and 
when no data is available for a POST or a PUT.
"""

from datetime import datetime, timedelta

import helpers.radar as radar
import helpers.water as water
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from helpers import MSC, usace_office_group

# CWMS Locations data type
CWMS_LOCATION = "cwms-location"
CWMS_TIMESERIES = "cwms-timeseries"


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
    schedule="0 14 * * *",
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    doc_md=__doc__,
)
def a2w_sync_cwms_timeseries():
    previous = None
    for office in MSC:
        with TaskGroup(group_id=f"{office}") as tg:

            @task(
                task_id=f"{office}_water_tsid_keys",
                trigger_rule=TriggerRule.ALL_DONE,
            )
            def water_tsid_keys(office):
                try:
                    water_hook = water.WaterHook(method="GET")
                    tsids = water_hook.request(
                        endpoint=f"/timeseries?datatype={CWMS_TIMESERIES}&provider={office.lower()}"
                    )
                    tsid_keys_asdict = {item["key"]: item for item in tsids}
                    return tsid_keys_asdict
                except Exception as ex:
                    raise AirflowSkipException(ex)

            @task(
                task_id=f"{office}_radar_timeseries",
                trigger_rule=TriggerRule.NONE_SKIPPED,
            )
            def radar_timeseries(office, tsid_keys_asdict):
                logical_date = get_current_context()["logical_date"]
                logical_date = logical_date - timedelta(hours=12)
                begin_date = logical_date.strftime("%Y-%m-%dT%H:%M:%SZ")

                # get the data source uri
                try:
                    water_hook = water.WaterHook(method="GET")
                    datasource = water_hook.request(
                        endpoint=f"/datasources?datatype={CWMS_TIMESERIES}&provider={office.lower()}"
                    )
                    uri = datasource[0]["datatype_uri"]

                    office_ = usace_office_group(office)
                    radar_hook = radar.RadarHook()
                    radar_tsids = radar_hook.request_(
                        method="GET",
                        url=uri
                        + f"?name=@&office={office_}&begin={begin_date}&format=json",
                    )
                except Exception as ex:
                    raise AirflowSkipException(ex)

                # get a set of RADAR tsids, Water keys and find what Water does not have
                radar_tsids_set = {
                    item["name"]
                    for item in radar_tsids["time-series-catalog"]["time-series"]
                }
                water_key_set = set(tsid_keys_asdict.keys())
                tsids_create_set = radar_tsids_set.difference(water_key_set)

                tsids_create = []
                for tsid in tsids_create_set:
                    try:
                        code = tsid.split(".")[0]
                    except IndexError as err:
                        print(err)
                        continue

                    payload = {
                        "provider": office,
                        "datatype": CWMS_TIMESERIES,
                        "key": tsid,
                        "location": {
                            "provider": office,
                            "code": code,
                        },
                    }
                    tsids_create.append(payload)

                if len(tsids_create) == 0:
                    raise AirflowSkipException

                return tsids_create

            @task(
                task_id=f"{office}_create_timeseries",
                trigger_rule=TriggerRule.NONE_SKIPPED,
            )
            def create_timeseries(provider, payload):
                if len(payload) > 0:
                    water_hook = water.WaterHook(method="POST")
                    resp = water_hook.request(
                        endpoint=f"/providers/{provider.lower()}/timeseries",
                        json=payload,
                    )
                    return resp

            _water_tsid_keys = water_tsid_keys(office)
            _radar_timeseries = radar_timeseries(office, _water_tsid_keys)
            _create_timeseries = create_timeseries(office, _radar_timeseries)

        if previous:
            previous >> tg

        previous = tg


DAG_ = a2w_sync_cwms_timeseries()
