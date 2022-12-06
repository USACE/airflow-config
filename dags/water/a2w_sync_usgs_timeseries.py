"""
# Sync USGS Site Parameters (timeseries) with Water-API

- Get water usgs parameters
- Fetch and write


### USGS Water Services URL

- https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state.lower()}&siteType=LK,ST&siteStatus=active
"""

from datetime import datetime, timedelta
import logging

import helpers.water as water
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from helpers import US_STATES

DATATYPE = "usgs-timeseries"
PROVIDER = "usgs"
SUPPORTED_PARAM_CODES = ["00065", "000061", "62614", "62615", "00045", "00010", "00011"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    default_args=default_args,
    tags=["a2w", "usgs"],
    schedule_interval="@daily",
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
    doc_md=__doc__,
)
def a2w_sync_usgs_timeseries():
    for state in US_STATES:
        with TaskGroup(group_id=f"{state}") as tg:

            @task(task_id=f"{state}_water_tsid_keys")
            def water_tsid_keys(state):

                water_hook = water.WaterHook(method="GET")
                tsids = water_hook.request(
                    endpoint=f"/timeseries?datatype={DATATYPE}&provider={PROVIDER}&state={state}"
                )

                tsid_keys_asdict = {item["key"]: item for item in tsids}

                return tsid_keys_asdict

            @task(task_id=f"{state}_usgs_timeseries")
            def usgs_timeseries(state, tsid_keys_asdict):

                usgs_state_sites_url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state.lower()}&siteType=LK,ST&siteStatus=active"
                r = requests.get(usgs_state_sites_url)
                usgs_state_sites = r.json()

                create_payload = []

                for i, state_site in enumerate(usgs_state_sites["value"]["timeSeries"]):
                    site_number = state_site["sourceInfo"]["siteCode"][0]["value"]
                    # print(site_number)
                    param_code = state_site["variable"]["variableCode"][0]["value"]
                    # print(param_code)

                    new_tsid = f"{site_number}.{param_code}"

                    if (
                        new_tsid not in tsid_keys_asdict
                        and param_code in SUPPORTED_PARAM_CODES
                    ):
                        _payload_obj = {}
                        _payload_obj["datatype"] = DATATYPE
                        _payload_obj["key"] = new_tsid
                        _payload_obj["location"] = {
                            "provider": PROVIDER,
                            "datatype": "usgs-location",
                            "code": site_number,
                            "state": state,
                        }
                        _payload_obj["provider"] = PROVIDER

                        create_payload.append(_payload_obj)

                if create_payload == 0:
                    raise AirflowSkipException

                return create_payload

            @task(task_id=f"{state}_create_timeseries")
            def create_timeseries(payload):

                if len(payload) == 0:
                    raise AirflowSkipException("Skipping...No new timeseries to post")

                logging.info(f"POSTING {len(payload)} objects to water-api")

                water_hook = water.WaterHook(method="POST")
                resp = water_hook.request(
                    endpoint=f"/providers/{PROVIDER}/timeseries",
                    json=payload,
                )

                logging.info(f"{len(resp)} timeseries accepted by water-api")

                return

            _water_tsid_keys = water_tsid_keys(state)
            _usgs_timeseries = usgs_timeseries(state, _water_tsid_keys)
            create_timeseries(_usgs_timeseries)


DAG_ = a2w_sync_usgs_timeseries()
