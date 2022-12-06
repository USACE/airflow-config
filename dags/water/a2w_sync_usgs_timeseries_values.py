"""
# Sync USGS Timeseries Values with Water-API

- Get water-api usgs timeseries (that are enabled for etl)
- Get values by state
- Write values to water-api


### USGS Water Services URL

- https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state.lower()}&siteType=LK,ST&siteStatus=active

Each State Request might look like:

- http://waterservices.usgs.gov/nwis/iv/format=json&sites=01646500,03184500&parameterCd=00060,00065&stateCd={state.lower()}&siteType=LK,ST&siteStatus=active

"""

from datetime import datetime, timedelta
import logging
from urllib.parse import urlencode, unquote

import helpers.water as water
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from helpers import US_STATES

DATATYPE = "usgs-timeseries"
PROVIDER = "usgs"
# SUPPORTED_PARAM_CODES = ["00065", "000061", "62614", "62615", "00045", "00010", "00011"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=4)).replace(minute=0, second=0),
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
    schedule="@hourly",
    max_active_runs=1,
    max_active_tasks=4,
    catchup=False,
    doc_md=__doc__,
)
def a2w_sync_usgs_timeseries_values():
    for state in US_STATES:
        with TaskGroup(group_id=f"{state}") as tg:

            @task(task_id=f"{state}_water_enabled_tsid_keys")
            def water_tsid_keys(state):

                water_hook = water.WaterHook(method="GET")
                tsids = water_hook.request(
                    endpoint=f"/timeseries?datatype={DATATYPE}&provider={PROVIDER}&state={state}&etl_values_enabled=true"
                )

                tsid_keys_asdict = {item["key"]: item for item in tsids}

                if len(tsid_keys_asdict) == 0:
                    raise AirflowSkipException(f"No etl enable timeseries for {state}")

                return tsid_keys_asdict

            @task(task_id=f"{state}_transform_keys_to_site_params")
            def tranform_keys_to_site_params(tsid_keys: list) -> dict:

                # Dictionary to store the site code as key
                # and the params codes as a list
                # Example: site_params[03184500] = [00065,00061]
                site_params = {}

                for key in tsid_keys:
                    site_number, param_code = key.split(".")

                    # If the site number already in the dict keys
                    if site_number in site_params.keys():
                        if param_code not in site_params[site_number]:
                            # add param to existing dict entry (if not exists)
                            site_params[site_number].append(param_code)
                    else:
                        # otherwise add new key with new param code
                        site_params[site_number] = [param_code]

                return site_params

            @task(task_id=f"{state}_extract_usgs_timeseries_values")
            def extract_usgs_timeseries_values(site_params: list) -> dict:

                # Task instance context
                context = get_current_context()
                ti = context["ti"]
                execution_date = ti.execution_date

                unique_param_codes = []
                for site_number, param_list in site_params.items():
                    for code in param_list:
                        if code not in unique_param_codes:
                            unique_param_codes.append(code)

                base_url = "https://waterservices.usgs.gov/nwis/iv/?"
                query_dict = {
                    "format": "json",
                    "sites": ",".join(site_params.keys()),
                    # "parameterCd": ",".join(unique_param_codes),
                    # 'period': 'PT2H',
                    # "modifiedSince": "P2D",
                    "siteType": "LK,ST",
                    "siteStatus": "active",
                }
                # This is added to the 'startDT'
                tw_delta = -timedelta(hours=2)

                # Set the execution date and time window for URL
                query_dict["startDT"] = (execution_date + tw_delta).isoformat()

                # Airflow only looks at the last period during an execution run,
                # so to ensure the latest data is retrieved, add 2 hours to end date
                query_dict["endDT"] = (execution_date + timedelta(hours=2)).isoformat()

                url = f"{base_url}{urlencode(query_dict)}"
                logging.info(unquote(url))

                # Make the request to USGS API
                r = requests.get(url)
                usgs_timeseries = r.json()["value"]["timeSeries"]

                # Parse results into a simplier dict to be returned
                site_results = {}

                for ts in usgs_timeseries:
                    _, site_number, param_code, _ = ts["name"].split(":")
                    # Look only at enabled param codes
                    if param_code in unique_param_codes:
                        site_results[f"{site_number}.{param_code}"] = ts["values"][0][
                            "value"
                        ]

                return site_results

            @task(task_id=f"{state}_load_timeseries_values")
            def load_timeseries_values(site_param_values: dict):

                payload = []

                for site_param, values in site_param_values.items():

                    payload_obj = {}
                    payload_obj["datatype"] = DATATYPE
                    payload_obj["provider"] = PROVIDER
                    payload_obj["key"] = site_param  # ex: 03185000.00060
                    payload_obj_values = []
                    for tv in values:
                        payload_obj_values.append([tv["dateTime"], tv["value"]])

                    payload_obj["values"] = payload_obj_values
                    # print(f"Payload -> {payload_obj}")

                    payload.append(payload_obj)

                # Make the POST request to water-api
                logging.info(f"POSTING {len(payload)} objects to water-api")

                water_hook = water.WaterHook(method="POST")
                resp = water_hook.request(
                    endpoint=f"/providers/{PROVIDER}/timeseries/values",
                    json=payload,
                )

                logging.info(f"{len(resp)} timeseries accepted by water-api")

            # --------------------------------------------------

            _water_tsid_keys = water_tsid_keys(state)
            _site_params = tranform_keys_to_site_params(_water_tsid_keys)
            _usgs_timeseries_values = extract_usgs_timeseries_values(_site_params)
            load_timeseries_values(_usgs_timeseries_values)


DAG_ = a2w_sync_usgs_timeseries_values()
