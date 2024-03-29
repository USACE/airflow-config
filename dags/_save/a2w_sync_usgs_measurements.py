"""
# Sync USGS Measurements to Water API

Available sites and their parameters selected from the Water API, grouped
by state, are used to acquire gauge measurements from the USGS waterservices
API.  List of 'wants' and list of 'supported' parameters are compared and
only those supported parameters are POSTed to the database

- Fetch (Extract) Water API watersheds and their USGS sites
- Build a tast for each state
- Translate the extracted sites by watershed
- Post results to Water API
"""
import json
from datetime import datetime, timedelta
from itertools import groupby
from operator import itemgetter
from typing import Dict
from urllib.parse import urlencode

import requests
from airflow import DAG, AirflowException
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from helpers import water

# Base url for the USGS Water Services and dictionary to build the query
# Dynamic query values added with DAG creation for Sites and Parameters
base_url = "https://waterservices.usgs.gov/nwis/iv/?"
query_dict = {
    "format": "json",
    # 'period': 'PT2H',
    # "modifiedSince": "P2D",
    "siteType": "LK,ST",
    "siteStatus": "active",
}
# This is added to the 'startDT'
tw_delta = -timedelta(hours=2)


# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=4)).replace(minute=0, second=0),
    # "start_date": datetime(2021, 11, 11),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# Generator producing time=value dictionaries
def measurement_series(name: str, java_json: json):
    for timeseries in java_json["value"]["timeSeries"]:
        if name == timeseries["name"]:
            for values in timeseries["values"]:
                for value in values["value"]:
                    value_ = value["value"]
                    dt = datetime.strptime(
                        (value["dateTime"]), "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                    yield {"time": dt.isoformat(), "value": float(value_)}


# Generator producing code=parameterCd and measurements=time/value list
# References the above generator
def parameter_measurements(url: str):
    with requests.get(
        url=url,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        stream=True,
    ) as req:

        if req.status_code != 200:
            raise Exception
        java_json = req.json()
        # Loop through timeseries
        for timeseries in java_json["value"]["timeSeries"]:
            name = timeseries["name"]
            _, siteCd, parameterCd, _ = name.split(":")
            code_measurements = {
                "site": siteCd,
                "code": parameterCd,
                "measurements": [m for m in measurement_series(name, java_json)],
            }
            yield code_measurements


with DAG(
    dag_id="a2w_sync_usgs_measurements",
    default_args=default_args,
    tags=["a2w", "usgs", "measurements"],
    schedule="15 */1 * * *",
    max_active_runs=2,
    max_active_tasks=3,
    doc_md=__doc__,
) as dag:
    """Method defining the DAG
    Tasks will be defined withing the scope of this method"""

    @task
    def fetch_watershed_usgs_sites():
        return {
            st["state_abbrev"].upper(): st["sites"]
            for st in water.watersheds_usgs_sites()
        }

    _fetch_watershed_usgs_sites = fetch_watershed_usgs_sites()

    # Loop through the list of states creating a task per state
    for state in water.get_states():

        @task(task_id=f"fetch_parse_post_{state}")
        def fetch_parse_post(sites_by_state: Dict, state: str):
            # Task instance context
            context = get_current_context()
            ti = context["ti"]
            execution_date = ti.execution_date

            # KeyError if state not in dictionary and will raise a skip exception
            state = state.upper()
            try:
                sites = sites_by_state[state]
            except KeyError:
                raise AirflowSkipException

            # Site number to parameter codes dictionary
            site_number_codes = {
                site["site_number"]: site["parameter_codes"] for site in sites
            }
            # List of all site numbers for the URL
            site_numbers = [str(site["site_number"]) for site in sites]
            # List of all parameters codes
            all_parameter_codes = [site["parameter_codes"] for site in sites]
            # Flatten list of all parameter codes with no duplicates for URL
            parameter_codes = list(
                set([str(item) for sublist in all_parameter_codes for item in sublist])
            )
            # Set the execution date and time window for URL
            query_dict["startDT"] = (execution_date + tw_delta).isoformat()
            # Airflow only looks at the last period during an execution run,
            # so to ensure the latest data is retrieved, add 1 hour to end date
            query_dict["endDT"] = (execution_date + timedelta(hours=1)).isoformat()
            query_dict["sites"] = ",".join(site_numbers)
            query_dict["parameterCd"] = ",".join(parameter_codes)
            url = f"{base_url}{urlencode(query_dict)}"
            print(f"{url=}")

            # Use the generators to parse and translate the sites, parameters, and measurements
            error = None
            for site, param_measures in groupby(
                parameter_measurements(url), key=itemgetter("site")
            ):
                payload = list()
                for s in param_measures:
                    if s["code"] in site_number_codes[site]:
                        s.pop("site", None)
                        payload.append(s)

                # POST to the endpoint
                error = water.post_usgs_measurements(site=site, payload=payload)
            # Let each payload try to POST and raise an exception if any one failed after all tried
            if isinstance(error, AirflowException):
                raise AirflowException

        fetch_parse_post(_fetch_watershed_usgs_sites, state)
