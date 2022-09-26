"""
# Sync A2W NWS Stages by State

- Get NWS Stages

    - [AHPS Report](https://water.weather.gov/monitor/ahpsreport.php?type=csv)

    - Returns the locations and stages from the AHAPS report as a list of objects


- Get USGS sites from Water API

    - Returns the existing USGS stage numbers in the Water API


- Sync NWS Stages to Water

    - Build the payload and post to Water API

"""
import csv
import json
from datetime import datetime, timedelta
from io import StringIO
from textwrap import dedent

import helpers.water as water
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "schedule_interval": "@daily"
    # 'max_active_runs':1,
    # 'concurrency':4,
}


with DAG(
    default_args=default_args,
    dag_id="a2w_sync_nws_stages",
    tags=["a2w", "nws"],
    doc_md=dedent(__doc__),
) as dag:

    ########################################
    # Returns the Locations and Stages from the NWS as a list of objects
    def get_nws_stages():

        url = "https://water.weather.gov/monitor/ahpsreport.php?type=csv"

        r = requests.get(url)
        buff = StringIO(r.text)
        reader = csv.reader(buff, delimiter=",")

        keys = []
        result = []

        # Convert the csv to a list of json objects
        # with headers as keys
        for idx, line in enumerate(reader):
            if idx == 0:
                # this is the header
                keys = line
            else:
                # Build each line (object) by setting the keys and values
                _line = {}
                for i, k in enumerate(keys):
                    _line[k] = line[i].strip()
                result.append(_line)

        return json.dumps(result)

    ########################################
    # Returns the existing USGS Stages Numbers in the Water API
    def get_water_usgs_site_numbers():

        sites = json.loads(water.get_usgs_sites_by_state(state="all"))

        result_sites = []
        for site in sites:
            result_sites.append(site["site_number"])

        return json.dumps(result_sites)

    ########################################
    # Returns the existing NWS Stages in the Water API
    def get_water_nws_stages():

        stages = json.loads(water.get_nws_stages_by_site(site="all"))

        existing_stages = {}
        for site_stages in stages:
            existing_stages[site_stages["nwsid"]] = site_stages

        return json.dumps(existing_stages)

    ########################################
    def sync_nws_stages_to_water(
        state, nws_stages, water_nws_stages, water_usgs_site_numbers
    ):

        water_usgs_site_numbers = json.loads(water_usgs_site_numbers)
        water_nws_stages = json.loads(water_nws_stages)

        post_items = []
        put_items = []

        for idx, line in enumerate(json.loads(nws_stages)):

            if line["state"].strip() == state.lower():

                # Entry must have:
                # 1) USGSID must be in API sites
                # 2) NWSID = 5 chars
                # 3) a proper USGS ID
                # 4) stage must have a unit of 'FT
                # 5) all stages cannot be 0
                if (
                    line["usgs id"].strip() in water_usgs_site_numbers
                    and len(line["nws shef id"].strip()) == 5
                    and line["usgs id"].strip() != ""
                    and len(line["usgs id"].strip()) >= 8
                    and line["usgs id"].strip().isnumeric()
                    and line["flood stage unit"].strip() == "FT"
                    and float(line["action stage"].strip()) != 0
                    and float(line["flood stage"].strip()) != 0
                    and float(line["moderate flood stage"].strip()) != 0
                    and float(line["major flood stage"].strip()) != 0
                ):

                    if line["proximity"].strip() in ["at", "near", "below", "near"]:
                        # ex: Name above xyz lake
                        name = f"{line['location name'].strip()} {line['proximity'].strip()} {line['river/water-body name'].strip()}"
                    else:
                        name = line["location name"].strip()

                    nwsid = line["nws shef id"].strip()
                    usgs_id = line["usgs id"].strip()
                    action = float(line["action stage"].strip())
                    flood = float(line["flood stage"].strip())
                    moderate_flood = float(line["moderate flood stage"].strip())
                    major_flood = float(line["major flood stage"].strip())

                    # Build payload
                    payload = {}
                    payload["nwsid"] = nwsid
                    payload["usgs_site_number"] = usgs_id
                    payload["name"] = name
                    payload["action"] = action
                    payload["flood"] = flood
                    payload["moderate_flood"] = moderate_flood
                    payload["major_flood"] = major_flood

                    # API Post Payload
                    if nwsid in water_nws_stages.keys():
                        # site stages already in database, test for changes, if changes use put
                        print(
                            f"NWS site {nwsid} already in DB. Checking for changes..."
                        )
                        if (
                            water_nws_stages[nwsid]["action"] != action
                            or water_nws_stages[nwsid]["flood"] != flood
                            or water_nws_stages[nwsid]["moderate_flood"]
                            != moderate_flood
                            or water_nws_stages[nwsid]["major_flood"] != major_flood
                        ):
                            water.put_nws_stages(nwsid, payload)
                            put_items.append(nwsid)
                    else:
                        # new site stages record, use POST
                        try:
                            # POST single site stages to the water API
                            water.post_nws_stages(payload)
                            post_items.append(nwsid)
                        except:
                            continue

        # Show task as skipped if no work done
        if len(put_items) == 0 and len(post_items) == 0:
            raise AirflowSkipException

        return

    ########################################

    # Build two DAGSs
    # ----------------
    states = water.get_states()

    get_water_usgs_sites_task = PythonOperator(
        task_id="get_water_usgs_sites", python_callable=get_water_usgs_site_numbers
    )

    get_nws_stages_task = PythonOperator(
        task_id="get_nws_stages", python_callable=get_nws_stages
    )

    get_water_nws_stages_task = PythonOperator(
        task_id="get_water_nws_stages", python_callable=get_water_nws_stages
    )

    for state in states:
        sync_task_id = f"sync_nws_stages_{state}"
        sync_nws_stages_task = PythonOperator(
            task_id=sync_task_id,
            python_callable=sync_nws_stages_to_water,
            op_kwargs={
                "state": state,
                "nws_stages": "{{task_instance.xcom_pull(task_ids='get_nws_stages')}}",
                "water_nws_stages": "{{task_instance.xcom_pull(task_ids='get_water_nws_stages')}}",
                "water_usgs_site_numbers": "{{task_instance.xcom_pull(task_ids='get_water_usgs_sites')}}",
            },
        )

        (
            get_nws_stages_task
            >> get_water_usgs_sites_task
            >> get_water_nws_stages_task
            >> sync_nws_stages_task
        )
