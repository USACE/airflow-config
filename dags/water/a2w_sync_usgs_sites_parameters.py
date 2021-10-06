"""
URL => https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state.lower()}&siteType=LK,ST&siteStatus=active
"""

import json
import requests

from airflow import DAG
from datetime import datetime, timedelta, timezone
from textwrap import dedent

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator

import helpers.water as water

implementation = {
    "stable": {
        "dag_id": "a2w_sync_usgs_site_parameters",
        "tags": ["a2w", "usgs"],
    },
    # 'develop': {
    #     'dag_id': 'develop_a2w_sync_usgs_site_parameters',
    #     'tags': ['develop', 'a2w', 'usgs'],
    # }
}

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
    # 'max_active_runs':1,
    # 'concurrency':4,
}


def create_dag(**kwargs):

    with DAG(
        default_args=default_args,
        dag_id=kwargs["dag_id"],
        tags=kwargs["tags"],
        schedule_interval=kwargs["schedule_interval"],
        doc_md=dedent(__doc__),
    ) as dag:
        ########################################
        def get_water_usgs_parameters():
            parameter_codes = json.loads(water.get_usgs_parameters())
            water_usgs_parameters = []
            for pc in parameter_codes:
                water_usgs_parameters.append(pc["code"])

            return json.dumps(water_usgs_parameters)

        ########################################
        def fetch_and_write(state_abbrev, water_usgs_parameters):

            water_usgs_parameters = json.loads(water_usgs_parameters)
            sites = json.loads(water.get_usgs_sites_by_state(state_abbrev))

            map = {}

            for s in sites:
                _p = {}
                for wp in water_usgs_parameters:
                    # Check all USGS param codes in WATER API
                    # and assigned True/False to sites params map
                    if wp in s["parameter_codes"]:
                        _p[wp] = True
                    else:
                        _p[wp] = False
                map[s["site_number"]] = _p

            insert_payload = []

            usgs_state_sites_url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state_abbrev.lower()}&siteType=LK,ST&siteStatus=active"
            r = requests.get(usgs_state_sites_url)
            usgs_state_sites = r.json()

            for i, state_site in enumerate(usgs_state_sites["value"]["timeSeries"]):
                # Note: Each site may be listed multiple times, once per parameter

                site_number = state_site["sourceInfo"]["siteCode"][0]["value"]
                # print(site_number)
                param_code = state_site["variable"]["variableCode"][0]["value"]
                # print(param_code)
                if site_number in map.keys():

                    if (
                        param_code in water_usgs_parameters
                        and not map[site_number][param_code]
                    ):
                        insert_payload.append(
                            {
                                "site_number": site_number,
                                "parameter_codes": [param_code],
                            }
                        )
                        # print('payload inserted')
                    # else:
                    #     print(" -- PARAM EXISTS --")
                else:
                    print(f"** USGS Site {site_number} not in Water API")

            # POST to the water API
            print(f"** INSERT PAYLOAD contained {len(insert_payload)} items.")

            # Post payload for current state
            if len(insert_payload) > 0:
                water.post_usgs_site_parameters(insert_payload)
            else:
                raise AirflowSkipException
            # print("Sending payload...")

            return

        ########################################

        # Build two DAGSs (develop & stable)
        # ----------------
        states = water.get_states()

        get_usgs_parameter_task_id = "get_water_usgs_parameters"
        get_water_usgs_parameters_task = PythonOperator(
            task_id=get_usgs_parameter_task_id,
            python_callable=get_water_usgs_parameters,
        )

        for state in states:
            fetch_write_task_id = f"fetch_and_write_{state}"
            write_site_parameters_task = PythonOperator(
                task_id=fetch_write_task_id,
                python_callable=fetch_and_write,
                op_kwargs={
                    "state_abbrev": state,
                    "water_usgs_parameters": "{{task_instance.xcom_pull(task_ids='get_water_usgs_parameters')}}",
                },
            )
            get_water_usgs_parameters_task >> write_site_parameters_task
        return dag
        # ----------------


# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val["dag_id"]
    d_tags = val["tags"]
    globals()[d_id] = create_dag(dag_id=d_id, tags=d_tags, schedule_interval="@daily")
