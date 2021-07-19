"""
This pipeline handles location sync between CWMS RADAR API and A2W Locations.\n
Info: http://cwms-data.usace.army.mil/cwms-data/locations
"""

import re
import json
from typing import Any, Dict, List
import requests
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow import AirflowException
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import task, get_current_context

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import helpers.sharedApi as sharedApi
import helpers.radar as radar
import helpers.water as water



# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}
with DAG(
    'develop_a2w_location_sync',
    default_args=default_args,
    description='A2W Location Sync',
    # start_date=(datetime.utcnow()-timedelta(hours=72)).replace(minute=0, second=0),
    start_date=(datetime.utcnow()-timedelta(hours=2)).replace(minute=0, second=0),
    tags=['a2w', 'develop'],    
    schedule_interval='45 * * * *',
    # schedule_interval='@hourly',
    catchup=False

) as dag:
    dag.doc_md = dedent(__doc__)

    offices = sharedApi.get_offices()
    # Convert the returned string to an object
    offices = json.loads(offices)
    # Get location kind from water-api
    location_kinds = json.loads(water.get_location_kind())

    def get_radar_locations(office: str) -> str:
        print(f'Getting data for {office}')

        # Make a call to RADAR API for single office
        url = f'{radar.RADAR_API_ROOT}/locations?office={office}&name=@&format=json'
        print(f'Requesting URL: {url}')
        r = requests.get(url)
        resp_text = r.text
        payload = resp_text.translate(resp_text.maketrans("", "", "\n\r\t"))
        payload = payload.replace("None", "")

        return payload

    def post_a2w_locations(office: radar.Office, payload: str):
        print(f'******** posting data for {office.symbol} ********')

        payload_json = json.loads(payload)

        locations = payload_json["locations"]["locations"]

        post_locations = list()
        for location in locations:
            # Get the kind_id from the location-kind list
            kind_id = [
                kind["id"]
                for kind in location_kinds
                if kind["name"] == location["classification"]["location-kind"]
            ]
            loc = radar.Location(
                office_id = office.id,
                name = location["identity"]["name"],
                public_name = location["label"]["public-name"],
                kind_id = kind_id[0]
            )
            geo = radar.Geometry()
            lat = location["geolocation"]["latitude"]
            lon  = location["geolocation"]["longitude"]
            if isinstance(lat, float): geo.latitude = lat
            if isinstance(lon, float): geo.longitude = lon

            post_locations.append(
                {
                    "office_id": loc.office_id,
                    "name": loc.name,
                    "public_name": loc.public_name,
                    "kind_id": loc.kind_id,
                    "geometry" : {
                        "type": geo.type,
                        "coordinates": [
                            geo.latitude,
                            geo.longitude
                        ]
                    }
                }
            )
        water.post_locations(
            payload=post_locations,
            conn_type="develop"
        )

    check_radar_task = PythonOperator(
        task_id='check_radar_service', 
        python_callable=radar.check_service
        )

    # Commented out due to error being produced in airflow
    # for office in offices:
    #     _office = radar.Office(**office)

    #     if _office.symbol is not None:

    #         # Build dymanic task name
    #         fetch_office_task_id = f"get_radar_locations_{_office.symbol}"

    #         fetch_task = PythonOperator(
    #         task_id=fetch_office_task_id, 
    #         python_callable=get_radar_locations, 
    #         op_kwargs={
    #             'office': _office.symbol,
    #         })

    #         # Build dymanic task name
    #         post_office_task_id = f"post_a2w_locations_{_office.symbol}"

    #         post_task = PythonOperator(
    #         task_id=post_office_task_id, 
    #         python_callable=post_a2w_locations, 
    #         op_kwargs={
    #             'office': _office,
    #             'payload': "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(fetch_office_task_id)
    #         })

    #         # Call the tasks
    #         check_radar_task >> fetch_task >> post_task