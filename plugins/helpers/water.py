import json
import requests
from typing import Dict, List
from airflow.hooks.base_hook import BaseHook
from sqlalchemy.util.langhelpers import public_factory
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException

WATER_API_ROOT = "https://develop-water-api.rsgis.dev"

################################################################ 
def get_develop_connection():    
    return BaseHook.get_connection('WATER_DEVELOP')
################################################################ 
def get_connection():    
    return BaseHook.get_connection('WATER_STABLE')
################################################################ 

def post_locations(payload, conn_type: str):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/sync/locations?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(r.status_code)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return

def sync_usgs_sites(payload, conn_type: str):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/sync/usgs_sites?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(r.status_code)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return

def get_location_kind() -> str:
    r = requests.get(WATER_API_ROOT + "/location_kind")
    if r.status_code == 200:
        return r.text
    else:
        raise AirflowException
