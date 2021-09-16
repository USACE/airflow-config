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
def get_states():
    return ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

def post_location(payload, conn_type: str):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/locations?key={conn.password}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(r.status_code)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return

def update_radar_locations(id, payload, conn_type: str='develop'):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='PUT')
        endpoint = f"/locations/{id}?key={conn.password}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(r.status_code)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

def sync_radar_locations(payload, conn_type: str='develop'):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/sync/locations?key={conn.password}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(r.status_code)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise
    


def sync_usgs_sites(payload, conn_type: str):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/usgs/sync/sites?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(f'Response Code: {r.status_code}')
        print(r.text)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return

def get_location_kind(conn_type='develop'):
    # Offices endpoint returns a list of objects
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    endpoint = '/location_kind'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)
    
    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text

def get_location_office_id(office_id, conn_type='develop'):
    # Offices endpoint returns a list of objects
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    endpoint = f'/locations?office_id={office_id}'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return (r.status_code, r.json())

def get_locations(conn_type='develop'):
    # Offices endpoint returns a list of objects
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    endpoint = f'/locations'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)
    
    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.json()

# Unique list of USGS parameter codes and descriptions
# stores in the Water DB
def get_usgs_parameters(conn_type='develop'):
    # Parameters endpoint returns a list of objects
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    endpoint = '/usgs/parameters'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)
    
    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text

def get_usgs_sites_by_state(state, conn_type='develop'):
    # Offices endpoint returns a list of objects
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    if state == 'all':
        endpoint = f'/usgs/sites'
    else:
        endpoint = f'/usgs/sites?state={state}'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)
    
    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text

def post_usgs_site_parameters(payload, conn_type='develop'):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/usgs/site_parameters?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(f'Response Code: {r.status_code}')
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return

def get_nws_stages_by_site(site, conn_type='develop'):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method='GET')
    if site == 'all':
        endpoint = f'/nws/stages'
    else:
        endpoint = f'/nws/stages/{site}'
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text

def post_nws_stages(payload, conn_type='develop'):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='POST')
        endpoint = f"/nws/stages?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != '201':
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")        
        raise

    return

def put_nws_stages(site, payload, conn_type='develop'):
    if conn_type.lower() == 'develop':
        conn = get_develop_connection()
    else:
        conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method='PUT')
        endpoint = f"/nws/stages/{site}?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != '200':
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return
