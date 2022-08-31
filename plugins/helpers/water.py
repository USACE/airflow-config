from typing import List
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException
from airflow.models import Variable

S3_BUCKET = Variable.get("S3_BUCKET")


def get_connection():
    return BaseHook.get_connection("WATER")


def get_offices():

    # Offices endpoint returns a list of objects
    conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = "/offices"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text


def watersheds_usgs_sites():
    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="GET")
        endpoint = f"/watersheds/usgs_sites"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        r = h.run(endpoint=endpoint, headers=headers)
        return r.json()
    except AirflowException as err:
        print(f"Airflow Exception: {err}")
        raise


def get_states():
    # fmt: off
    return ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
    # fmt: on


def post_location(payload):
    conn = get_connection()

    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
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


def update_radar_locations(id, payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="PUT")
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

    return


def sync_radar_locations(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
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

    return


def sync_usgs_sites(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/usgs/sync/sites?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(f"Response Code: {r.status_code}")
        print(r.text)
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return


def get_location_kind():

    # Offices endpoint returns a list of objects
    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = "/location_kind"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text


def get_location_office_id(office_id):

    # Offices endpoint returns a list of objects
    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = f"/locations?office_id={office_id}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return (r.status_code, r.json())


def get_locations():

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = f"/locations"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.json()


# Unique list of USGS parameter codes and descriptions
# stores in the Water DB
def get_usgs_parameters():

    # Parameters endpoint returns a list of objects
    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = "/usgs/parameters"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text


def get_usgs_sites_by_state(state):

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    if state == "all":
        endpoint = f"/usgs/sites"
    else:
        endpoint = f"/usgs/sites?state={state}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text


def post_usgs_site_parameters(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/usgs/site_parameters?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        print(f"Response Code: {r.status_code}")
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return


def post_usgs_measurements(site: str, payload: List):
    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/usgs/sites/{site}/measurements?key={conn.password}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        return r.text
    except AirflowException as err:
        print(f"Airflow Exception: {err}")
        return err


def get_nws_stages_by_site(site):

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    if site == "all":
        endpoint = f"/nws/stages"
    else:
        endpoint = f"/nws/stages/{site}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text


def post_nws_stages(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/nws/stages?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != "201":
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return


def put_nws_stages(site, payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="PUT")
        endpoint = f"/nws/stages/{site}?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != "200":
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise

    return
