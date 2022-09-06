import json
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException


def get_connection():
    return BaseHook.get_connection("SHARED")


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
