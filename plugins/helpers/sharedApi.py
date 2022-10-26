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


def get_static_offices():
    # fmt: off
    return [
        "LRB", "LRC", "LRD", "LRE", "LRH", "LRL", "LRN", "LRP", 
        "MVD", "MVK", "MVM", "MVN", "MVP", "MVR", "MVS", 
        "NAB", "NAD", "NAE", "NAN", "NAO", "NAP",
        "NWD", "NWK", "NWO", "NWP", "NWS", "NWW", 
        "POA", "POD", "POH",
        "SAC", "SAD", "SAJ", "SAM", "SAS", "SAW",
        "SPA", "SPD", "SPK", "SPL", "SPN", 
        "SWD", "SWF", "SWG", "SWL", "SWT"
        ]
    # fmt: on


def get_nwd_group(office):

    nwd_map = {
        "NWK": "NWDM",
        "NWO": "NWDM",
        "NWP": "NWDP",
        "NWS": "NWDP",
        "NWW": "NWDP",
        "NWD": "NWD",
    }

    if office.upper() in nwd_map.keys():
        return nwd_map[office]
