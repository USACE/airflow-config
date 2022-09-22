from dataclasses import dataclass, field
import json
import re
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import AirflowException
from airflow import settings
from airflow.models import Connection

RADAR_API_ROOT = "http://cwms-data.usace.army.mil/cwms-data"


def get_connection():
    conn = Connection(
        conn_id="RADAR",
        conn_type="http",
        host="https://cwms-data.usace.army.mil/cwms-data",
        login="nologin",
        password="nopass",
        port=None,
    )
    # create a connection object
    session = settings.Session()  # get the session

    # Check to see if connection already exists
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    if str(conn_name) == str(conn.conn_id):
        # return existing connection
        return BaseHook.get_connection("RADAR")

    # otherwise connection doesn't exist, so let's add it
    session.add(conn)
    session.commit()
    return conn


def get_timeseries(tsids: list, begin: str, end: str, office=None):
    if len(tsids) == 0 or begin is None or end is None:
        raise AirflowException("Invalid parameters provided for function.")
    conn = get_connection()
    tsid_str = "|".join(tsids)

    if office is not None:
        office_str = f"&office={office}"
    else:
        office_str = ""

    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = f"/timeseries?name={tsid_str}{office_str}&begin={begin}&end={end}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    return r.text


@dataclass
class Office:
    id: str
    name: str
    symbol: str
    parent_id: str


@dataclass
class Location:
    office_id: str = None
    name: str = None
    public_name: str = None
    kind_id: str = None
    kind: str = None


@dataclass
class Geometry:
    type: str = "Point"
    latitude: float = 0
    longitude: float = 0


@dataclass
class Political:
    nation: str = None
    state: str = None
    county: str = None
    timezone: str = None
    nearest_city: str = None
    bounding_office: str = None


def check_service():
    headers = {"Content-Type": "application/json", "accept": "*/*"}
    r = requests.get(url=f"{RADAR_API_ROOT}/offices", headers=headers)
    if r.status_code == 200:
        return True
    else:
        raise AirflowException("RADAR API SERVICE DOWN!")


##############################################################
