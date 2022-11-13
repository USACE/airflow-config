import json
from typing import List
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.http.hooks.http import HttpHook

S3_BUCKET = Variable.get("S3_BUCKET")


class WaterHook(HttpHook):
    """
    method: str = 'POST'
    http_conn_id: str = default_conn_name
    auth_type: Any = HTTPBasicAuth
    tcp_keep_alive: bool = True
    tcp_keep_alive_idle: int = 120
    tcp_keep_alive_count: int = 20
    tcp_keep_alive_interval: int = 30
    """

    def __init__(self, *args, **kw):

        self.args = args
        self.kw = kw

        self.conn_name = "WATER"
        self.conn = BaseHook.get_connection(self.conn_name)

        self.response_type = "json"

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.kw["http_conn_id"] = self.conn.conn_id

        super().__init__(*args, **kw)

    def request(self, *args, **kw):
        """
        endpoint: str | None = None
        data: Dict[str, Any] | str | None = None
        headers: Dict[str, Any] | None = None
        extra_options: Dict[str, Any] | None = None
        **request_kwargs: Any
        """


        if self.kw["method"] in ["POST", "PUT"]:
            if len(args) >= 1:
                args = list(args)
                args[0] += f"?key={self.conn.password}"
            if "endpoint" in kw:
                kw["endpoint"] += f"?key={self.conn.password}"

        resp = self.run(*args, **kw)
        if resp.status_code in [200, 201, 202]:
            if self.response_type == "json":
                return resp.json()
            elif self.response_type == "text":
                return resp.text


def get_connection():
    return BaseHook.get_connection("WATER")


def a2w_post_method(endpoint, headers, payload, json=True):
    try:
        conn = get_connection()
        ep = "/" + endpoint if not endpoint.startswith("/") else endpoint
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        resp = h.run(
            endpoint=ep + f"?key={conn.password}", headers=headers, json=payload
        )
        return resp.text
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise


def a2w_get_method(endpoint, headers, json=False):
    try:
        conn = get_connection()

        ep = "/" + endpoint if not endpoint.startswith("/") else endpoint

        h = HttpHook(http_conn_id=conn.conn_id, method="GET")
        resp = h.run(endpoint=ep, headers=headers)

        if resp.status_code == 200:
            if json:
                return resp.json()
            else:
                return resp.text
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        raise


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
        return r.text
    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        print(json.dumps(payload))
        raise


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


def get_location_kind(kind=None):

    # Offices endpoint returns a list of objects
    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = "/location_kind"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    # Don't bother converting the string to list or obj, airflow will
    # convert to a string to pass across xcomms
    if kind is None:
        return r.text
    else:
        location_kind = r.json()
        kind_id = {item["name"]: item["id"] for item in location_kind}
        return kind_id[kind]


def get_locations(office_id=None, kind_id=None):

    kind_str = ""
    if kind_id is not None:
        kind_str = f"&kind_id={kind_id}"

    office_str = ""
    if office_id is not None:
        office_str = f"&office_id={office_id}"

    conn = get_connection()
    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = f"/locations?{office_str}{kind_str}"
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


def get_cwms_timeseries(
    provider, datasource_type="cwms-timeseries", mapped=0, query=""
):

    conn = get_connection()

    h = HttpHook(http_conn_id=conn.conn_id, method="GET")
    endpoint = f"/timeseries?provider={provider}&datasource_type={datasource_type}&only_mapped={mapped}&q={query}&key={conn.password}"
    headers = {"Content-Type": "application/json"}
    r = h.run(endpoint=endpoint, headers=headers)

    return r.json()


def post_cwms_timeseries_measurements(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/timeseries/measurements?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != "202":
            print(f"Expected status code 202, received {r.status_code}")
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        print(payload)
        raise

    return


def post_cwms_timeseries(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/timeseries?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != "201":
            print(f"Expected status code 201, received {r.status_code}")
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        print(payload)
        raise

    return


def post_cwms_timeseries_measurements(payload):

    conn = get_connection()
    try:
        h = HttpHook(http_conn_id=conn.conn_id, method="POST")
        endpoint = f"/timeseries/measurements?key={conn.password}"
        headers = {"Content-Type": "application/json"}
        r = h.run(endpoint=endpoint, json=payload, headers=headers)
        if r.status_code != "201":
            print(f"Expected status code 201, received {r.status_code}")
            print(r.text)

    except AirflowException as error:
        print(f"Airflow Exception: {error}")
        print(payload)
        raise

    return
