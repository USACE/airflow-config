from urllib.parse import urlsplit, urlunsplit
from airflow import AirflowException
import requests


url_parts = urlsplit("https://cwms-data.usace.army.mil/cwms-data")


class RadarHook(requests.Session):
    """ """

    __attrs__ = ["response_type"]

    def __init__(self, *args, **kw):

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        self.response_type = "json"

        super().__init__(*args, **kw)

    def request_(self, *args, **kw):
        resp = self.request(*args, **kw)
        if resp.status_code in [200, 201, 202]:
            if self.response_type == "json":
                return resp.json()
            elif self.response_type == "text":
                return resp.text


def radar_request(uri, query=None, fragment=None):
    url_parts = urlsplit(uri)
    url = urlunsplit(
        (
            url_parts.scheme,
            url_parts.netloc,
            url_parts.path,
            query,
            fragment,
        )
    )
    r = requests.get(url=url, timeout=90)
    return r


def api_request(subdirectory, query=None, fragment=None):
    url = urlunsplit(
        (
            url_parts.scheme,
            url_parts.netloc,
            f"{url_parts.path}/{subdirectory}",
            query,
            fragment,
        )
    )
    try:
        r = requests.get(url=url, timeout=90)
        if r.status_code == 200:
            return r.text
    except AirflowException as err:
        print(f"Airflow Exception: {err}")
        raise
    finally:
        if r is not None:
            r.close()
            r = None
