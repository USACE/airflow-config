from urllib.parse import urlsplit, urlunsplit
from airflow import AirflowException
import requests

url_parts = urlsplit("https://cwms-data.usace.army.mil/cwms-data")


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
    r = requests.get(url)
    if r.status_code == 200:
        return r.text


def get_timeseries(tsids: list, begin: str, end: str, office=None):
    if len(tsids) == 0 or begin is None or end is None:
        raise AirflowException("Invalid parameters provided for function.")
    tsid_str = "|".join(tsids)

    if office is not None:
        office_str = f"&office={office}"
    else:
        office_str = ""

    return api_request(
        "timeseries", f"name={tsid_str}{office_str}&begin={begin}&end={end}"
    )
