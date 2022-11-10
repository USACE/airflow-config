from urllib.parse import urlsplit, urlunsplit
from airflow import AirflowException
import requests
import logging

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
    try:
        logging.info(url)
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


def get_levels(tsids: list, office=None):
    if len(tsids) == 0:
        raise AirflowException("Invalid parameters provided for function.")
    tsid_str = "|".join(tsids)

    if office is not None:
        office_str = f"&office={office}"
    else:
        office_str = ""

    return api_request("levels", f"name={tsid_str}{office_str}")
