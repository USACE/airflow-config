from urllib.parse import urlsplit, urlunsplit
from airflow.models import Variable

import requests

url_parts = urlsplit(Variable.get("CWMSDATA"))

def api_request(subdirectory, query=None, fragment=None):
    url = urlunsplit(
        (url_parts.scheme, url_parts.netloc, f"{url_parts.path}/{subdirectory}", query, fragment)
    )
    r = requests.get(url)
    if r.status_code == 200:
        return r.text
