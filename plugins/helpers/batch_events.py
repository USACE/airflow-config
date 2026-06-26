import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow.models import Variable


def get_config(name: str, default: str | None = None) -> str:
    value = Variable.get(name, default_var=None)
    if value:
        return value

    value = os.environ.get(name)
    if value:
        return value

    env_var_name = f"AIRFLOW_VAR_{name}"
    value = os.environ.get(env_var_name)
    if value:
        return value

    if default is not None:
        return default

    raise RuntimeError(f"Missing Airflow configuration value: {name}")


def get_service_account_token() -> str:
    token_url = get_config("BATCH_EVENTS_KEYCLOAK_TOKEN_URL")
    form = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": get_config("BATCH_EVENTS_KEYCLOAK_CLIENT_ID"),
            "client_secret": get_config("BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET"),
            "scope": get_config("BATCH_EVENTS_KEYCLOAK_SCOPE", "openid profile"),
        }
    ).encode("utf-8")
    request = Request(
        token_url,
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    token_host_header = get_config("BATCH_EVENTS_KEYCLOAK_TOKEN_HOST_HEADER", "")
    if token_host_header:
        request.add_header("Host", token_host_header)
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))["access_token"]


def trigger_job(script_id: str) -> dict:
    api_root = get_config("BATCH_EVENTS_API_ROOT").rstrip("/")
    token = get_service_account_token()
    body = json.dumps({"scriptId": script_id}).encode("utf-8")
    request = Request(
        f"{api_root}/jobs",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))
