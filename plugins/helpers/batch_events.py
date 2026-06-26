import json
import os
from datetime import datetime
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


def get_scheduled_scripts() -> list[dict]:
    api_root = get_config("BATCH_EVENTS_API_ROOT").rstrip("/")
    token = get_service_account_token()
    request = Request(
        f"{api_root}/scripts/scheduled",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def scheduled_scripts_for_offices(
    schedule_type: str,
    offices: list[str] | None = None,
) -> list[dict]:
    office_filter = {
        office.strip().upper()
        for office in (offices or [])
        if office and office.strip()
    }
    return [
        script
        for script in get_scheduled_scripts()
        if script.get("scheduleEnabled")
        and script.get("scheduleType") == schedule_type
        and (
            not office_filter
            or str(script.get("office", "")).upper() in office_filter
        )
    ]


def scripts_due_at_minute(minute: int) -> list[dict]:
    return [
        script
        for script in get_scheduled_scripts()
        if script.get("scheduleEnabled")
        and script.get("scheduleType") == "hourly"
        and script.get("scheduleMinute") == minute
    ]


def _cron_field_matches(field: str, value: int, minimum: int, maximum: int) -> bool:
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue

        step = 1
        if "/" in part:
            part, step_value = part.split("/", 1)
            step = int(step_value)
            if step < 1:
                raise ValueError("Cron step must be at least 1")

        if part == "*":
            start, end = minimum, maximum
        elif "-" in part:
            start_value, end_value = part.split("-", 1)
            start, end = int(start_value), int(end_value)
        else:
            start = end = int(part)

        if start < minimum or end > maximum or start > end:
            raise ValueError(f"Cron field value out of range: {field}")

        if start <= value <= end and (value - start) % step == 0:
            return True

    return False


def cron_matches(expression: str, logical_date: datetime) -> bool:
    minute, hour, day, month, weekday = expression.split()
    # Python weekday is Monday=0; cron weekday commonly treats Sunday as 0 or 7.
    cron_weekday = (logical_date.weekday() + 1) % 7

    return (
        _cron_field_matches(minute, logical_date.minute, 0, 59)
        and _cron_field_matches(hour, logical_date.hour, 0, 23)
        and _cron_field_matches(day, logical_date.day, 1, 31)
        and _cron_field_matches(month, logical_date.month, 1, 12)
        and (
            _cron_field_matches(weekday, cron_weekday, 0, 7)
            or (cron_weekday == 0 and _cron_field_matches(weekday, 7, 0, 7))
        )
    )


def scripts_due_at(logical_date: datetime) -> list[dict]:
    due_scripts = []
    for script in get_scheduled_scripts():
        if not script.get("scheduleEnabled"):
            continue

        schedule_type = script.get("scheduleType")
        if (
            schedule_type == "hourly"
            and script.get("scheduleMinute") == logical_date.minute
        ):
            due_scripts.append(script)
            continue

        schedule_cron = script.get("scheduleCron")
        if (
            schedule_type == "cron"
            and schedule_cron
            and cron_matches(schedule_cron, logical_date)
        ):
            due_scripts.append(script)

    return due_scripts
