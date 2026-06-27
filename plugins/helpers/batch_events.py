import json
import logging
import os
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow.models import Variable

logger = logging.getLogger(__name__)


def normalize_office(office: str | None) -> str | None:
    if not office:
        return None
    value = office.strip().upper().replace("-", "_")
    return value or None


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


def get_optional_config(name: str) -> str | None:
    try:
        return get_config(name)
    except RuntimeError:
        return None


def parse_office_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [
        office
        for office in (normalize_office(part) for part in value.split(","))
        if office
    ]


def get_office_client_map() -> dict:
    raw_clients = get_optional_config("BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS")
    if not raw_clients:
        return {}

    try:
        clients = json.loads(raw_clients)
    except json.JSONDecodeError as exc:
        raise RuntimeError("BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS is not valid JSON") from exc
    if not isinstance(clients, dict):
        raise RuntimeError("BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS must be a JSON object")
    return clients


def get_office_client_config(office: str | None) -> dict[str, str]:
    office_key = normalize_office(office)
    if not office_key:
        return {}

    client_id = get_optional_config(f"BATCH_EVENTS_KEYCLOAK_CLIENT_ID_{office_key}")
    client_secret = get_optional_config(
        f"BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET_{office_key}"
    )
    if client_id and client_secret:
        return {"client_id": client_id, "client_secret": client_secret}

    clients = get_office_client_map()
    config = clients.get(office_key) or clients.get(office_key.lower())
    if not config:
        return {}
    if not isinstance(config, dict):
        raise RuntimeError(
            f"BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS.{office_key} must be an object"
        )

    client_id = config.get("client_id") or config.get("clientId")
    client_secret = config.get("client_secret") or config.get("clientSecret")
    if not client_id or not client_secret:
        raise RuntimeError(
            f"BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS.{office_key} must include client_id and client_secret"
        )

    return {"client_id": client_id, "client_secret": client_secret}


def get_scheduled_script_offices() -> list[str]:
    offices = parse_office_list(get_optional_config("BATCH_EVENTS_SCHEDULED_OFFICES"))
    if offices:
        return offices
    return [
        office
        for office in (normalize_office(key) for key in get_office_client_map())
        if office
    ]


def get_service_account_token(office: str | None = None) -> str:
    office_config = get_office_client_config(office)
    client_id = office_config.get("client_id")
    if not client_id:
        client_id = get_config("BATCH_EVENTS_KEYCLOAK_CLIENT_ID")
    client_secret = office_config.get("client_secret")
    if not client_secret:
        client_secret = get_config("BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET")

    token_url = get_config("BATCH_EVENTS_KEYCLOAK_TOKEN_URL")
    form = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
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


def trigger_job(script_id: str, office: str | None = None) -> dict:
    api_root = get_config("BATCH_EVENTS_API_ROOT").rstrip("/")
    token = get_service_account_token(office)
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


def get_scheduled_scripts_for_office(office: str | None = None) -> list[dict]:
    api_root = get_config("BATCH_EVENTS_API_ROOT").rstrip("/")
    token = get_service_account_token(office)
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


def get_scheduled_scripts(offices: list[str] | None = None) -> list[dict]:
    office_list = [
        office
        for office in (normalize_office(office) for office in (offices or []))
        if office
    ]
    if office_list:
        scripts_by_id = {}
        for office in office_list:
            try:
                office_scripts = get_scheduled_scripts_for_office(office)
            except Exception:
                if len(office_list) == 1:
                    raise
                logger.exception(
                    "Skipping scheduled scripts for office %s after Batch Events lookup failed",
                    office,
                )
                continue

            for script in office_scripts:
                scripts_by_id.setdefault(script["id"], script)
        return list(scripts_by_id.values())

    configured_offices = get_scheduled_script_offices()
    if configured_offices:
        return get_scheduled_scripts(configured_offices)

    return get_scheduled_scripts_for_office()


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
        for script in get_scheduled_scripts(offices)
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
    day_is_wildcard = day == "*"
    weekday_is_wildcard = weekday == "*"
    day_matches = _cron_field_matches(day, logical_date.day, 1, 31)
    weekday_matches = (
        _cron_field_matches(weekday, cron_weekday, 0, 7)
        or (cron_weekday == 0 and _cron_field_matches(weekday, 7, 0, 7))
    )

    if day_is_wildcard and weekday_is_wildcard:
        date_matches = True
    elif day_is_wildcard:
        date_matches = weekday_matches
    elif weekday_is_wildcard:
        date_matches = day_matches
    else:
        date_matches = day_matches or weekday_matches

    return (
        _cron_field_matches(minute, logical_date.minute, 0, 59)
        and _cron_field_matches(hour, logical_date.hour, 0, 23)
        and _cron_field_matches(month, logical_date.month, 1, 12)
        and date_matches
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
        ):
            try:
                if cron_matches(schedule_cron, logical_date):
                    due_scripts.append(script)
            except ValueError as exc:
                logger.warning(
                    "Skipping scheduled script %s because scheduleCron %r is invalid: %s",
                    script.get("id", "<unknown>"),
                    schedule_cron,
                    exc,
                )

    return due_scripts
