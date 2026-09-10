import json
import logging
import os
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from airflow.models import Variable

logger = logging.getLogger(__name__)
DEFAULT_SCHEDULE_TIMEZONE = "UTC"


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
    weekday_matches = _cron_field_matches(weekday, cron_weekday, 0, 7) or (
        cron_weekday == 0 and _cron_field_matches(weekday, 7, 0, 7)
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


def script_schedule_datetime(script: dict, logical_date: datetime) -> datetime:
    timezone_name = (
        script.get("scheduleTimezone") or DEFAULT_SCHEDULE_TIMEZONE
    ).strip()
    try:
        schedule_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid scheduleTimezone {timezone_name!r}") from exc

    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)

    # Airflow ticks in UTC; each registry schedule is evaluated in the script's
    # selected timezone to preserve office-local schedule semantics.
    return logical_date.astimezone(schedule_timezone)


def scripts_due_at(logical_date: datetime) -> list[dict]:
    due_scripts = []
    for script in get_scheduled_scripts():
        if not script.get("scheduleEnabled"):
            continue

        try:
            schedule_date = script_schedule_datetime(script, logical_date)
        except ValueError as exc:
            logger.warning(
                "Skipping scheduled script %s because scheduleTimezone %r is invalid: %s",
                script.get("id", "<unknown>"),
                script.get("scheduleTimezone"),
                exc,
            )
            continue

        if getattr(schedule_date, "fold", 0) == 1:
            continue

        schedule_type = script.get("scheduleType")
        if (
            schedule_type == "hourly"
            and script.get("scheduleMinute") == schedule_date.minute
        ):
            due_scripts.append(script)
            continue

        schedule_cron = script.get("scheduleCron")
        if schedule_type == "cron" and schedule_cron:
            try:
                if cron_matches(schedule_cron, schedule_date):
                    due_scripts.append(script)
            except ValueError as exc:
                logger.warning(
                    "Skipping scheduled script %s because scheduleCron %r is invalid: %s",
                    script.get("id", "<unknown>"),
                    schedule_cron,
                    exc,
                )

    return due_scripts


def get_scheduled_script_offices() -> list[str]:
    return parse_office_list(get_optional_config("BATCH_EVENTS_SCHEDULED_OFFICES"))


def get_api_key(office: str | None = None) -> str:
    office_key = normalize_office(office)
    if office_key:
        key = get_optional_config(f"BATCH_EVENTS_API_KEY_{office_key}")
        if key:
            return key
    return get_config("BATCH_EVENTS_API_KEY")


def api_request(path: str, office: str | None = None, payload: dict | None = None):
    api_root = get_config("BATCH_EVENTS_API_ROOT").rstrip("/")
    request = Request(
        f"{api_root}/{path}",
        data=None if payload is None else json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"apikey {get_api_key(office)}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="GET" if payload is None else "POST",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def get_scheduled_scripts_for_office(office: str | None = None) -> list[dict]:
    scripts = api_request("scripts/scheduled", office)
    # A fallback credential may cover multiple offices. Do not expand an explicit
    # rollout office list to every office authorized by that credential.
    return [
        script
        for script in scripts
        if not office or normalize_office(script["office"]) == normalize_office(office)
    ]


def trigger_job(script_id: str, office: str | None = None) -> dict:
    return api_request("jobs", office, {"scriptId": script_id})
