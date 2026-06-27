from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import types

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "plugins"))

airflow_module = types.ModuleType("airflow")
airflow_models_module = types.ModuleType("airflow.models")
airflow_models_module.Variable = object
sys.modules.setdefault("airflow", airflow_module)
sys.modules.setdefault("airflow.models", airflow_models_module)

from helpers import batch_events  # noqa: E402
from helpers.batch_events import cron_matches  # noqa: E402


class FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_trigger_job_posts_once_to_batch_events_without_polling(monkeypatch):
    requests = []
    token_offices = []
    monkeypatch.setattr(
        batch_events,
        "get_service_account_token",
        lambda office=None: token_offices.append(office) or "token",
    )
    monkeypatch.setattr(
        batch_events,
        "get_config",
        lambda name, default=None: (
            "https://batch-events.example/api"
            if name == "BATCH_EVENTS_API_ROOT"
            else default
        ),
    )

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        return FakeResponse({"id": "job-1", "jobStatus": "Pending"})

    monkeypatch.setattr(batch_events, "urlopen", fake_urlopen)

    job = batch_events.trigger_job("script-1", office="SWT")

    assert job == {"id": "job-1", "jobStatus": "Pending"}
    assert token_offices == ["SWT"]
    assert len(requests) == 1
    request, timeout = requests[0]
    assert timeout == 30
    assert request.full_url == "https://batch-events.example/api/jobs"
    assert request.get_method() == "POST"
    assert request.headers["Authorization"] == "Bearer token"
    assert json.loads(request.data.decode("utf-8")) == {"scriptId": "script-1"}


def test_get_service_account_token_uses_office_specific_config(monkeypatch):
    requests = []
    values = {
        "BATCH_EVENTS_KEYCLOAK_TOKEN_URL": "https://keycloak/token",
        "BATCH_EVENTS_KEYCLOAK_CLIENT_ID": "default-client",
        "BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET": "default-secret",
        "BATCH_EVENTS_KEYCLOAK_CLIENT_ID_SWT": "swt-client",
        "BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET_SWT": "swt-secret",
        "BATCH_EVENTS_KEYCLOAK_SCOPE": "openid profile",
        "BATCH_EVENTS_KEYCLOAK_TOKEN_HOST_HEADER": "",
    }
    monkeypatch.setattr(
        batch_events,
        "get_config",
        lambda name, default=None: values.get(name, default),
    )

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        return FakeResponse({"access_token": "office-token"})

    monkeypatch.setattr(batch_events, "urlopen", fake_urlopen)

    token = batch_events.get_service_account_token("swt")

    assert token == "office-token"
    request, timeout = requests[0]
    assert timeout == 30
    body = request.data.decode("utf-8")
    assert "client_id=swt-client" in body
    assert "client_secret=swt-secret" in body
    assert "default-client" not in body


def test_get_service_account_token_uses_office_client_map(monkeypatch):
    requests = []
    values = {
        "BATCH_EVENTS_KEYCLOAK_TOKEN_URL": "https://keycloak/token",
        "BATCH_EVENTS_KEYCLOAK_CLIENT_ID": "default-client",
        "BATCH_EVENTS_KEYCLOAK_CLIENT_SECRET": "default-secret",
        "BATCH_EVENTS_KEYCLOAK_OFFICE_CLIENTS": json.dumps(
            {
                "SWT": {
                    "clientId": "mapped-swt-client",
                    "clientSecret": "mapped-swt-secret",
                }
            }
        ),
        "BATCH_EVENTS_KEYCLOAK_SCOPE": "openid profile",
        "BATCH_EVENTS_KEYCLOAK_TOKEN_HOST_HEADER": "",
    }
    monkeypatch.setattr(
        batch_events,
        "get_config",
        lambda name, default=None: values.get(name, default),
    )

    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse({"access_token": "mapped-token"})

    monkeypatch.setattr(batch_events, "urlopen", fake_urlopen)

    token = batch_events.get_service_account_token("SWT")

    assert token == "mapped-token"
    body = requests[0].data.decode("utf-8")
    assert "client_id=mapped-swt-client" in body
    assert "client_secret=mapped-swt-secret" in body


def test_get_scheduled_scripts_uses_configured_office_clients(monkeypatch):
    requests = []
    monkeypatch.setattr(
        batch_events,
        "get_config",
        lambda name, default=None: (
            "https://batch-events.example/api"
            if name == "BATCH_EVENTS_API_ROOT"
            else "SWT,LRL"
            if name == "BATCH_EVENTS_SCHEDULED_OFFICES"
            else default
        ),
    )
    monkeypatch.setattr(
        batch_events,
        "get_service_account_token",
        lambda office=None: f"token-{office}",
    )

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        token = request.headers["Authorization"].removeprefix("Bearer ")
        office = token.removeprefix("token-")
        return FakeResponse(
            [
                {
                    "id": f"{office.lower()}-script",
                    "office": office,
                    "scheduleEnabled": True,
                    "scheduleType": "hourly",
                }
            ]
        )

    monkeypatch.setattr(batch_events, "urlopen", fake_urlopen)

    scripts = batch_events.get_scheduled_scripts()

    assert [script["id"] for script in scripts] == ["swt-script", "lrl-script"]
    assert [request.headers["Authorization"] for request, _timeout in requests] == [
        "Bearer token-SWT",
        "Bearer token-LRL",
    ]


def test_get_scheduled_scripts_uses_single_default_client_without_office_config(
    monkeypatch,
):
    requests = []
    monkeypatch.setattr(
        batch_events,
        "get_config",
        lambda name, default=None: (
            "https://batch-events.example/api"
            if name == "BATCH_EVENTS_API_ROOT"
            else default
        ),
    )
    monkeypatch.setattr(
        batch_events,
        "get_service_account_token",
        lambda office=None: f"token-{office or 'default'}",
    )

    def fake_urlopen(request, timeout):
        requests.append(request)
        return FakeResponse([{"id": "script-1", "scheduleEnabled": True}])

    monkeypatch.setattr(batch_events, "urlopen", fake_urlopen)

    scripts = batch_events.get_scheduled_scripts()

    assert scripts == [{"id": "script-1", "scheduleEnabled": True}]
    assert len(requests) == 1
    assert requests[0].headers["Authorization"] == "Bearer token-default"


def test_cron_matches_daily_schedule():
    logical_date = datetime(2026, 6, 26, 17, 0, tzinfo=timezone.utc)

    assert cron_matches("0 17 * * *", logical_date)
    assert not cron_matches("15 17 * * *", logical_date)


def test_cron_matches_ranges_lists_and_steps():
    logical_date = datetime(2026, 6, 26, 6, 30, tzinfo=timezone.utc)

    assert cron_matches("*/15 0-8 * 6,7 5", logical_date)
    assert not cron_matches("*/20 0-8 * 6,7 5", logical_date)


def test_cron_matches_day_of_month_or_day_of_week_when_both_are_restricted():
    friday = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)

    assert cron_matches("15 17 1 * 5", friday)
    assert cron_matches("15 17 26 * 1", friday)
    assert not cron_matches("15 17 1 * 1", friday)


def test_cron_matches_sunday_as_zero_or_seven():
    sunday = datetime(2026, 6, 28, 17, 15, tzinfo=timezone.utc)

    assert cron_matches("15 17 * * 0", sunday)
    assert cron_matches("15 17 * * 7", sunday)


def test_scheduled_scripts_for_offices_filters_type_enabled_and_office(monkeypatch):
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda offices=None: [
            {
                "id": "1",
                "office": "SWT",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
            },
            {
                "id": "2",
                "office": "LRL",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
            },
            {
                "id": "3",
                "office": "SWT",
                "scheduleEnabled": True,
                "scheduleType": "cron",
            },
            {
                "id": "4",
                "office": "SWT",
                "scheduleEnabled": False,
                "scheduleType": "hourly",
            },
        ],
    )

    scripts = batch_events.scheduled_scripts_for_offices("hourly", ["swt"])

    assert [script["id"] for script in scripts] == ["1"]


def test_scheduled_scripts_for_offices_allows_all_offices_when_filter_empty(
    monkeypatch,
):
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda offices=None: [
            {
                "id": "1",
                "office": "SWT",
                "scheduleEnabled": True,
                "scheduleType": "cron",
            },
            {
                "id": "2",
                "office": "LRL",
                "scheduleEnabled": True,
                "scheduleType": "cron",
            },
        ],
    )

    scripts = batch_events.scheduled_scripts_for_offices("cron", [])

    assert [script["id"] for script in scripts] == ["1", "2"]


def test_scripts_due_at_matches_hourly_minute_and_cron(monkeypatch):
    logical_date = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {
                "id": "hourly-due",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
                "scheduleMinute": 15,
            },
            {
                "id": "hourly-later",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
                "scheduleMinute": 45,
            },
            {
                "id": "cron-due",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 17 * * 5",
            },
            {
                "id": "disabled",
                "scheduleEnabled": False,
                "scheduleType": "hourly",
                "scheduleMinute": 15,
            },
        ],
    )

    scripts = batch_events.scripts_due_at(logical_date)

    assert [script["id"] for script in scripts] == ["hourly-due", "cron-due"]


def test_scripts_due_at_includes_multiple_offices_due_same_minute(monkeypatch):
    logical_date = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {
                "id": "swt-hourly",
                "office": "SWT",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
                "scheduleMinute": 15,
            },
            {
                "id": "lrl-hourly",
                "office": "LRL",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
                "scheduleMinute": 15,
            },
            {
                "id": "mvk-later",
                "office": "MVK",
                "scheduleEnabled": True,
                "scheduleType": "hourly",
                "scheduleMinute": 30,
            },
        ],
    )

    scripts = batch_events.scripts_due_at(logical_date)

    assert [script["id"] for script in scripts] == ["swt-hourly", "lrl-hourly"]


def test_scripts_due_at_skips_invalid_cron_without_blocking_other_scripts(
    monkeypatch, caplog
):
    logical_date = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {
                "id": "bad-cron",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "99 17 * * *",
            },
            {
                "id": "good-cron",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 17 * * 5",
            },
        ],
    )

    scripts = batch_events.scripts_due_at(logical_date)

    assert [script["id"] for script in scripts] == ["good-cron"]
    assert "Skipping scheduled script bad-cron" in caplog.text
