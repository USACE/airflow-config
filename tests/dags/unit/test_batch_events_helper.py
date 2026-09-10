from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "plugins"))

airflow_module = types.ModuleType("airflow")
airflow_models_module = types.ModuleType("airflow.models")
airflow_models_module.Variable = object
sys.modules.setdefault("airflow", airflow_module)
sys.modules.setdefault("airflow.models", airflow_models_module)

from helpers import batch_events  # noqa: E402
from helpers.batch_events import cron_matches  # noqa: E402


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


def test_scripts_due_at_matches_cron_in_script_timezone(monkeypatch):
    logical_date = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {
                "id": "chicago-noon",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 12 * * 5",
                "scheduleTimezone": "America/Chicago",
            },
            {
                "id": "utc-noon",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 12 * * 5",
                "scheduleTimezone": "UTC",
            },
        ],
    )

    scripts = batch_events.scripts_due_at(logical_date)

    assert [script["id"] for script in scripts] == ["chicago-noon"]


def test_scripts_due_at_skips_invalid_timezone_without_blocking_other_scripts(
    monkeypatch, caplog
):
    logical_date = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {
                "id": "bad-zone",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 17 * * 5",
                "scheduleTimezone": "Mars/Base",
            },
            {
                "id": "good-zone",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 17 * * 5",
                "scheduleTimezone": "UTC",
            },
        ],
    )

    scripts = batch_events.scripts_due_at(logical_date)

    assert [script["id"] for script in scripts] == ["good-zone"]
    assert "Skipping scheduled script bad-zone" in caplog.text
    assert "scheduleTimezone" in caplog.text


def test_scripts_due_at_skips_nonexistent_spring_forward_local_occurrence(
    monkeypatch,
):
    logical_date = datetime(2026, 3, 8, 8, 15, tzinfo=timezone.utc)
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {
                "id": "missing-215",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 2 * * *",
                "scheduleTimezone": "America/Chicago",
            },
            {
                "id": "existing-315",
                "scheduleEnabled": True,
                "scheduleType": "cron",
                "scheduleCron": "15 3 * * *",
                "scheduleTimezone": "America/Chicago",
            },
        ],
    )

    scripts = batch_events.scripts_due_at(logical_date)

    assert [script["id"] for script in scripts] == ["existing-315"]


def test_scripts_due_at_runs_repeated_fall_back_local_occurrence_once(monkeypatch):
    script = {
        "id": "fall-back-115",
        "scheduleEnabled": True,
        "scheduleType": "cron",
        "scheduleCron": "15 1 * * *",
        "scheduleTimezone": "America/Chicago",
    }
    monkeypatch.setattr(batch_events, "get_scheduled_scripts", lambda: [script])

    first_occurrence = batch_events.scripts_due_at(
        datetime(2026, 11, 1, 6, 15, tzinfo=timezone.utc)
    )
    repeated_occurrence = batch_events.scripts_due_at(
        datetime(2026, 11, 1, 7, 15, tzinfo=timezone.utc)
    )

    assert [script["id"] for script in first_occurrence] == ["fall-back-115"]
    assert repeated_occurrence == []


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
