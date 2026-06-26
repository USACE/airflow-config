from datetime import datetime, timezone
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


def test_cron_matches_daily_schedule():
    logical_date = datetime(2026, 6, 26, 17, 0, tzinfo=timezone.utc)

    assert cron_matches("0 17 * * *", logical_date)
    assert not cron_matches("15 17 * * *", logical_date)


def test_cron_matches_ranges_lists_and_steps():
    logical_date = datetime(2026, 6, 26, 6, 30, tzinfo=timezone.utc)

    assert cron_matches("*/15 0-8 * 6,7 5", logical_date)
    assert not cron_matches("*/20 0-8 * 6,7 5", logical_date)


def test_scheduled_scripts_for_offices_filters_type_enabled_and_office(monkeypatch):
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {"id": "1", "office": "SWT", "scheduleEnabled": True, "scheduleType": "hourly"},
            {"id": "2", "office": "LRL", "scheduleEnabled": True, "scheduleType": "hourly"},
            {"id": "3", "office": "SWT", "scheduleEnabled": True, "scheduleType": "cron"},
            {"id": "4", "office": "SWT", "scheduleEnabled": False, "scheduleType": "hourly"},
        ],
    )

    scripts = batch_events.scheduled_scripts_for_offices("hourly", ["swt"])

    assert [script["id"] for script in scripts] == ["1"]


def test_scheduled_scripts_for_offices_allows_all_offices_when_filter_empty(monkeypatch):
    monkeypatch.setattr(
        batch_events,
        "get_scheduled_scripts",
        lambda: [
            {"id": "1", "office": "SWT", "scheduleEnabled": True, "scheduleType": "cron"},
            {"id": "2", "office": "LRL", "scheduleEnabled": True, "scheduleType": "cron"},
        ],
    )

    scripts = batch_events.scheduled_scripts_for_offices("cron", [])

    assert [script["id"] for script in scripts] == ["1", "2"]
