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

from helpers.batch_events import cron_matches  # noqa: E402


def test_cron_matches_daily_schedule():
    logical_date = datetime(2026, 6, 26, 17, 0, tzinfo=timezone.utc)

    assert cron_matches("0 17 * * *", logical_date)
    assert not cron_matches("15 17 * * *", logical_date)


def test_cron_matches_ranges_lists_and_steps():
    logical_date = datetime(2026, 6, 26, 6, 30, tzinfo=timezone.utc)

    assert cron_matches("*/15 0-8 * 6,7 5", logical_date)
    assert not cron_matches("*/20 0-8 * 6,7 5", logical_date)
