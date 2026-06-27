from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WMES_DAGS = ROOT / "dags" / "wmes"


def read_dag(name: str) -> str:
    return (WMES_DAGS / name).read_text(encoding="utf-8")


def test_registry_scheduled_driver_uses_batch_events_dynamic_mapping():
    source = read_dag("cwms_batch_events_scheduled_jobs.py")

    assert "batch_events.scripts_due_at(logical_date)" in source
    assert "batch_events.trigger_job(script[\"id\"], office=script[\"office\"])" in source
    assert "trigger_script.expand(script=get_due_scripts())" in source
    assert 'schedule="* * * * *"' in source
    assert "max_active_tasks=30" in source
    assert "BatchOperator" not in source
    assert "SubmitJob" not in source
    assert "submit_job" not in source


def test_registry_scheduled_driver_does_not_wait_for_batch_completion():
    source = read_dag("cwms_batch_events_scheduled_jobs.py")

    for forbidden in [
        "describe_jobs",
        "get_log_events",
        "get_logs",
        "jobStatus",
        "sleep(",
        "wait_for",
    ]:
        assert forbidden not in source


def test_legacy_wmes_batch_dags_are_manual_batch_events_compatibility_paths():
    for dag_name, helper_call in {
        "cwms_hourly_jobs.py": 'batch_events.scheduled_scripts_for_offices(\n            "hourly"',
        "cwms_daily_jobs.py": 'batch_events.scheduled_scripts_for_offices(\n            "cron"',
    }.items():
        source = read_dag(dag_name)

        assert "schedule=None" in source
        assert helper_call in source
        assert "batch_events.trigger_job(script[\"id\"], office=script[\"office\"])" in source
        assert "trigger_script.expand(" in source
        assert "max_active_tasks=30" in source
        assert "BatchOperator" not in source
        assert "SubmitJob" not in source
        assert "submit_job" not in source
        assert "describe_jobs" not in source
        assert "get_log_events" not in source
        assert "wait_for" not in source
