from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag(include_examples=False)
    # import errors aren't raised but captured to ensure all dags are parsed
    assert len(dag_bag.import_errors) == 0, "No Import Failures"


def test_at_least_one_tag():
    dag_bag = DagBag(include_examples=False)

    for dag_id, dag in dag_bag.dags.items():
        err_msg = f"{dag_id} in {dag.fileloc} has no tags"
        assert dag.tags, err_msg


def test_at_least_one_retry():
    dag_bag = DagBag(include_examples=False)
    for dag in dag_bag.dags:
        retries = dag_bag.dags[dag].default_args.get("retries", 0)
        if dag == "cwms_batch_events_scheduled_jobs":
            # POST /jobs is not idempotent: an automatic retry after an ambiguous
            # response could submit the same occurrence twice.
            assert retries == 0
            continue
        error_msg = f"Retries not greater than 0 for DAG {dag}"
        if "backload" not in dag:
            assert retries > 0, error_msg


def test_registry_scheduler_starts_paused_and_maps_independent_jobs():
    dag = DagBag(include_examples=False).dags["cwms_batch_events_scheduled_jobs"]
    assert dag is not None
    assert dag.is_paused_upon_creation
    assert not dag.catchup
    assert set(dag.task_ids) == {"get-due-scripts", "trigger-script"}


def test_registry_tick_evaluates_due_minute_not_interval_start(monkeypatch):
    from datetime import datetime, timezone
    from unittest.mock import Mock

    dag = DagBag(include_examples=False).dags["cwms_batch_events_scheduled_jobs"]
    callback = dag.get_task("get-due-scripts").python_callable
    due = datetime(2026, 6, 26, 17, 15, tzinfo=timezone.utc)
    context = {
        "data_interval_end": due,
        "logical_date": datetime(2026, 6, 26, 17, 14, tzinfo=timezone.utc),
    }
    monkeypatch.setitem(callback.__globals__, "get_current_context", lambda: context)
    helper = Mock()
    helper.scripts_due_at.return_value = [{"id": "script-1"}]
    monkeypatch.setitem(callback.__globals__, "batch_events", helper)
    assert callback() == [{"id": "script-1"}]
    helper.scripts_due_at.assert_called_once_with(due)


def test_max_active_runs_less_than_3():
    dag_bag = DagBag(include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert (
            dag.max_active_runs < 3
        ), f"Max active runs for DAG {dag_id} is {dag.max_active_runs}, which is not less than 3"


def test_max_active_tasks_less_than_5():
    dag_bag = DagBag(include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert (
            dag.max_active_tasks < 31
        ), f"Max active tasks for DAG {dag_id} is {dag.max_active_tasks}, which is not less than 5"


def test_dag_owner_is_airflow():
    dag_bag = DagBag(include_examples=False)
    for dag in dag_bag.dags:
        owner = dag_bag.dags[dag].default_args.get("owner", None)
        error_msg = f"'owner' not set to 'airflow' for DAG {dag}"
        assert owner == "airflow", error_msg
