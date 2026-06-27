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
        error_msg = f"Retries not greater than 0 for DAG {dag}"
        if "backload" not in dag:
            assert retries > 0, error_msg


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


def test_batch_events_scheduled_driver_runs_every_minute():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.dags["cwms_batch_events_scheduled_jobs"]

    assert str(dag.timetable.summary) == "* * * * *"
    assert dag.max_active_runs == 2
    assert dag.max_active_tasks == 30
    assert {"get-due-scripts", "trigger-script"}.issubset(dag.task_ids)


def test_legacy_wmes_batch_dags_are_manual_compatibility_dags():
    dag_bag = DagBag(include_examples=False)

    for dag_id in ["cwms_hourly_jobs", "cwms_daily_jobs"]:
        dag = dag_bag.dags[dag_id]
        assert dag.timetable.summary == "None"
        assert dag.max_active_tasks == 30
        assert "trigger-script" in dag.task_ids
