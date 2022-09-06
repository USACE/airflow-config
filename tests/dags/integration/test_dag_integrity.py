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


def test_dag_owner_is_airflow():
    dag_bag = DagBag(include_examples=False)
    for dag in dag_bag.dags:
        owner = dag_bag.dags[dag].default_args.get("owner", None)
        error_msg = f"'owner' not set to 'airflow' for DAG {dag}"
        assert owner == "airflow", error_msg
