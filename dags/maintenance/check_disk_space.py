from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="check_disk_space",
    schedule_interval="@hourly",
    start_date=datetime(2022, 4, 6),
    catchup=False,
    tags=["maintenance"],
) as dag:

    # Task 1
    # dummy_task = DummyOperator(task_id="dummy_task")

    # Task 2
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="df -h && echo '----- LOGS BREAKDOWN--------' && du -d 1 -h $AIRFLOW_HOME/logs",
    )
    bash_task
