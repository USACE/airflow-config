from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import logging
import subprocess

with DAG(
    dag_id="check_disk_space",
    schedule_interval="@hourly",
    start_date=datetime(2022, 4, 6),
    catchup=False,
    tags=["maintenance"],
) as dag:

    def run_cmd():

        bashCommand = "df -h && echo '----- /opt/airflow BREAKDOWN--------' && du -d 1 -h $AIRFLOW_HOME"

        ret = subprocess.run(bashCommand, capture_output=True, shell=True)

        lines = ret.stdout.decode().splitlines()

        for line in lines:
            logging.error(line)

    bash_task = PythonOperator(
        task_id="display_task_id",
        python_callable=run_cmd,
    )
    bash_task
