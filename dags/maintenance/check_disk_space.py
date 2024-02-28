from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import logging
import subprocess

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    default_args=default_args,
    dag_id="check_disk_space",
    schedule="@hourly",
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
