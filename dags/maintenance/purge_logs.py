"""
### Purge Logs

Airflow doesn't have a built in log rotation/purge (as of v2.1).  This DAG will delete and logs on disk older than 2 days.

We already write to remote logs (via S3)
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import logging
import subprocess
from textwrap import dedent

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    default_args=default_args,
    dag_id="maint_purge_logs",
    schedule_interval="@daily",
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=["maintenance"],
    doc_md=dedent(__doc__),
) as dag:

    def display_storage():

        bashCommand = "df -h && echo '----- /opt/airflow BREAKDOWN--------' && du -d 1 -h $AIRFLOW_HOME"
        ret = subprocess.run(bashCommand, capture_output=True, shell=True)
        lines = ret.stdout.decode().splitlines()
        for line in lines:
            logging.error(line)

    def purge_logs():

        display_storage()
        bashCommand = "echo '\n-- Deleting files older than 2 days --' && find $AIRFLOW_HOME/logs -mtime +2 -exec rm -fv {} \;"
        ret = subprocess.run(bashCommand, capture_output=True, shell=True)
        lines = ret.stdout.decode().splitlines()
        for line in lines:
            logging.error(line)
        display_storage()

    purge_task = PythonOperator(
        task_id="display_task_id",
        python_callable=purge_logs,
    )

    purge_task
