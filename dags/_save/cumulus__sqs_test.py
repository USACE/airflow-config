"""
SQS TEST
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

from helpers.sqs import trigger_sqs

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 9, 18, 0, 0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

sqs_test = DAG(
    "sqs_test",
    default_args=default_args,
    schedule="1 * * * *",
    tags=["cumulus", "experimental"],
)

test = PythonOperator(
    task_id="sqstest",
    python_callable=trigger_sqs,
    op_kwargs={
        "queue_name": "cumulus-test",
        "message": json.dumps({"msg": "HAVE A NICE DAY"}, separators=(",", ":")),
    },
    dag=sqs_test,
)
