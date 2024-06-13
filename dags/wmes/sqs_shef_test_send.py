import os
import time


from airflow import DAG

# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.sqs import send_sqs_message

# from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
# from airflow.operators.dummy import DummyOperator

from airflow.models import Variable

WMES_SHEF_QUEUE_NAME = Variable.get("WMES_SHEF_QUEUE_NAME")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=1)).replace(
        minute=0, second=0
    ),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule="5 * * * *",
    tags=["wmes", "shef", "sqs"],
    max_active_runs=1,
    max_active_tasks=1,
)
def sqs_shef_test_send_messages():
    """This pipeline handles ... \n"""

    @task()
    def send_shef_message():

        for i in range(10):
            print(f"Sending Message {i}")
            send_sqs_message(WMES_SHEF_QUEUE_NAME, f"my test message #{i}")
            time.sleep(1)

    send_shef_message()


sqs_shef_test_send_messages()
