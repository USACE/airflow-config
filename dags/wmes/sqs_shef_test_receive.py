import os
import time


from airflow import DAG

# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.sqs import receive_sqs_messages, delete_sqs_message

# from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
# from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException

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
    schedule="*/5 * * * *",
    tags=["wmes", "shef", "sqs"],
    max_active_runs=1,
    max_active_tasks=1,
)
def sqs_shef_test_receive_messages():
    """This pipeline handles ... \n"""

    @task()
    def read_shef_queue():

        # Check queue for messages
        # response will be None if no messages are available
        response = receive_sqs_messages(queue_name=WMES_SHEF_QUEUE_NAME)

        if response is not None and "Messages" in response:
            # Process messages
            print(f'Received {len(response["Messages"])} messages from SQS queue.')
            for message in response["Messages"]:
                print("FULL MESSAGE CONTENTS")
                print("--------------")
                print(message)
                print("--------------")
                # Print message body
                print(f"Received message: {message['Body']}")

                # Delete the message from the queue
                delete_response = delete_sqs_message(
                    queue_name=WMES_SHEF_QUEUE_NAME,
                    receipt_handle=message["ReceiptHandle"],
                )
                print(f"Delete response: {delete_response}")

        else:
            print("WithinDAG - No messages received from SQS queue.")
            raise AirflowSkipException("No messages received from SQS queue.")

    @task
    def transform():
        print("transform the message to timeseries data")

    @task
    def load_to_cda():
        print("post to CDA")

    # waiting_for_messages >> read_shef_queue() >> send_to_wmes()
    read_shef_queue() >> transform() >> load_to_cda()


sqs_shef_test_receive_messages()
