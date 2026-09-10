from datetime import datetime, timedelta, timezone
import io
import json
import requests
import traceback

from helpers.sqs import receive_sqs_messages, delete_sqs_message
from shef import shef_parser

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import get_current_context

WMES_SHEF_QUEUE_NAME = Variable.get("WMES_SHEF_QUEUE_NAME")
CDA_API_KEY = Variable.get("API_KEY")
CDA_URL = Variable.get("CDA_URL")
OFFICES = Variable.get("SHEF_SQS_TS_OFFICES").split(",")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(minutes=15)).replace(
        minute=0, second=0
    ),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    schedule="*/5 * * * *",
    tags=["wmes", "shef", "sqs"],
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
)
def sqs_shef_process_messages():
    """This pipeline will read available messages from the WMES SHEF queue and
    process them as specified based on the provided slug.  The SQS message is
    subsequently deleted if processing completes successfully."""

    @task()
    def read_shef_queue():

        # Check queue for messages
        # response will be None if no messages are available
        response = receive_sqs_messages(queue_name=WMES_SHEF_QUEUE_NAME)

        if response is not None and "Messages" in response:
            # Process messages
            print(f'Received {len(response["Messages"])} messages from SQS queue.')
            messages = []
            for message in response["Messages"]:
                print("FULL MESSAGE CONTENTS")
                print("--------------")
                print(message)
                print("--------------")
                # Print message body
                print(f"Received message: {message['Body']}")
                messages.append(message)

            return messages

        else:
            print("WithinDAG - No messages received from SQS queue.")
            raise AirflowSkipException("No messages received from SQS queue.")

    @task(map_index_template="{{ task_id }}")
    def process_message(message):
        context = get_current_context()
        try:
            message_body = json.loads(message["Body"])
            context["task_id"] = message_body["metadata"]["filename"]
            try:
                process_shef_file(message_body)
                return message
            except Exception:
                print(
                    f"Exception occured while processing {message_body['metadata']['filename']}"
                )
                print(traceback.format_exc())
                raise AirflowFailException(
                    "SHEF processing task failed. Leaving SQS message in queue..."
                )
        except json.JSONDecodeError:
            print(
                f"Unrecognized message format for MessageId {message['MessageId']} - Adding to delete queue"
            )
            return message

    def process_shef_file(message):
        print(f"Processing SHEF file: {message['metadata']['filename']}")
        callback_url = message["callback_url"]
        params = dict()
        params["disposition"] = "inline"
        response = requests.get(callback_url, params=params)
        input = io.StringIO(response.text)
        shef_parser.parse(
            input_stream=input,
            loader_spec=f"cda[{CDA_URL}][{CDA_API_KEY}][{OFFICES}]",
        )

    @task(trigger_rule="all_done")
    def delete_processed_messages(processed_messages):
        if not processed_messages:
            raise AirflowSkipException("No messages to delete. Skipping...")
        for message in processed_messages:
            if message:
                print(f"Deleting SQS MessageId {message['MessageId']}")
                receipt = message["ReceiptHandle"]
                delete_response = delete_sqs_message(
                    queue_name=WMES_SHEF_QUEUE_NAME,
                    receipt_handle=receipt,
                )
                print(f"Delete response: {delete_response}")
            else:
                print("Empty message found.  Skipping...")

    messages = read_shef_queue()
    processed_messages = process_message.expand(message=messages)
    delete_processed_messages(processed_messages)


sqs_shef_process_messages()
