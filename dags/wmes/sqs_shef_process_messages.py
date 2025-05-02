from datetime import datetime, timedelta, timezone
import io
import json
import requests
import traceback

from helpers.sqs import receive_sqs_messages, delete_sqs_message
from shef import shef_parser

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

WMES_SHEF_QUEUE_NAME = Variable.get("WMES_SHEF_QUEUE_NAME")
CDA_API_KEY = Variable.get("API_KEY")
CDA_URL = Variable.get("CDA_URL")

# Associate offices with product slugs for use in CDA requests
OFFICE_PRODUCTS = {
    "LRL": [
        "ohrfc-lrl-qpf-locals",
        "ohrfc-lrl-qpf-res",
        "ohrfc-lrl-qpf-stages",
        "ohrfc-lrl-qpf-totals",
    ]
}


def get_office_from_slug(slug: str):
    for office, slugs in OFFICE_PRODUCTS.items():
        if slug in slugs:
            return office
    return None


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(minutes=15)).replace(
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

    @task
    def process_messages(messages):
        processed_messages = []
        for message in messages:
            try:
                message_body = json.loads(message["Body"])
                slug = message_body["product"]["slug"]
                try:
                    office_code = get_office_from_slug(slug)
                    if office_code:
                        process_shef_file(message_body, office_code)
                    else:
                        print(f"Unhandled slug: {slug} -- Skipping processing")
                    processed_messages.append(message)
                except Exception:
                    print(
                        f"Exception occured while processing {message_body['metadata']['filename']}"
                    )
                    print(traceback.format_exc())
            except json.JSONDecodeError:
                print(
                    f"Unrecognized message format for MessageId {message['MessageId']} - Adding to delete queue"
                )
                processed_messages.append(message)
        return processed_messages

    def process_shef_file(message, office_code):
        print(f"Processing SHEF file: {message['metadata']['filename']}")
        print(f"Associated office: {office_code}")
        callback_url = message["callback_url"]
        params = dict()
        params["disposition"] = "inline"
        response = requests.get(callback_url, params=params)
        input = io.StringIO(response.text)
        shef_parser.parse(
            input_stream=input,
            loader_spec=f"cda[{office_code}][{CDA_URL}][{CDA_API_KEY}]",
        )

    @task
    def delete_processed_messages(processed_messages):
        for message in processed_messages:
            print(f"Deleting SQS MessageId {message['MessageId']}")
            receipt = message["ReceiptHandle"]
            delete_response = delete_sqs_message(
                queue_name=WMES_SHEF_QUEUE_NAME,
                receipt_handle=receipt,
            )
            print(f"Delete response: {delete_response}")

    delete_processed_messages(process_messages(read_shef_queue()))


sqs_shef_process_messages()
