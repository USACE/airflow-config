# Function to receive messages from SQS
import os
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
import boto3

# AWS_ENDPOINT_URL_SQS should be used by boto3 (and sqsHook) for default sqs endpoint


#############################################################################
def send_sqs_message(queue_name, message):

    sqs = boto3.client("sqs")

    # Get the queue URL
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    print(f"Queue URL: {queue_url}")

    hook = SqsHook()
    response = hook.send_message(queue_url=queue_url, message_body=message)
    print(f"Sent message with response: {response}")

    return


#############################################################################
def receive_sqs_messages(queue_name):

    sqs = boto3.client("sqs")

    # Get the queue URL
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    print(f"Queue URL: {queue_url}")

    # Receive messages from the queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # Adjust as per your requirement
        VisibilityTimeout=60,  # Timeout in seconds for which the message is hidden from subsequent retrievals
        WaitTimeSeconds=20,  # Long polling: Wait up to 20 seconds for new messages
    )

    return response


#############################################################################
def delete_sqs_message(queue_name, receipt_handle):

    sqs = boto3.client("sqs")

    # Get the queue URL
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    print(f"Queue URL: {queue_url}")

    response = sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
    )

    return response
