import boto3
from botocore.exceptions import ClientError
import json
import logging
from tempfile import TemporaryDirectory
import os
from urllib.request import urlretrieve

from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# CONSTANTS
DOWNLOAD_OPERATOR_USE_CONNECTION = os.getenv("DOWNLOAD_OPERATOR_USE_CONNECTION", default=None)
DOWNLOAD_OPERATOR_USE_LAMBDA = True if os.getenv("DOWNLOAD_OPERATOR_USE_LAMBDA", default="FALSE").upper() == "TRUE" else False
DOWNLOAD_OPERATOR_USE_LAMBDA_NAME = os.getenv("DOWNLOAD_OPERATOR_USE_LAMBDA_NAME", default=None)
DOWNLOAD_OPERATOR_USE_LAMBDA_REGION = os.getenv("DOWNLOAD_OPERATOR_USE_LAMBDA_REGION", default=None)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""
    hook = S3Hook(aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION)
    load_file = hook.load_file(file_name, object_name, bucket)
    return load_file


def download(url, outfile):
    local_filename, headers = urlretrieve(url, outfile)
    return os.path.abspath(local_filename)


def trigger_download_lambda(payload):
    """Sends Message to AWS lambda to perform download"""
    hook = AwsLambdaHook(
        aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION,
        function_name=DOWNLOAD_OPERATOR_USE_LAMBDA_NAME,
        region_name=DOWNLOAD_OPERATOR_USE_LAMBDA_REGION,
    )
    resp = hook.invoke_lambda(payload=payload)
    return resp


def trigger_download_local(payload):
    """Performs Download Locally"""
    url, bucket, key = payload['url'], payload['s3_bucket'], payload['s3_key']
    
    with TemporaryDirectory() as td:
        # Download specified file
        _file = download(url, os.path.join(td, url.split("/")[-1]))
        # Save file to specified S3 path
        success = upload_file(_file, bucket, key)
        
    return json.dumps({"success": success, "url": url, "s3_bucket": bucket, "s3_key": key})


def trigger_download(*args, **kwargs):
    """Wrapper function. Accepts same signature expected by Airflow Callable"""

    print(f'Download URL: {kwargs["url"]}')

    if DOWNLOAD_OPERATOR_USE_LAMBDA:
        trigger_download_lambda(kwargs)
    else:
        trigger_download_local(kwargs)
