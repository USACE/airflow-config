import boto3
from botocore.exceptions import ClientError
import json
import logging
from tempfile import TemporaryDirectory
from pathlib import Path
import os
from urllib.request import urlretrieve

from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# CONSTANTS
DOWNLOAD_OPERATOR_USE_CONNECTION = os.getenv("DOWNLOAD_OPERATOR_USE_CONNECTION", default=None)
DOWNLOAD_OPERATOR_USE_LAMBDA = True if os.getenv("DOWNLOAD_OPERATOR_USE_LAMBDA", default="FALSE").upper() == "TRUE" else False
DOWNLOAD_OPERATOR_USE_LAMBDA_NAME = os.getenv("DOWNLOAD_OPERATOR_USE_LAMBDA_NAME", default=None)
DOWNLOAD_OPERATOR_USE_LAMBDA_REGION = os.getenv("DOWNLOAD_OPERATOR_USE_LAMBDA_REGION", default=None)


def upload_string_s3(data, bucket, key, replace=True):
    """Upload a file with a string inside to S3 bucket"""
    hook = S3Hook(aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION)
    load_str = hook.load_string(string_data=data, key=key, bucket_name=bucket, replace=True)
    return load_str

def upload_file(filename, bucket, key):
    """Upload a file to an S3 bucket"""
    hook = S3Hook(aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION)
    load_file = hook.load_file(filename=filename, key=key, bucket_name=bucket, replace=True)
    return load_file

def copy_s3_file(src_bucket, src_key, dst_bucket, dst_key):
    """Copy S3 file from one location to another"""
    hook = S3Hook(aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION)        
    h = hook.copy_object(src_key, dst_key, source_bucket_name=src_bucket, dest_bucket_name=dst_bucket, source_version_id=None)
    return

def s3_file_exists(bucket, key):
    """Check for S3 file"""
    hook = S3Hook(aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION)
    h = hook.check_for_key(key=key, bucket_name=bucket)
    return h

def read_s3_file(file_name, bucket, object_name=None):
    """Download a file from S3 bucket, return text contents"""
    hook = S3Hook(aws_conn_id=DOWNLOAD_OPERATOR_USE_CONNECTION)
    try:
        h = hook.download_file(file_name, bucket)
    except:
        return []
    
    with open(h, "r") as f:   
        contents = f.readlines()

    # S3 Hook.download_file uses a built-in namedtemporaryfile method
    # however tests were showing the file wasn't being deleted    
    if os.path.exists(h): os.remove(h)

    # returns a list of strings
    return contents

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
