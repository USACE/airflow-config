"""
# Analysis Of Record for Calibration (AORC)

## Colorado State University (CSU) archive processor
"""

import json
import shutil
import pendulum
import os
import logging
from pathlib import Path
from string import Template
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
import zipfile
import tarfile
from airflow.exceptions import AirflowSkipException

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import copy_s3_file

import helpers.cumulus as cumulus
import helpers.downloads as downloads


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "end_date": datetime(2021, 1, 1),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule="@yearly",
    tags=["cumulus", "aorc", "precip", "archive"],
    max_active_runs=1,
    max_active_tasks=10,
)
def cumulus_aorc_precip_csu():
    """
    # AORC - CSU Archive

    This DAG serves as the main orchestrator for processing of AORC CSU archive files.

    ## S3 Processing

    The first task, load_annual_zip(), will pull annual AORC precip .zip files from
    the source bucket, unzip them, untar the nested monthly precip .tgz files, and
    upload the contained .nc4 files to the destination acquirables bucket.

    ## S3 Bucket and Key

    Source Bucket = `aorc-csu`

    key = `CY[YYYY].zip`

    ## Cumulus Acquirable

    AORC CSU acquirable slug: `aorc-precip-csu`
    """
    CUMULUS_ACQUIRABLE = "aorc-precip-csu"

    S3_SRC_BUCKET = "aorc-csu"
    S3_DST_BUCKET = "castle-data-develop"

    @task()
    def load_annual_zip():
        logical_date = get_current_context()["logical_date"]
        year_str = logical_date.format("YYYY")
        key = f"CY{year_str}_test.zip"
        if not downloads.s3_file_exists(S3_SRC_BUCKET, key):
            raise AirflowSkipException(f"Could not find object {key} in source bucket")
        with TemporaryDirectory() as annual_dir:
            logging.info(f"Downloading object {key} from bucket {S3_SRC_BUCKET}...")
            with downloads.S3TempDownload(S3_SRC_BUCKET, key) as temp_zip:
                zip = zipfile.ZipFile(temp_zip)
                try:
                    zip.extractall(annual_dir)
                finally:
                    zip.close()
            monthly_files = os.listdir(annual_dir)
            for monthly_file in monthly_files:
                logging.debug(f"Processing monthly tar file '{monthly_file}'")
                file_base = monthly_file.split(".")[-2]
                year = file_base[-6:-2]
                month = file_base[-2:]
                with TemporaryDirectory(dir=annual_dir) as monthly_dir:
                    tar = tarfile.open(os.path.join(annual_dir, monthly_file))
                    try:
                        tar.extractall(monthly_dir)
                    finally:
                        tar.close()
                    nested_dir = os.path.join(monthly_dir, year, f"{year}{month}")
                    all_files = os.listdir(nested_dir)
                    logging.debug(
                        f"Found {len(all_files)} total files for {year}/{month}"
                    )
                    nc_files = [file for file in all_files if file[-4:] == ".nc4"]
                    logging.info(f"Found {len(nc_files)} .nc4 files for {year}/{month}")
                    for file in nc_files:
                        dst_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{CUMULUS_ACQUIRABLE}/{year}/{month}/{file}"
                        filepath = os.path.join(nested_dir, file)
                        downloads.upload_file(filepath, S3_DST_BUCKET, dst_key)

    load_annual_zip()


csu_dag = cumulus_aorc_precip_csu()
