"""
Acquire and Process NDFD

Root URL : https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus
"""
import os, json, logging
import requests
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import dag
from helpers.downloads import trigger_download, read_s3_file
from airflow.exceptions import AirflowSkipException

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

import helpers.cumulus as cumulus

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
with DAG(
    "cumulus_ndfd",
    default_args=default_args,
    description="National Digital Forecast Database (Precip and AirTemp)",
    start_date=(datetime.utcnow() - timedelta(hours=1)).replace(minute=0, second=0),
    # start_date=datetime(2021, 3, 27),
    tags=["cumulus", "forecast", "precip", "airtemp"],
    # schedule_interval='*/15 * * * *',
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    dag.doc_md = __doc__

    URL_ROOT = f"https://tgftp.nws.noaa.gov/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus"
    ##############################################################################
    def get_product_datetime_from_dir_listing(contents, product_filename):

        for line in contents.splitlines():
            line_parts = line.split(" ")
            if line_parts[-1] == product_filename:

                # Ex: May 20 14:19 ds.qpf.bin
                # ----------------------------------
                # line_parts[-1] -> filename
                # line_parts[-2] -> time (HH:MM) UTC
                # line_parts[-3] -> day (no leading zero)
                # line_parts[-4] -> month name (3 char)

                file_dt_str = f"{line_parts[-4]} {line_parts[-3]} {line_parts[-2]}"
                file_dt = datetime.strptime(file_dt_str, "%b %d %H:%M")
                file_dt = file_dt.replace(year=datetime.now().year)  # set current year

        return file_dt

    ##############################################################################
    def check_new_forecast(status_s3_key, src_status_filepath, src_filename):

        # Check S3 status file for last datetime string saved from prev request
        s3_forecast_datetime = None

        s3_status_contents = read_s3_file(status_s3_key, cumulus.S3_BUCKET)
        # read_s3_file returns a list, convert back to string
        s3_status_contents = "".join(s3_status_contents)

        if s3_status_contents:
            s3_forecast_datetime = get_product_datetime_from_dir_listing(
                s3_status_contents, src_filename
            )
            logging.info(f"S3 Status Forecast datetime is: {s3_forecast_datetime}")
        else:
            logging.warning("S3 status file not found.")
            # S3 forecast has no content (no file) which should only happen on
            # first run or if somewhere deletes the file manually.

        # Check the datetime of the source product
        src_file_datetime = None

        r = requests.get(src_status_filepath)
        src_status_contents = r.text.strip()

        if src_status_contents:
            src_file_datetime = get_product_datetime_from_dir_listing(
                src_status_contents, src_filename
            )
            logging.info(f"{src_filename} source datetime is: {src_file_datetime}")
        else:
            logging.warning("Source status file not found.")

        # Compare datetimes
        if src_file_datetime == s3_forecast_datetime:
            logging.info("No updated forecast detected")
            raise AirflowSkipException
        else:
            return src_file_datetime.timestamp()

    ##############################################################################
    def download_product(
        cumulus_slug, src_file_url, s3_key_prefix, dst_filename, file_datetime
    ):

        # Convert the execution date which comes in a string back to a datetime obj
        # Ex: 2021-05-20T01:00:00+00:00
        # ex_date = datetime.strptime(ex_date, '%Y-%m-%dT%H:%M:%S%z')

        file_datetime = datetime.fromtimestamp(float(file_datetime))
        file_datetime = file_datetime.replace(tzinfo=timezone.utc)
        logging.info("File datetime is: {file_datetime}")

        print(f"Downloading {src_file_url}")

        s3_key = f"{s3_key_prefix}/{dst_filename[0]}{file_datetime.strftime(dst_filename[1])}{dst_filename[2]}"
        # s3_key = '%s/%s/'.format(s3_key_prefix, dst_filename[0], )
        print(f"S3_KEY is: {s3_key}")
        output = trigger_download(
            url=src_file_url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        # Replace/Update the status file with new datetime contents
        output = trigger_download(
            url=f"{os.path.dirname(src_file_url)}/ls-l",
            s3_bucket=cumulus.S3_BUCKET,
            s3_key=f"{s3_key_prefix}/_status/ls-l",
        )

        return json.dumps(
            {
                "datetime": file_datetime.isoformat(),
                "s3_key": s3_key,
                "product_slug": cumulus_slug,
            }
        )

    ##############################################################################
    def notify_cumulus(payload):

        print(f"Notify Cumulus was called with a payload of: {payload}")
        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[payload["product_slug"]],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

    ##############################################################################

    PRODUCTS = [
        {
            "name": "qpf_day1_3_06h",
            "cumulus_slug": "ndfd-conus-qpf-06h",
            "src_dir": f"{URL_ROOT}/VP.001-003",
            "src_filename": "ds.qpf.bin",
            "dst_filename": ["ds.qpf_", "%Y%m%d%H%M", ".bin"],
        },
        {
            "name": "airtemp_day1_3",
            "cumulus_slug": "ndfd-conus-airtemp",
            "src_dir": f"{URL_ROOT}/VP.001-003",
            "src_filename": "ds.temp.bin",
            "dst_filename": ["ds.airtemp_", "%Y%m%d%H%M", "-1-3.bin"],
        },
        {
            "name": "airtemp_day4_7",
            "cumulus_slug": "ndfd-conus-airtemp",
            "src_dir": f"{URL_ROOT}/VP.004-007",
            "src_filename": "ds.temp.bin",
            "dst_filename": ["ds.airtemp_", "%Y%m%d%H%M", "-4-7.bin"],
        },
    ]

    for p in PRODUCTS:

        check_new_forecast_task_id = f"check_new_forecast_{p['name']}"
        check_new_forecast_task = PythonOperator(
            task_id=check_new_forecast_task_id,
            python_callable=check_new_forecast,
            op_kwargs={
                "status_s3_key": f"{cumulus.S3_ACQUIRABLE_PREFIX}/{p['cumulus_slug']}/_status/ls-l",
                "src_status_filepath": f"{p['src_dir']}/ls-l",
                "src_filename": p["src_filename"],
            },
        )

        download_task_id = f"download_{p['name']}"
        download_task = PythonOperator(
            task_id=download_task_id,
            python_callable=download_product,
            op_kwargs={
                "src_file_url": f"{p['src_dir']}/{p['src_filename']}",
                "s3_key_prefix": f"{cumulus.S3_ACQUIRABLE_PREFIX}/{p['cumulus_slug']}",
                "dst_filename": p["dst_filename"],
                "cumulus_slug": p["cumulus_slug"],
                # 'ex_date': "{{execution_date}}",
                "file_datetime": "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(
                    check_new_forecast_task_id
                ),
            },
        )

        notify_cumulus_task = PythonOperator(
            task_id=f"notify_cumulus{p['name']}",
            python_callable=notify_cumulus,
            op_kwargs={
                "payload": "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(
                    download_task_id
                )
            },
        )

        check_new_forecast_task >> download_task >> notify_cumulus_task
