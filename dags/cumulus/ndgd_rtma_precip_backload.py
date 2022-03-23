import os, json
from datetime import datetime, timedelta, timezone

# from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowSkipException

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download, s3_list_keys, copy_s3_file

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": (datetime.utcnow() - timedelta(hours=48)).replace(minute=0, second=0),
    "start_date": datetime(2020, 1, 1),
    "end_date": datetime(2020, 1, 2),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}


@dag(
    default_args=default_args,
    schedule_interval="@monthly",
    tags=["cumulus", "precip"],
)
def cumulus_ndgd_rtma_precip_backload():
    """Copy from CPC Dir and rename placing into correct S3 acquirable dir and notifying API to process.\n
    Files will be processed by month (not days or hours)."""

    PRODUCT_SLUG = "ndgd-leia98-precip"

    def get_src_datetime(src_filename):

        dt_string = os.path.basename(src_filename).split("--")[0]
        dt = datetime.strptime(dt_string, "%Y.%m.%d.%H%M%S").replace(
            tzinfo=timezone.utc
        )

        return dt

    def get_new_filename(old_name):

        # old name looks like: 2019.10.31.235009--NCEP-rtma_precip--RT.23.ds.precipa.bin
        # convert to name like: ds.precipa_20211017_11.bin
        dt = get_src_datetime(os.path.basename(old_name))
        return f'ds.precipa_{dt.strftime("%Y%m%d_%H")}.bin'

    @task()
    def copy_raw_precip():

        results = []

        execution_date = get_current_context()["execution_date"]
        # execution_date = execution_date + relativedelta(months=1)

        # return json.dumps({"datetime": execution_date.isoformat(), "s3_key": s3_key})

        # Scan for files in dest directory
        # --------------------------------------
        dst_files = []
        dest_filename_prefix = f'ds.precipa_{execution_date.strftime("%Y%m")}'
        key_prefix = (
            f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{dest_filename_prefix}"
        )
        print("*" * 60)
        print(f"Scanning for objects dst: {key_prefix}")
        dst_objects = s3_list_keys(
            bucket_name=cumulus.S3_BUCKET,
            prefix=key_prefix,
            delimiter=None,
        )
        print(f"Match Count: {dest_filename_prefix}: {len(dst_objects)}")
        print("*" * 60)
        for obj in dst_objects:
            dst_files.append(os.path.basename(obj))

        # --------------------------------------

        # Scan for files in source directory
        month_dir = f'NCEP-rtma_precip-{execution_date.strftime("%Y.%m")}'
        key_prefix = f"{cumulus.S3_ACQUIRABLE_PREFIX}/leia98_from_CPC/{month_dir}/{execution_date.strftime('%Y')}"
        print("*" * 60)
        print(f"Scanning for objects in src: {key_prefix}")
        print("*" * 60)
        objects = s3_list_keys(
            bucket_name=cumulus.S3_BUCKET,
            prefix=key_prefix,
            delimiter=None,
        )

        if len(objects) == 0:
            print(f"no object found in {key_prefix}")
            raise AirflowSkipException

        for obj in objects:
            new_filename = get_new_filename(obj)
            if new_filename not in dst_files and ".bin" in obj:
                dst_s3_key = (
                    f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{new_filename}"
                )
                # print(obj)
                print(f"Copying {obj} to {dst_s3_key}")
                copy_s3_file(cumulus.S3_BUCKET, obj, cumulus.S3_BUCKET, dst_s3_key)
                results.append(
                    {
                        "datetime": get_src_datetime(obj).isoformat(),
                        "s3_key": dst_s3_key,
                    }
                )

        # If no files copied
        if len(results) == 0:
            print("No new files to copy.")
            raise AirflowSkipException

        return json.dumps(results)

    @task()
    def notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        for items in payload:

            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=items["datetime"],
                s3_key=items["s3_key"],
            )

        print(f"Sent {len(payload)} notifications to API")

        return

    # copy_raw_precip()
    notify_cumulus(copy_raw_precip())


cbrfc_dag = cumulus_ndgd_rtma_precip_backload()
