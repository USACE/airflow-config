import json
import requests
import logging
import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow import AirflowException
from datetime import datetime, timedelta
from airflow.operators.python import get_current_context

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

from helpers.downloads import check_key_exists
import helpers.cumulus as cumulus

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
with DAG(
    "cumulus_hrrr_precip_backload_from_s3",
    default_args=default_args,
    description="HRRR Forecast Precip Backload From S3",
    start_date=datetime(2021, 10, 1),
    end_date=datetime(2022, 5, 9),
    tags=["cumulus", "precip", "forecast"],
    schedule_interval="@hourly",
    catchup=True,
) as dag:
    dag.doc_md = """This pipeline handles the reading of existing HRRR acquirables from S3 and sending API notifications to reprocess products. \n
    High-Resolution Rapid Refresh (HRRR) \n
    Files matching hrrr.t{HH}z.wrfsfcf{HH}.grib2 - Multiple hourly files (second variable) per forecast file (first variable)
    """

    PRODUCT_SLUG = "hrrr-total-precip"
    ##############################################################################
    def check_acquirable_file(hour):

        exec_dt = get_current_context()["execution_date"]
        acq_filename = f'hrrr.{exec_dt.strftime("%Y%m%d")}.t{exec_dt.strftime("%H")}z.wrfsfcf{str(hour).zfill(2)}.grib2'
        acq_s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{acq_filename}"

        print(f"Checking for acquirablefile: {acq_s3_key}")
        if check_key_exists(acq_s3_key, cumulus.S3_BUCKET):
            return json.dumps({"datetime": exec_dt.isoformat(), "s3_key": acq_s3_key})
        else:
            raise ValueError("Acquirablefile not found")

    ##############################################################################
    def notify_api(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)
        # print(f'payload is: {payload}')

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

        return

    ##############################################################################
    for fcst_hour in range(0, 19):

        print(f"Forecast Hour: {fcst_hour}")

        check_task_id = f"check_fcst_hr_{str(fcst_hour).zfill(2)}"

        check_task = PythonOperator(
            task_id=check_task_id,
            python_callable=check_acquirable_file,
            op_kwargs={
                "hour": str(fcst_hour).zfill(2),
            },
        )

        notify_api_task = PythonOperator(
            task_id=f"notify_api_fcst_hr_{str(fcst_hour).zfill(2)}",
            python_callable=notify_api,
            op_kwargs={
                "payload": "{{{{task_instance.xcom_pull(task_ids='{}')}}}}".format(
                    check_task_id
                )
            },
        )

        check_task >> notify_api_task
