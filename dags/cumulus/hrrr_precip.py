import json


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task

from datetime import datetime, timedelta
from airflow.operators.python import get_current_context

from helpers.downloads import trigger_download
import helpers.cumulus as cumulus

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "airflow", "retries": 6, "retry_delay": timedelta(minutes=10)}
with DAG(
    "cumulus_hrrr_precip",
    default_args=default_args,
    description="HRRR Forecast Precip",
    # start_date=(datetime.utcnow()-timedelta(hours=72)).replace(minute=0, second=0),
    start_date=(datetime.utcnow() - timedelta(hours=2)).replace(minute=0, second=0),
    tags=["cumulus", "precip", "forecast"],
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
) as dag:
    dag.doc_md = """This pipeline handles download and API notification for HRRR hourly forecast products. \n
    High-Resolution Rapid Refresh (HRRR) \n
    Info: https://rapidrefresh.noaa.gov/hrrr/\n
    Multiple sources:\n
    - https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/\n
    - https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.20210414/conus/\n
    Files matching hrrr.t{HH}z.wrfsfcf{HH}.grib2 - Multiple hourly files (second variable) per forecast file (first variable)
    """

    URL_ROOT = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
    PRODUCT_SLUG = "hrrr-total-precip"

    ##############################################################################
    @task()
    def download_precip_fcst_hour(hour):

        exec_dt = get_current_context()["logical_date"]

        directory = f'hrrr.{exec_dt.strftime("%Y%m%d")}/conus'
        src_product_filename = (
            f'hrrr.t{exec_dt.strftime("%H")}z.wrfsfcf{str(hour).zfill(2)}.grib2'
        )
        src_index_filename = src_product_filename + ".idx"

        dst_product_filename = f'hrrr.{exec_dt.strftime("%Y%m%d")}.t{exec_dt.strftime("%H")}z.wrfsfcf{str(hour).zfill(2)}.grib2'
        dst_product_s3_key = (
            f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{dst_product_filename}"
        )
        dst_index_s3_key = dst_product_s3_key + ".idx"

        print(f"Downloading product file: {src_product_filename}")
        trigger_download(
            url=f"{URL_ROOT}/{directory}/{src_product_filename}",
            s3_bucket=cumulus.S3_BUCKET,
            s3_key=dst_product_s3_key,
        )

        print(f"Downloading index file: {src_index_filename}")
        trigger_download(
            url=f"{URL_ROOT}/{directory}/{src_index_filename}",
            s3_bucket=cumulus.S3_BUCKET,
            s3_key=dst_index_s3_key,
        )

        return json.dumps(
            {"datetime": exec_dt.isoformat(), "s3_key": dst_product_s3_key}
        )

    ##############################################################################
    @task()
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
    @task()
    def get_product_hours():
        """Get a list of forecast hours available for the current execution time

        HRRR forecast products are generated for 49 hours at each 6-hour interval
        (0000, 0600, 1200, 1800 GMT) and for 19 hours otherwise.

        Returns:
            list[str]: A list of available hours in %H format
        """
        exec_dt = get_current_context()["logical_date"]
        exec_hr = exec_dt.strftime("%H")
        if exec_hr in ["00", "06", "12", "18"]:
            product_hours = list(range(0, 49))
        else:
            product_hours = list(range(0, 19))

        return product_hours

    product_hours = get_product_hours()
    product_payloads = download_precip_fcst_hour.expand(hour=product_hours)
    notify_api.expand(payload=product_payloads)
