"""National Blend of Models

Returns
-------
Airflow DAG
    Directed Acyclic Graph
"""
from datetime import datetime, timedelta
import json
from string import Template
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
}

MAX_HOUR = 36


@dag(
    default_args=default_args,
    tags=["cumulus", "precip", "airtemp", "NBM"],
    schedule="3 * * * *",
    max_active_runs=1,
    max_active_tasks=4,
)
def cumulus_nbm():
    """
    # National Blend of Models

    This pipeline handles download and API notification for NBM forecast products

    - [NBM Information](https://vlab.ncep.noaa.gov/web/mdl/nbm-download)

    - [NBM Product Source](https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod)

    Filename Pattern:

    `blend.tCCz.core.fHHH.RR.grib2`

    `where CC = forecast cycle, HHH = forecasted hour and RR = region (e.g., co, ak, pr)`
    """
    s3_bucket = cumulus.S3_BUCKET
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    region = "co"

    url_root = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
    blend_template = Template("blend.${date_}/${hr_}/core")
    src_filename_template = Template("blend.t${hr_}z.core.f${fhour_}.${region_}.grib2")
    dst_filename_template = Template(
        "blend.${ymd_}.t${hr_}z.core.f${fhour_}.${region_}.grib2"
    )
    product_slug = "nbm-co-01h"

    def create_task_group(**kwargs):
        fhour = kwargs["forecast_hour"]

        with TaskGroup(group_id=f"download_notify_nbm_f{fhour:03d}") as task_group:

            @task(task_id=f"download_f{fhour:03d}")
            def download():
                execution_date = get_current_context()["logical_date"]
                execution_hr = execution_date.hour
                execution_dt = execution_date.strftime("%Y%m%d")

                blend = blend_template.substitute(
                    date_=execution_dt, hr_=f"{execution_hr:02d}"
                )
                src_filename = src_filename_template.substitute(
                    hr_=f"{execution_hr:02d}",
                    fhour_=f"{fhour:03d}",
                    region_=region,
                )
                dst_filename = dst_filename_template.substitute(
                    ymd_=execution_dt,
                    hr_=f"{execution_hr:02d}",
                    fhour_=f"{fhour:03d}",
                    region_=region,
                )

                s3_key = f"{key_prefix}/{product_slug}/{dst_filename}"
                trigger_download(
                    url=f"{url_root}/{blend}/{src_filename}",
                    s3_bucket=s3_bucket,
                    s3_key=s3_key,
                )

                return json.dumps(
                    {
                        "datetime": execution_date.isoformat(),
                        "s3_key": s3_key,
                        "product_slug": product_slug,
                    }
                )

            @task(task_id=f"notify_f{fhour:03d}")
            def notify(payload):
                payload_json = json.loads(payload)
                result = cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[payload_json["product_slug"]],
                    datetime=payload_json["datetime"],
                    s3_key=payload_json["s3_key"],
                )
                print(result)

            download_ = download()
            notify(download_)

            return task_group
            # return notify_

    _ = [
        create_task_group(forecast_hour=forecast_hour)
        for forecast_hour in range(1, MAX_HOUR + 1)
    ]


DAG_ = cumulus_nbm()
