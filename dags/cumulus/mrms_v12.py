"""
# Multi-Radar/Multi-Sensor V12

This pipeline handles download, processing, and derivative product creation for MultiRadar MultiSensor QPE 1HR Pass1 and Pass2

Raw data downloaded to S3 and notifies the Cumulus API of new product(s)

URLs:
- BASE - https://mrms.ncep.noaa.gov/data/2D
- Conus - [___<BASE>___/MultiSensor_QPE_01H_Pass1/](https://mrms.ncep.noaa.gov/data/2D/MultiSensor_QPE_01H_Pass1/)
- Alaska - [___<BASE>___/ALASKA/MultiSensor_QPE_01H_Pass1/](https://mrms.ncep.noaa.gov/data/2D/ALASKA/MultiSensor_QPE_01H_Pass1/)
- Carib - [___<BASE>___/CARIB/MultiSensor_QPE_01H_Pass1/](https://mrms.ncep.noaa.gov/data/2D/CARIB/MultiSensor_QPE_01H_Pass1/)

Filename Pattern:

`MRMS_MultiSensor_QPE_01H_Pass1_00.00_YYYYMMDD-HH0000.grib2.gz`
"""


from datetime import datetime, timedelta
import json
from string import Template
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=24)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    default_args=default_args,
    tags=["stable", "cumulus", "precip", "MRMS"],
    schedule_interval="5 * * * *",
    doc_md=__doc__,
)
def mrms_v12():
    """[summary]

    Returns
    -------
    [type]
        [description]
    """
    s3_bucket = cumulus.S3_BUCKET
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    filename_template = Template(
        "MRMS_MultiSensor_QPE_01H_Pass${pass_}_00.00_${datetime_}.grib2.gz"
    )

    url_root = "https://mrms.ncep.noaa.gov/data/2D"
    products = {
        "conus_p1": {
            "slug": "ncep-mrms-v12-multisensor-qpe-01h-pass1",
            "suffix": "MultiSensor_QPE_01H_Pass1",
            "pass": 1,
        },
        "conus_p2": {
            "slug": "ncep-mrms-v12-multisensor-qpe-01h-pass2",
            "suffix": "MultiSensor_QPE_01H_Pass2",
            "pass": 2,
        },
        "alaska_p1": {
            "slug": "ncep-mrms-v12-msqpe01h-p1-alaska",
            "suffix": "ALASKA/MultiSensor_QPE_01H_Pass1",
            "pass": 1,
        },
        "alaska_p2": {
            "slug": "ncep-mrms-v12-msqpe01h-p2-alaska",
            "suffix": "ALASKA/MultiSensor_QPE_01H_Pass2",
            "pass": 2,
        },
        "carib_p1": {
            "slug": "ncep-mrms-v12-msqpe01h-p1-carib",
            "suffix": "CARIB/MultiSensor_QPE_01H_Pass1",
            "pass": 1,
        },
        "carib_p2": {
            "slug": "ncep-mrms-v12-msqpe01h-p2-carib",
            "suffix": "CARIB/MultiSensor_QPE_01H_Pass2",
            "pass": 2,
        },
    }

    # create dynamic tasks
    def create_task(**kwargs):
        t_id = kwargs["task_id"]

        @task(task_id=t_id)
        def download():
            product_slug = kwargs["task_values"]["slug"]
            url_suffix = kwargs["task_values"]["suffix"]
            file_dir = f"{url_root}/{url_suffix}"
            execution_date = get_current_context()["execution_date"]
            filename = filename_template.substitute(
                pass_=kwargs["task_values"]["pass"],
                datetime_=execution_date.strftime("%Y%m%d-%H0000"),
            )
            s3_key = f"{key_prefix}/{product_slug}/{filename}"

            print(f"Downloading {filename}")

            trigger_download(
                url=f"{file_dir}/{filename}", s3_bucket=s3_bucket, s3_key=s3_key
            )

            return json.dumps(
                {
                    "datetime": execution_date.isoformat(),
                    "s3_key": s3_key,
                    "product_slug": product_slug,
                }
            )

        @task(task_id=f"{t_id}_notify_cumulus")
        def notify(payload):
            payload_json = json.loads(payload)
            result = cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[payload_json["product_slug"]],
                datetime=payload_json["datetime"],
                s3_key=payload_json["s3_key"],
            )
            print(result)

        download_ = download()
        notify_ = notify(download_)

        return notify_

    _ = [
        create_task(task_id=t_id, task_values=t_val) for t_id, t_val in products.items()
    ]


DAG_ = mrms_v12()
