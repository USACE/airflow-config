"""Multi-Radar/Multi-Sensor V12

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
    "start_date": (datetime.utcnow() - timedelta(hours=24)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=15),
}


@dag(
    default_args=default_args,
    tags=["cumulus", "precip", "MRMS"],
    schedule_interval="0 * * * *",
)
def cumulus_mrms_v12_pass2():
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
    s3_bucket = cumulus.S3_BUCKET
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    filename_template = Template(
        "MRMS_MultiSensor_QPE_01H_Pass${pass_}_00.00_${datetime_}.grib2.gz"
    )

    url_root = "https://mrms.ncep.noaa.gov/data/2D"
    regional_products = {
        "conus": [
            {
                "slug": "ncep-mrms-v12-multisensor-qpe-01h-pass2",
                "suffix": "MultiSensor_QPE_01H_Pass2",
                "pass": 2,
            },
        ],
        "alaska": [
            {
                "slug": "ncep-mrms-v12-msqpe01h-p2-alaska",
                "suffix": "ALASKA/MultiSensor_QPE_01H_Pass2",
                "pass": 2,
            },
        ],
        "carib": [
            {
                "slug": "ncep-mrms-v12-msqpe01h-p2-carib",
                "suffix": "CARIB/MultiSensor_QPE_01H_Pass2",
                "pass": 2,
            },
        ],
    }

    # create dynamic tasks
    def create_task_group(name, products):

        with TaskGroup(group_id=f"{name}_mrmsv12") as task_group:
            for product in products:
                slug_ = product["slug"]
                suffix_ = product["suffix"]
                pass_ = product["pass"]

                @task(task_id=f"download_pass{pass_}")
                def download(slug_, suffix_, pass_):
                    file_dir = f"{url_root}/{suffix_}"
                    execution_date = get_current_context()["logical_date"]
                    filename = filename_template.substitute(
                        pass_=pass_,
                        datetime_=execution_date.strftime("%Y%m%d-%H0000"),
                    )
                    s3_key = f"{key_prefix}/{slug_}/{filename}"

                    print(f"Downloading {filename}")

                    trigger_download(
                        url=f"{file_dir}/{filename}", s3_bucket=s3_bucket, s3_key=s3_key
                    )

                    return json.dumps(
                        {
                            "datetime": execution_date.isoformat(),
                            "s3_key": s3_key,
                            "product_slug": slug_,
                        }
                    )

                @task(task_id=f"notify_pass{pass_}")
                def notify(payload):
                    payload_json = json.loads(payload)
                    result = cumulus.notify_acquirablefile(
                        acquirable_id=cumulus.acquirables[payload_json["product_slug"]],
                        datetime=payload_json["datetime"],
                        s3_key=payload_json["s3_key"],
                    )
                    print(result)

                download_ = download(slug_, suffix_, pass_)
                notify(download_)

            return task_group

    _ = [create_task_group(key, val) for key, val in regional_products.items()]


DAG_ = cumulus_mrms_v12_pass2()
