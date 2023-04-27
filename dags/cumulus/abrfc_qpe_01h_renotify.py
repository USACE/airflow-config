"""
# Re-notify Cumulus of ABRFC products in S3 forcing a re-processing
"""

import json
from datetime import datetime, timedelta

import helpers.cumulus as cumulus
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers import downloads

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 21),
    "catchup_by_default": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    default_args=default_args,
    schedule="10 * * * *",
    tags=["cumulus", "ABRFC", "notify"],
    max_active_runs=6,
)
def cumulus_abrfc_qpe_01h_renotify():
    """
    # Arkansas-Red Basin River Forecast Center QPE 1-hour

    Re-notification is a process that takes the DAG's logical date as the starting point, checks the S3 bucket for ABRFC products and notifies the Cumulus database it exists.

    Notifying the database there is an existing product initiates a Geo Processing of that product.

    This DAG is a way to re-process existing raw products with an updated/modified geo processor
    """

    product_slug = "abrfc-qpe-01h"

    @task()
    def check_bucket():
        logical_date = get_current_context()["logical_date"]
        filename = logical_date.strftime("abrfc_qpe_01hr_%Y%m%d%HZ.nc")
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
        s3_key = f"{key_prefix}/{product_slug}/{filename}"

        if downloads.check_key_exists(s3_key, cumulus.S3_BUCKET):
            return json.dumps(
                {
                    "datetime": logical_date.isoformat(),
                    "s3_key": s3_key,
                    "product_slug": product_slug,
                }
            )

    @task()
    def notify(payload):
        if payload:
            payload_json = json.loads(payload)
            result = cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[payload_json["product_slug"]],
                datetime=payload_json["datetime"],
                s3_key=payload_json["s3_key"],
            )

    notify(check_bucket())


DAG_ = cumulus_abrfc_qpe_01h_renotify()
