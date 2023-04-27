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

    Notifying the database there is an existing product to initiate a Geo Processing of that product.

    This DAG is a way to re-process existing raw products with an updated/modified geo processor
    """

    product_slug = "abrfc-qpe-01h"

    @task()
    def notify():
        logical_date = get_current_context()["logical_date"]
        filename = logical_date.strftime("abrfc_qpe_01hr_%Y%m%d%HZ.nc")
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
        s3_key = f"{key_prefix}/{product_slug}/{filename}"

        result = cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[product_slug],
            datetime=logical_date.isoformat(),
            s3_key=s3_key,
        )

    notify()

DAG_ = cumulus_abrfc_qpe_01h_renotify()
