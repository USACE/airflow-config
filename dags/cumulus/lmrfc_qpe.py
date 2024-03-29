"""
## Cumulus acquriable for LMRFC QPF

URL Dir - https://tgftp.nws.noaa.gov/data/rfc/lmrfc/xmrg_qpf/

File matching for:

QPE --> yyyyMMddHHz.grib.gz
"""

from datetime import datetime, timedelta

from airflow import DAG

import helpers.cumulus as cumulus
from helpers.downloads import trigger_download

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

implementation = {
    "default": {
        "bucket": cumulus.S3_BUCKET,
        "dag_id": "cumulus_lmrfc_qpe",
        "tags": ["cumulus", "precip", "LMRFC", "QPE"],
    },
}

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=1)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=10),
}


def create_dag(**kwargs):

    s3_bucket = kwargs["s3_bucket"]

    @dag(
        default_args=default_args,
        dag_id=kwargs["dag_id"],
        tags=kwargs["tags"],
        schedule=kwargs["schedule"],
        doc_md=__doc__,
        max_active_runs=2,
        max_active_tasks=4,
    )
    def cumulus_acq_rfc():
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

        base_url = "https://tgftp.nws.noaa.gov/data/rfc/lmrfc/xmrg_qpe"

        slug = "lmrfc-qpe-01h"

        @task()
        def download_rfc():
            context = get_current_context()
            ti = context["ti"]
            execution_date = ti.execution_date
            filename = f'{execution_date.strftime("%Y%m%d%H")}z.grib.gz'
            url = f"{base_url}/{filename}"
            s3_key = f"{key_prefix}/{slug}/{filename}"
            result = trigger_download(url=url, s3_bucket=s3_bucket, s3_key=s3_key)
            return [
                {
                    "execution": execution_date.isoformat(),
                    "url": url,
                    "s3_key": s3_key,
                    "s3_bucket": s3_bucket,
                    "slug": slug,
                }
            ]

        @task()
        def notify_cumulus(download_result):
            for item in download_result:
                result = cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[item["slug"]],
                    datetime=item["execution"],
                    s3_key=item["s3_key"],
                )

        # Task 1: Get dictionary of QPE and QPF files available
        _download_rfc = download_rfc()
        # Task 2: Use that list and compare what is in the S3 Bucket
        _notify_cumulus = notify_cumulus(_download_rfc)

        _download_rfc >> _notify_cumulus

    return cumulus_acq_rfc()


# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val["dag_id"]
    d_tags = val["tags"]
    d_bucket = val["bucket"]
    globals()[d_id] = create_dag(
        dag_id=d_id, tags=d_tags, s3_bucket=d_bucket, schedule="8 * * * *"
    )
