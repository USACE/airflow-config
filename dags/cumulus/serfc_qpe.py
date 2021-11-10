"""
## Cumulus acquriable for SERFC QPE

URL Dir - https://tgftp.nws.noaa.gov/data/rfc/serfc/misc/

File matching for:

QPE --> xmrgMMDDYYYYHHz.grb.gz
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

import helpers.cumulus as cumulus
from helpers.downloads import trigger_download

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

implementation = {
    "default": {
        "bucket": cumulus.S3_BUCKET,
        "dag_id": "cumulus_serfc_qpe",
        "tags": ["cumulus", "precip", "SERFC", "QPE"],
    },
}

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(days=2)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=10),
}


def create_dag(**kwargs):

    s3_bucket = kwargs["s3_bucket"]

    @dag(
        default_args=default_args,
        dag_id=kwargs["dag_id"],
        tags=kwargs["tags"],
        schedule_interval=kwargs["schedule_interval"],
        doc_md=dedent(__doc__),
    )
    def cumulus_acq_serfc():
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

        base_url = "https://tgftp.nws.noaa.gov/data/rfc/serfc/misc"

        slug = "serfc-qpe-01h"

        @task()
        def download_serfc():
            context = get_current_context()
            ti = context["ti"]
            execution_date = ti.execution_date
            filename = f'xmrg{execution_date.strftime("%m%d%Y%H")}z.grb.gz'
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
        _download_serfc = download_serfc()
        # Task 2: Use that list and compare what is in the S3 Bucket
        _notify_cumulus = notify_cumulus(_download_serfc)

        _download_serfc >> _notify_cumulus

    return cumulus_acq_serfc()


# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val["dag_id"]
    d_tags = val["tags"]
    d_bucket = val["bucket"]
    globals()[d_id] = create_dag(
        dag_id=d_id,
        tags=d_tags,
        s3_bucket=d_bucket,
        schedule_interval="5 * * * *",
    )
