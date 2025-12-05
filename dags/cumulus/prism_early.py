"""
Acquire and Process PRISM Real-time
"""

import json
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc)-timedelta(days=14)).replace(
        minute=0, second=0
    ),    
    # "start_date": datetime(2021, 11, 9),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def generate_filename_for_product(product, date):
    # product: 'tmin', 'tmax', 'ppt'
    # date: datetime object
    return f'prism_{product}_us_25m_{date.strftime("%Y%m%d")}.zip'

@dag(
    default_args=default_args,
    schedule="30 12 * * *",
    tags=["cumulus", 'prism'],
    max_active_runs=2,
    max_active_tasks=4,
)

def cumulus_prism_early():
    """This pipeline handles download, processing, and derivative product creation for \n
    PRISM: Min Temp (tmin) early, Max Temp (tmax) early and Precip (ppt) early
    URL Dir - ftp://prism.nacse.org/daily/tmin/YYYY/
    Files matching prism_ppt_us_25m_YYYYMMDD.zip'- Daily around 12:30-14:30 UTC
    """

    URL_ROOT = f"ftp://prism.nacse.org/daily"
    URL_ROOT = f"https://data.prism.oregonstate.edu/time_series/us/an/4km"


    # Download Tasks
    #################################################
    @task()
    def download_raw_prism_early(short_name='ppt'):
        product_slug = f"prism-{short_name}-early"
        logical_date = get_current_context()["logical_date"]-timedelta(hours=24)
        execution_date = logical_date.date()
        results = []

        dt = execution_date
        file_dir = f'{URL_ROOT}/{short_name}/daily/{dt.strftime("%Y")}'
        filename = generate_filename_for_product(short_name, dt)
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{product_slug}/{filename}"
        print(f"Downloading {filename}")

        output = trigger_download(
            url=f"{file_dir}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key,
        )
        results.append(
            {
                "datetime": logical_date.isoformat(),
                "s3_key": s3_key,
                "product_slug": product_slug,
                "filename": filename,
            }
        )

        return json.dumps(results)

    # Notify Tasks
    #################################################
    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for item in payload:
            print("Notifying Cumulus: " + item["filename"])
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[item["product_slug"]],
                datetime=item["datetime"],
                s3_key=item["s3_key"],
            )

    notify_cumulus(download_raw_prism_early(short_name='ppt'))
    notify_cumulus(download_raw_prism_early(short_name='tmax'))
    notify_cumulus(download_raw_prism_early(short_name= 'tmin'))


prism_dag = cumulus_prism_early()
