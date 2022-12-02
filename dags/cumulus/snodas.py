"""
# National Snow and Ice Data Center (NSIDC)
a part of CIRES at the University of Colorado Bolder

Returns
-------
Airflow DAG
    Directed Acyclic Graph
"""

import json
from datetime import datetime, timedelta

import helpers.cumulus as cumulus
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2021, 1, 10, 0, 0, 0),
    "start_date": (datetime.utcnow() - timedelta(hours=96)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 16,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# An Example Using the Taskflow API
@dag(
    default_args=default_args,
    schedule="20 13 * * *",
    tags=["cumulus", "snow"],
    max_active_runs=2,
    max_active_tasks=4,
)
def cumulus_snodas_unmasked():
    """
    # National Snow and Ice Data Center (NSIDC)
    ___a part of CIRES at the University of Colorado Bolder___

    **Backloading 2004-2010**

    This data set contains snow pack properties, such as depth and snow water equivalent (SWE),

    from the NOAA National Weather Service's National Operational Hydrologic Remote Sensing Center (NOHRSC)

    SNOw Data Assimilation System (SNODAS). SNODAS is a modeling and data assimilation system developed by

    NOHRSC to provide the best possible estimates of snow cover and associated parameters to support hydrologic modeling and analysis.

    Snow Data Assimilation System (SNODAS) Data Products at NSIDC, Version 1 (G02158)



    click [__here__](https://nsidc.org/data/g02158/versions/1)

    User Guide

    click [__here__](https://nsidc.org/sites/default/files/g02158-v001-userguide_2_1.pdf)

    Cooperative Institute for Research in Environmental Sciences (CIRES) at the University of Colorado Boulder

    click [__here__](https://cires.colorado.edu/)
    """

    PRODUCT_SLUG = "nohrsc-snodas-unmasked"

    @task()
    def snodas_download_unmasked():

        # In order to get the current day's file, set execution forward 1 day
        execution_date = get_current_context()["logical_date"] + timedelta(hours=24)

        URL_ROOT = f'ftp://sidads.colorado.edu/DATASETS/NOAA/G02158/unmasked/{execution_date.year}/{execution_date.strftime("%m_%b")}'
        filename = f'SNODAS_unmasked_{execution_date.strftime("%Y%m%d")}.tar'
        s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{PRODUCT_SLUG}/{filename}"
        output = trigger_download(
            url=f"{URL_ROOT}/{filename}", s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key
        )

        return json.dumps({"datetime": execution_date.isoformat(), "s3_key": s3_key})

    @task()
    def snodas_unmasked_notify_cumulus(payload):

        # Airflow will convert the parameter to a string, convert it back
        payload = json.loads(payload)

        cumulus.notify_acquirablefile(
            acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
            datetime=payload["datetime"],
            s3_key=payload["s3_key"],
        )

    snodas_unmasked_notify_cumulus(snodas_download_unmasked())


snodas_dag = cumulus_snodas_unmasked()
