"""
Cumulus acquriable for SERFC QPE and QPF

URL Dir - https://tgftp.nws.noaa.gov/data/rfc/serfc/misc/

File matching for:

QPE --> xmrgMMDDYYYYHHz.grb.gz

QPF --> ALR_QPF_SFC_YYYYMMDDHH_FFF.grb.gz, where FFF is the forecast hour
"""

import os
import re
from datetime import datetime, timedelta
from textwrap import dedent
import requests

import helpers.cumulus as cumulus
import helpers.downloads as downloads
from helpers.downloads import trigger_download


from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context



# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow()-timedelta(hours=1)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

@dag(default_args=default_args,
     dag_id='CUMULUS-SERFC-PRECIP',
     tags=['cumulus','precip', 'SERFC'],
     doc_md=dedent(__doc__),
     )
def cumulus_acq_serfc_precip():
    conn_type = 'develop'
    s3_bucket = 'cwbi-data-develop'
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    
    base_url = 'https://tgftp.nws.noaa.gov/data/rfc/serfc/misc'
    pattern_qpe = re.compile(r'xmrg\d+z\.grb\.gz')
    #                          xmrg0804202108z.grb.gz
    pattern_qpf = re.compile(r'ALR_QPF_SFC_\d+_\d+\.grb\.gz')
    #                          ALR_QPF_SFC_2021081012_072.grb.gz

    slug_qpe = 'serfc-qpe-01h'
    slug_qpf = 'serfc-qpf-06h'

    @task()
    def tgftp_serfc():
        """"""
        with requests.Session() as session:
            resp = session.get(base_url)
            if resp.status_code != 200: raise AirflowException
            qpe_names = list(set(pattern_qpe.findall(resp.text)))
            qpf_names = list(set(pattern_qpf.findall(resp.text)))
            return {
                slug_qpe: qpe_names,
                slug_qpf: qpf_names,
            }

    @task()
    def s3_keys(slugs):
        needed = {
            slug: [
                key
                for f in files
                if not downloads.check_key_exists((key := f'{key_prefix}/{slug}/{f}'), s3_bucket)
            ]
            for slug, files in slugs.items()
        }
        return needed

    @task()
    def download_serfc(slugs):
        for slug, keys in slugs.items():
            output = [
                trigger_download(
                url=f'{base_url}/{os.path.basename(k)}',
                s3_bucket=s3_bucket,
                s3_key=k
                )
                for k in keys
            ]
        return slugs

    @task()
    def notify_cumulus(slugs):
        context = get_current_context()
        ti = context['ti']
        execution_date = ti.execution_date
        for slug, keys in slugs.items():
            for k in keys:
                result = cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[slug],
                    datetime=execution_date.isoformat(),
                    s3_key=k,
                    conn_type=conn_type,
                )
                print(result)

    # Task 1: Get dictionary of QPE and QPF files available
    _tgftp_serfc = tgftp_serfc()
    # Task 2: Use that list and compare what is in the S3 Bucket
    _s3_keys = s3_keys(_tgftp_serfc)
    # Task 3: Download the files and save to an S3 Bucket
    _download_serfc = download_serfc(_s3_keys)
    # Task 4: Notify Cumulus
    _notify_cumulus = notify_cumulus(_download_serfc)

# Expose to the global() allowing airflow to add to the DagBag
dag = cumulus_acq_serfc_precip()