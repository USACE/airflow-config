"""
Cumulus acquriable for SERFC QPE and QPF

URL Dir - https://tgftp.nws.noaa.gov/data/rfc/serfc/misc/

File matching for:

QPE --> xmrgMMDDYYYYHHz.grb.gz

QPF --> ALR_QPF_SFC_YYYYMMDDHH_FFF.grb.gz, where FFF is the forecast hour
"""

from datetime import timedelta
from textwrap import dedent

import helpers.cumulus as cumulus
from helpers.downloads import trigger_download

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago

implementation = {
    'stable': {
        'bucket': 'cwbi-data-stable',
        'dag_id': 'cumulus_serfc_qpf',
        'tags': ['stable', 'cumulus','precip', 'SERFC', 'QPF'],
    },
    'develop': {
        'bucket': 'cwbi-data-develop',
        'dag_id': 'develop_cumulus_serfc_qpf',
        'tags': ['develop', 'cumulus','precip', 'SERFC', 'QPF'],
    }
}

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(5),
    'catchup_by_default': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=60),
}

# ALR QPF filename generator
def alr_qpf_filenames(edate):
    d = edate.strftime('%Y%m%d')
    hh = edate.hour
    for fff in range(6,78,6):
        yield f'ALR_QPF_SFC_{d}{hh:02d}_{fff:03d}.grb.gz'

def create_dag(**kwargs):

    s3_bucket = kwargs['s3_bucket']
    conn_type = kwargs['conn_type']
    
    @dag(default_args=default_args,
        dag_id=kwargs['dag_id'],
        tags=kwargs['tags'],
        schedule_interval=kwargs['schedule_interval'],
        doc_md=dedent(__doc__),
        )
    def cumulus_acq_serfc():
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

        base_url = 'https://tgftp.nws.noaa.gov/data/rfc/serfc/misc'

        slug = 'serfc-qpf-06h'

        @task()
        def download_serfc():
            context = get_current_context()
            ti = context['ti']
            execution_date = ti.execution_date

            return_list = list()
            for filename in alr_qpf_filenames(execution_date):
                url=f'{base_url}/{filename}'
                s3_key=f'{key_prefix}/{slug}/{filename}'
                result = trigger_download(
                    url=url,
                    s3_bucket=s3_bucket,
                    s3_key=s3_key
                    )
                return_list.append({
                    'execution': execution_date.isoformat(),
                    'url': url,
                    's3_key': s3_key,
                    's3_bucket': s3_bucket,
                    'slug': slug,
                })
            return return_list
        @task()
        def notify_cumulus(download_result):
            for item in  download_result:
                result = cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[item['slug']],
                    datetime=item['execution'],
                    s3_key=item['s3_key'],
                    conn_type=conn_type,
                )

        # Task 1: Get dictionary of QPE and QPF files available
        _download_serfc = download_serfc()
        # Task 2: Use that list and compare what is in the S3 Bucket
        _notify_cumulus = notify_cumulus(_download_serfc)
        
        _download_serfc >> _notify_cumulus

    return cumulus_acq_serfc()
# Expose to the global() allowing airflow to add to the DagBag
for key, val in implementation.items():
    d_id = val['dag_id']
    d_tags = val['tags']
    d_bucket=val['bucket']
    globals()[d_id] = create_dag(
        dag_id=d_id,
        tags=d_tags,
        s3_bucket=d_bucket,
        conn_type=key,
        schedule_interval='55 0,12,18 * * *'
    )
