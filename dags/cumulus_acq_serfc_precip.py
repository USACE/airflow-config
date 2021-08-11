"""
Cumulus acquriable for SERFC QPE and QPF

URL Dir - https://tgftp.nws.noaa.gov/data/rfc/serfc/misc/

File matching for:

QPE --> xmrgMMDDYYYYHHz.grb.gz

QPF --> ALR_QPF_SFC_YYYYMMDDHH_FFF.grb.gz, where FFF is the forecast hour
"""

from datetime import datetime, timedelta, timezone
from textwrap import dedent

import helpers.cumulus as cumulus
import helpers.downloads as downloads
from helpers.downloads import trigger_download


from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

implementation = {
    'stable': {
        'bucket': 'cwbi-data-stable',
        'dag_id': 'CUMULUS-SERFC-PRECIP',
        'tags': ['stable', 'cumulus','precip', 'SERFC', 'QPE', 'QPF'],
    },
    'develop': {
        'bucket': 'cwbi-data-stable',
        'dag_id': 'DEVELOP-CUMULUS-SERFC-PRECIP',
        'tags': ['develop', 'cumulus','precip', 'SERFC', 'QPE', 'QPF'],
    }
}

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': (datetime.utcnow()-timedelta(days=2)).replace(minute=0, second=0),
    'catchup_by_default': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=15),
}

# ALR QPF filename generator
def alr_qpf_filenames(edate):
    d = edate.strftime('%Y%m%d')
    exe_hr = edate.hour
    if 0 >= exe_hr < 12:
        hh = 0
    elif  12 >= exe_hr < 18:
        hh = 12
    else:
        hh = 18
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
    def cumulus_acq_serfc_precip():
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

        base_url = 'https://tgftp.nws.noaa.gov/data/rfc/serfc/misc'

        slug_qpe = 'serfc-qpe-01h'
        slug_qpf = 'serfc-qpf-06h'

        @task()
        def download_serfc_qpe():
            context = get_current_context()
            ti = context['ti']
            execution_date = ti.execution_date
            filename = f'xmrg{execution_date.strftime("%m%d%Y%H")}z.grb.gz'
            url=f'{base_url}/{filename}'
            s3_key=f'{key_prefix}/{slug_qpe}/{filename}'
            result = trigger_download(
                url=url,
                s3_bucket=s3_bucket,
                s3_key=s3_key
                )
            return [{
                'execution': execution_date.isoformat(),
                'url': url,
                's3_key': s3_key,
                's3_bucket': s3_bucket,
                'slug': slug_qpe,
            }]

        @task()
        def download_serfc_qpf():
            context = get_current_context()
            ti = context['ti']
            execution_date = ti.execution_date

            return_list = list()
            for filename in alr_qpf_filenames(execution_date):
                url=f'{base_url}/{filename}'
                s3_key=f'{key_prefix}/{slug_qpf}/{filename}'
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
                    'slug': slug_qpf,
                })
            return return_list

        @task()
        def notify_cumulus_qpe(download_result):
            for item in  download_result:
                result = cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[item['slug']],
                    datetime=item['execution'],
                    s3_key=item['s3_key'],
                    conn_type=conn_type,
                )

        @task()
        def notify_cumulus_qpf(download_result):
            for item in  download_result:
                result = cumulus.notify_acquirablefile(
                    acquirable_id=cumulus.acquirables[item['slug']],
                    datetime=item['execution'],
                    s3_key=item['s3_key'],
                    conn_type=conn_type,
                )

        # Task 1: Get dictionary of QPE and QPF files available
        _download_serfc_qpe = download_serfc_qpe()
        _download_serfc_qpf = download_serfc_qpf()
        # Task 2: Use that list and compare what is in the S3 Bucket
        _notify_cumulus_qpe = notify_cumulus_qpe(_download_serfc_qpe)
        _notify_cumulus_qpf = notify_cumulus_qpf(_download_serfc_qpf)

    return cumulus_acq_serfc_precip()
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
        schedule_interval='45 * * * *'
    )