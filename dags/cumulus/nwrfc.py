"""
## NWRFC QPE, QPF, QTE, QTF

Browser view: https://www.nwrfc.noaa.gov/misc/downloads/index.php?type=netcdf_forcings&sortby=date&sortasc=true&filter=

It looks like only current day is posted, so the link above may not be very durable for retrieving a sample file after today. 
Will have to fiddle the date numbers in the URL.

### QPE
Example file: https://www.nwrfc.noaa.gov/weather/netcdf/2022/20221212/QPE.2022121212.nc.gz

The QPE file contains 6hr grids (4 of them) - suspect this covers the past 24 hours of precipitation.

### QPF
Example file: https://www.nwrfc.noaa.gov/weather/netcdf/2022/20221212/QPF.2022121212.nc.gz

Sample file contains 6-hour interval grids, covering approximately 10 days into the future. 

### QTE

Sample file: https://www.nwrfc.noaa.gov/weather/netcdf/2022/20221212/QTE.2022121212.nc.gz

Contains 6-hour interval grids, 4 total. Covering the past 24 hour period.

### QTF

Sample file: https://www.nwrfc.noaa.gov/weather/netcdf/2022/20221212/QTF.2022121212.nc.gz

Sample file contains 6-hour interval grids, covering approximately 10 days into the future. 
    
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from helpers.downloads import trigger_download

import helpers.cumulus as cumulus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=72)).replace(minute=0, second=0),
    # "start_date": datetime(2022, 7, 1),
    "catchup_by_default": True,
    # "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 8,
    "retry_delay": timedelta(minutes=30),
}


@dag(
    default_args=default_args,
    schedule="50 12 * * *",
    tags=["cumulus", "precip", "airtemp", "QPE", "QPF", "QTE", "QTF", "NWRFC"],
    max_active_runs=2,
    max_active_tasks=4,
    doc_md=__doc__,
)
def cumulus_nwrfc():

    URL_ROOT = f"https://www.nwrfc.noaa.gov/weather/netcdf"

    AQUIRABLES = [
        {"nwrfc-qpe-06h": {"prefix": "QPE"}},
        {"nwrfc-qpf-06h": {"prefix": "QPF"}},
        {"nwrfc-qte-06h": {"prefix": "QTE"}},
        {"nwrfc-qtf-06h": {"prefix": "QTF"}},
    ]

    for acquirable in AQUIRABLES:

        for slug in acquirable.keys():

            with TaskGroup(group_id=f"{slug}") as tg:

                @task(task_id=f"download_{slug}")
                def download_acquirable(acquirable, slug):

                    logical_date = get_current_context()["logical_date"]
                    # Try to get the latest file, not the one from the prior period
                    logical_date = logical_date + timedelta(days=1)

                    dirpath = f'{logical_date.strftime("%Y")}/{logical_date.strftime("%Y%m%d")}'
                    file_prefix = acquirable[slug]["prefix"]
                    filename = (
                        f'{file_prefix}.{logical_date.strftime("%Y%m%d%H")}.nc.gz'
                    )
                    filepath = f"{dirpath}/{filename}"
                    s3_key = f"{cumulus.S3_ACQUIRABLE_PREFIX}/{slug}/{filename}"
                    print(f"Downloading {filepath}")

                    trigger_download(
                        url=f"{URL_ROOT}/{filepath}",
                        s3_bucket=cumulus.S3_BUCKET,
                        s3_key=s3_key,
                    )

                    return json.dumps(
                        {
                            "datetime": logical_date.isoformat(),
                            "s3_key": s3_key,
                            "slug": slug,
                        },
                    )

                @task()
                def notify_cumulus(payload):

                    # Airflow will convert the parameter to a string, convert it back
                    payload = json.loads(payload)

                    cumulus.notify_acquirablefile(
                        acquirable_id=cumulus.acquirables[payload["slug"]],
                        datetime=payload["datetime"],
                        s3_key=payload["s3_key"],
                    )

                notify_cumulus(download_acquirable(acquirable, slug))


DAG_ = cumulus_nwrfc()
