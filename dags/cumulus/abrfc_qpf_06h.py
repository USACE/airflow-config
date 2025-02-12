"""
Acquire and Process ABRFC QPF 6hr
    This pipeline handles download, processing, and derivative product creation for \n
    ABRFC QPE\n
    URL Dir - https://tgftp.nws.noaa.gov/data/rfc/abrfc/xmrg_qpf/
    Files matching QPF6_YYYYMMDDHHf0HH.cdf - 6 hour\n
    Note: Delay observed when watching new product timestamp on file at source.
    Example: timestamp said 15:50, but was pushed to server at 16:07
"""

from datetime import datetime, timedelta, timezone
import json

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException, AirflowSkipException


import helpers.cumulus as cumulus
from helpers.downloads import trigger_download

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=48)).replace(
        minute=0, second=0
    ),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 12,
    "retry_delay": timedelta(minutes=15),
}


# ALR QPF filename generator
def qpf_filenames(edate):
    hh = edate.hour
    print(hh)
    # if 0 >= hh < 12:
    #     hh = 0
    # elif 12 >= hh < 18:
    #     hh = 12
    # else:
    #     hh = 18
    d = edate.strftime("%Y%m%d")
    for fff in range(6, 78, 6):
        # forecast_date = (edate + timedelta(hours=fff)).strftime("%Y%m%d%H")
        yield f"QPF6_{d}{hh:02d}f{fff:03d}.cdf"


@dag(
    default_args=default_args,
    schedule="8 */6 * * *",
    tags=["cumulus", "precip", "QPF", "ABRFC"],
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=2,
)
def cumulus_abrfc_qpf_06h():
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

    URL_ROOT = "https://tgftp.nws.noaa.gov/data/rfc/abrfc/xmrg_qpf"

    PRODUCT_SLUG = "abrfc-qpf-06h"

    """
    Because this is a forecast product, we don't want to wait to get the product based
    on the last time period, but rather based on the current.  This is why the logical
    date is being shifted forward by 6 hours.
    """

    @task()
    def generate_filenames():
        # Overwrite the logical date to be 6 hours in the future
        logical_date = get_current_context()["logical_date"] + timedelta(hours=6)

        # This task generates the list of filenames
        return list(qpf_filenames(logical_date))

    ###########################################################################
    @task()
    def check_first_file():
        context = get_current_context()
        logical_date = context["logical_date"] + timedelta(hours=6)
        ti = context["ti"]  # task instance
        filename = next(qpf_filenames(logical_date))
        url = f"{URL_ROOT}/{filename}"

        try:
            trigger_download(
                url=url,
                s3_bucket=cumulus.S3_BUCKET,
                s3_key=f"{key_prefix}/{PRODUCT_SLUG}/{filename}",
            )
        except Exception as e:
            # If we don't always get a product for this time period
            # AND we've reached the try limit, skip the task instead of failing for better metrics analysis
            if logical_date.hour not in [0, 12, 18] and ti.try_number >= ti.max_tries:
                raise AirflowSkipException(
                    f"Skipping task due to no files available and max_tries ({ti.max_tries}) reached: {e}"
                )
            raise AirflowException(f"Error downloading file: {e}")

    ###########################################################################
    # The main task that will download the files dynamically
    @task(map_index_template="{{ task_id }}")
    def download_file(filename):
        print(f"Downloading {filename}")
        context = get_current_context()
        logical_date = context["logical_date"] + timedelta(hours=6)

        # Name the dynamic task instead of leaving the index number
        context["task_id"] = filename

        url = f"{URL_ROOT}/{filename}"
        s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{filename}"
        result = trigger_download(url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
        return {
            "execution": logical_date.isoformat(),
            "url": url,
            "s3_key": s3_key,
            "s3_bucket": cumulus.S3_BUCKET,
            "slug": PRODUCT_SLUG,
        }

    ###########################################################################
    @task()
    def notify_cumulus(download_result):

        if not len(list(download_result)):
            raise AirflowSkipException("Skipping task due to no files downloaded")

        print(f"Posting {len(list(download_result))} items to Cumulus API")

        for item in download_result:
            result = cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[item["slug"]],
                datetime=item["execution"],
                s3_key=item["s3_key"],
            )

    ###########################################################################
    # Generate filenames dynamically
    _generate_filenames = generate_filenames()

    """
    Check the first file to see if it exists.
    This is done so that the dynamic tasks don't fail which
    occupies many slots on the executor (which holds up other tasks from running)
    """
    _check_first_file = check_first_file()

    # Download the files dynamically using task mapping
    _download_results = download_file.expand(filename=_generate_filenames)

    # Once the download is done, notify the API with the results
    notify_cumulus_task = notify_cumulus(_download_results)

    notify_cumulus_task.trigger_rule = TriggerRule.ALL_DONE

    # Ensure the tasks run in order
    (
        _generate_filenames
        >> _check_first_file
        >> _download_results
        >> notify_cumulus_task
    )


abrfc_qpf_dag = cumulus_abrfc_qpf_06h()
