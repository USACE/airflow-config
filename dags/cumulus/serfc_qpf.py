"""
## Cumulus acquriable for SERFC QPF

URL Dir - https://tgftp.nws.noaa.gov/data/rfc/serfc/misc/

File matching for:

QPF --> ALR_QPF_SFC_YYYYMMDDHH_FFF.grb.gz, where FFF is the forecast hour
"""

from datetime import datetime, timedelta, timezone
from textwrap import dedent
import json

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException, AirflowSkipException

import helpers.cumulus as cumulus
from helpers.downloads import trigger_download

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

implementation = {
    "default": {
        "bucket": cumulus.S3_BUCKET,
        "dag_id": "cumulus_serfc_qpf",
        "tags": ["cumulus", "precip", "SERFC", "QPF"],
    },
}

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(days=1)).replace(
        minute=0, second=0
    ),
    # "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=15),
}


# ALR QPF filename generator
def alr_qpf_filenames(edate):
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
        yield f"ALR_QPF_SFC_{d}{hh:02d}_{fff:03d}.grb.gz"


def create_dag(**kwargs):

    s3_bucket = kwargs["s3_bucket"]

    @dag(
        default_args=default_args,
        dag_id=kwargs["dag_id"],
        tags=kwargs["tags"],
        schedule=kwargs["schedule"],
        doc_md=dedent(__doc__),
        max_active_runs=2,
        max_active_tasks=2,
    )
    def cumulus_acq_serfc():
        key_prefix = cumulus.S3_ACQUIRABLE_PREFIX

        base_url = "https://tgftp.nws.noaa.gov/data/rfc/serfc/misc"

        slug = "serfc-qpf-06h"

        """
        Because this is a forecast product, we don't want to wait to get the product based
        on the last time period, but rather based on the current.  This is why the execution
        date is being shifted forward by 6 hours.
        """

        @task()
        def generate_filenames():
            context = get_current_context()
            logical_date = context["logical_date"] + timedelta(hours=6)
            # This task generates the list of filenames
            return list(alr_qpf_filenames(logical_date))

        @task()
        def check_first_file():
            context = get_current_context()
            ti = context["ti"]
            logical_date = context["logical_date"] + timedelta(hours=6)
            filename = next(alr_qpf_filenames(logical_date))
            url = f"{base_url}/{filename}"

            try:
                trigger_download(
                    url=url,
                    s3_bucket=s3_bucket,
                    s3_key=f"{key_prefix}/{slug}/{filename}",
                )
            except Exception as e:
                # If we don't always get a product for this time period
                # AND we've reached the try limit, skip the task instead of failing for better metrics analysis
                if logical_date.hour not in [0, 12] and ti.try_number >= ti.max_tries:
                    raise AirflowSkipException(
                        f"Skipping task due to no files available and max_tries ({ti.max_tries}) reached: {e}"
                    )
                raise AirflowException(f"Error downloading file: {e}")

        # The main task that will download the files dynamically
        @task(map_index_template="{{ task_id }}")
        def download_file(filename):
            context = get_current_context()
            logical_date = context["logical_date"] + timedelta(hours=6)

            # Name the dynamic task instead of leaving the index number
            context["task_id"] = filename

            url = f"{base_url}/{filename}"
            s3_key = f"{key_prefix}/{slug}/{filename}"
            result = trigger_download(url=url, s3_bucket=s3_bucket, s3_key=s3_key)
            return {
                "execution": logical_date.isoformat(),
                "url": url,
                "s3_key": s3_key,
                "s3_bucket": s3_bucket,
                "slug": slug,
            }

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
        schedule="5 */6 * * *",
    )
