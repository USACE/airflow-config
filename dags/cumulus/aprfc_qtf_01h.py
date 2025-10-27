"""
Acquire and Process APRFC qtf 01h
"""

import calendar
import json
import re
from datetime import datetime, timedelta, timezone

import helpers.cumulus as cumulus
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from bs4 import BeautifulSoup
from helpers.downloads import s3_file_exists, trigger_download

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.now(timezone.utc) - timedelta(hours=36)).replace(
        minute=0, second=0
    ),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 6,
    "retry_delay": timedelta(minutes=30),
}


def get_latest_files(filenames):
    # Dictionary to store the latest file for each unique timestamp
    latest_files = {}

    # Updated regular expression to extract the issue timestamp and forecast timestamp
    pattern = r"^ta01.*_awips_(\d{12})_(\d{10}f\d{3})"

    for filename in filenames:
        match = re.search(pattern, filename)
        if match:
            # Combine the issue timestamp and forecast timestamp as the key
            issue_timestamp = match.group(1)
            forecast_timestamp = match.group(2)
            key = issue_timestamp + "_" + forecast_timestamp
            # Update the latest file for the key if it's not present or if the current filename is greater
            if key not in latest_files or filename > latest_files[key]:
                latest_files[key] = filename

    # Return the list of latest files
    return list(latest_files.values())


# APRFC qtf filename generator
def get_filenames(edate, url):
    """
    Scrape website, collect matching filenames, then:
      - find the latest issuance timestamp (the 12-digit value after 'awips_')
      - if latest issuance is within 36 hours of edate (Airflow logical_date), return only those files
      - otherwise return an empty list (nothing to download)
    """
    # Fetch page and extract links
    page = requests.get(url, timeout=30)
    soup = BeautifulSoup(page.content, "html.parser")
    links = [node.get("href") for node in soup.find_all("a")]

    # Match the target files; allows optional integer version before .grb and optional .gz
    regex = r"^ta01.*_awips_\d{12}_\d{10}f\d{3}(?:\.\d+)?\.grb(\.gz)?$"
    issue_re = re.compile(
        r"_awips_(\d{12})"
    )  # Capture the issuance timestamp after 'awips_'

    # Collect candidates with their issuance timestamp
    candidates = []
    for link in links:
        if link and re.match(regex, link):
            m = issue_re.search(link)
            if m:
                candidates.append((link, m.group(1)))

    # No matching files
    if not candidates:
        return []

    # Find the latest issuance
    latest_issue_str = max(issue for _, issue in candidates)

    # Convert to an aware UTC datetime for comparison against Airflow's logical_date
    latest_issue_dt = datetime.strptime(latest_issue_str, "%Y%m%d%H%M").replace(
        tzinfo=timezone.utc
    )

    # Only proceed if latest issuance is in the past and within the last 36 hours relative to edate (logical_date)
    if latest_issue_dt > edate or (edate - latest_issue_dt) > timedelta(hours=36):
        # Outside the 36-hour window or in the future: do not download anything
        return []

    # Filter to only files from the latest issuance
    latest_issue_files = [fn for fn, issue in candidates if issue == latest_issue_str]

    # Deduplicate within that issuance set by issue+forecast key as your existing logic defines
    return get_latest_files(latest_issue_files)


@dag(
    default_args=default_args,
    schedule="25 * * * *",
    tags=["cumulus", "temp", "QTF", "APRFC"],
    max_active_runs=1,
    max_active_tasks=1,
)
def cumulus_aprfc_qtf_01h():
    """This pipeline handles download, processing, and derivative product creation for \n
    APRFC QTF\n
    URL Dir - https://cbt.crohms.org/akgrids
    Files matching ta01f_has_92f_20241219_08_awips_202412150008.grb. - 1 hour\n
    """
    key_prefix = cumulus.S3_ACQUIRABLE_PREFIX
    URL_ROOT = f"https://cbt.crohms.org/akgrids"
    PRODUCT_SLUG = "aprfc-qtf-01h"

    @task()
    def download_raw_qtf():
        logical_date = get_current_context()["logical_date"]

        return_list = list()
        filenames = get_filenames(logical_date, URL_ROOT)
        for filename in filenames:
            url = f"{URL_ROOT}/{filename}"
            s3_key = f"{key_prefix}/{PRODUCT_SLUG}/{filename}"
            try:
                # Check if the file already exists in S3
                if s3_file_exists(cumulus.S3_BUCKET, s3_key):
                    print(f"Skipping existing S3 object: s3://{cumulus.S3_BUCKET}/{s3_key}")
                    continue  # Skip to the next file
            except:
                print(f'Error checking S3 for {s3_key}')
            print(f"Downloading file: {filename}")
            try:
                trigger_download(url=url, s3_bucket=cumulus.S3_BUCKET, s3_key=s3_key)
                return_list.append(
                    {
                        "execution": logical_date.isoformat(),
                        "s3_key": s3_key,
                        "filename": filename,
                    }
                )
            except:
                print(f"{filename} is not available to download")

        return json.dumps(return_list)

    @task()
    def notify_cumulus(payload):
        payload = json.loads(payload)
        for item in payload:
            print("Notifying Cumulus: " + item["filename"])
            cumulus.notify_acquirablefile(
                acquirable_id=cumulus.acquirables[PRODUCT_SLUG],
                datetime=item["execution"],
                s3_key=item["s3_key"],
            )

    notify_cumulus(download_raw_qtf())


aprfc_qtf_dag = cumulus_aprfc_qtf_01h()
